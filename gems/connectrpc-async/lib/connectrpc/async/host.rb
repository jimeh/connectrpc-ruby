# frozen_string_literal: true

module ConnectRPC
  module Async
    class Host
      include Internal::Affinity

      attr_reader :port, :addresses, :tls_generation

      def initialize(server:, host: "127.0.0.1", port: 0, tls: nil, max_connections: 128, max_requests: 128,
                     handshake_timeout: 10, idle_timeout: 60)
        initialize_affinity
        raise ArgumentError, "server must be a ConnectRPC::Server" unless server.is_a?(ConnectRPC::Server)

        @server = server
        @host = host
        @requested_port = port
        @max_connections = Internal.positive_integer(max_connections, "max_connections")
        @max_requests = Internal.positive_integer(max_requests, "max_requests")
        @handshake_timeout = Internal.seconds(handshake_timeout, "handshake_timeout")
        @idle_timeout = Internal.seconds(idle_timeout, "idle_timeout") unless idle_timeout.nil?
        @snapshot = Internal::TLS.server(**tls) unless tls.nil?
        @tls_generation = @snapshot ? 1 : nil
        @retirement_floor = 0
        @admitted = {}
        @identities = {}
        @addresses = [].freeze
        install_owner_guard
      end

      def inspect = "#<#{self.class} state=#{@state} port=#{@port.inspect} tls_generation=#{@tls_generation.inspect}>"

      def start
        check_affinity!
        return self if @state == :running
        raise LifecycleError, "Host cannot start after close" unless @state == :new

        start_listening
      end

      def reload_tls(certificate_chain:, private_key:, client_ca:)
        check_running_tls!
        replacement = Internal::TLS.server(certificate_chain: certificate_chain, private_key: private_key,
                                           client_ca: client_ca)
        @snapshot = replacement
        @tls_generation += 1
      end

      def drain_tls(grace: 5)
        check_running_tls!
        Internal.seconds(grace, "grace", zero: true)
        cutoff = @tls_generation
        @retirement_floor = [@retirement_floor, cutoff].max
        targets = @resources.reject(&:finished?).select { |resource| resource.generation < cutoff }
        targets.each { |resource| resource.draining = true }
        drain_resources(targets, Internal.now + grace)
      end

      def peer_certificate
        return unless Thread.current.equal?(@thread) && Fiber.scheduler.equal?(@reactor)

        der = @identities[::Async::Task.current?]
        OpenSSL::X509::Certificate.new(der.dup) if der
      end

      private

      def start_listening
        @bound = ::IO::Endpoint.tcp(@host, @requested_port).bound
        @addresses = @bound.sockets.map(&:local_address).freeze
        @port = @addresses.first.ip_port
        @state = :running
        @listeners = @bound.sockets.map do |listener|
          @lifetime.async(finished: false) { accept_loop(listener) }
        end
        self
      rescue StandardError
        abort_owned
        raise
      end

      def check_running_tls!
        check_affinity!
        raise LifecycleError, "A running TLS host is required" unless @state == :running && @snapshot
      end

      def accept_loop(listener)
        while @state == :running
          raw, = listener.accept
          @resources.reject!(&:finished?)
          if @resources.size >= @max_connections
            Internal.raw_close(raw)
            next
          end
          # Capture before scheduling or handshake; reload never changes an
          # already accepted socket's authentication generation.
          resource = Internal::Resource.new(progress: @progress, generation: @tls_generation, snapshot: @snapshot)
          resource.raw = raw
          @resources << resource
          resource.task = @lifetime.async(finished: false) { serve(resource) }
        end
      rescue IOError, SystemCallError
        raise if @state == :running
      end

      def serve(resource)
        expires = Internal.now + @handshake_timeout
        setup = resource.track(::Async::Task.current.async(finished: false) { setup_connection(resource) })
        Internal.wait(setup, expires)
        return if resource.draining || (@tls_generation && resource.generation < @retirement_floor)

        resource.ready = true
        resource.changed
        resource.connection.start_connection
        idle = @lifetime.async(finished: false) { watch_idle(resource) } if @idle_timeout
        resource.connection.each { |request| dispatch(resource, request) }
      rescue StandardError
        # Socket/setup/protocol failures terminate only this owned connection.
        nil
      ensure
        Internal.cancel(idle)
        resource.abort unless resource.finishing?
      end

      def setup_connection(resource)
        socket = resource.raw
        if resource.snapshot
          socket = resource.snapshot.socket(resource.raw)
          resource.socket = socket
          socket.accept
          raise OpenSSL::SSL::SSLError, "HTTP/2 ALPN is required" unless socket.alpn_protocol == "h2"
        end
        resource.socket = socket
        connection = Internal::ServerConnection.new(::IO::Stream(socket))
        resource.connection = connection
        connection.resource = resource
        connection.read_connection_preface(Internal.settings(@max_requests))
      end

      def watch_idle(resource)
        loop do
          if resource.empty?
            remaining = @idle_timeout - (Internal.now - resource.activity_at)
            if remaining <= 0
              resource.abort
              break
            end
            begin
              ::Async::Task.current.with_timeout([remaining, Internal::MAX_WAIT].min) { resource.progress.wait }
            rescue ::Async::TimeoutError
              next
            end
          else
            resource.progress.wait
          end
          break if resource.finished?
        end
      end

      def dispatch(resource, request)
        @admitted.delete_if { |stream, completed| completed && stream.closed? }
        if @state != :running || resource.draining || @admitted.size >= @max_requests
          return ::Protocol::HTTP::Response[503, {}, nil]
        end

        @admitted[request.stream] = false
        begin
          handler = resource.track(@lifetime.async(finished: false) do |task|
            @identities[task] = resource.socket.peer_cert.to_der.freeze if resource.snapshot&.authenticated?
            begin
              call_core(request)
            ensure
              @identities.delete(task)
            end
          end)
          handler.wait
        ensure
          @admitted[request.stream] = true
          @admitted.delete(request.stream) if request.stream.closed?
          resource.changed
        end
      end

      def call_core(request)
        path = request.path.split("?", 2).first
        return ::Protocol::HTTP::Response[404, {}, nil] unless @server.handles?(path)

        response = @server.call(path: path, http_method: request.method, headers: Internal.headers(request),
                                input: Internal::Input.new(request.body))
        body = response.body.empty? ? nil : [response.body]
        ::Protocol::HTTP::Response[response.status, Internal.fields(response.headers), body]
      end

      def drain_resources(resources, expires)
        tasks = resources.map do |resource|
          @lifetime.async(finished: false) { resource.drain(@lifetime, expires) }
        end
        tasks.map(&:wait).all?
      end

      def close_owned(expires)
        @bound&.close
        @listeners&.each { |task| Internal.cancel(task) }
        @resources.each { |resource| resource.draining = true }
        finish_closed(drain_resources(@resources, expires))
      end

      def abort_owned
        return if @aborting

        @aborting = true
        @state = :closing unless @state == :closed
        @bound&.close
        @listeners&.each { |task| Internal.cancel(task) }
        @resources.each(&:abort)
        finish_closed(false) unless @state == :closed
        Internal.cancel(@lifetime)
      end
    end
  end
end
