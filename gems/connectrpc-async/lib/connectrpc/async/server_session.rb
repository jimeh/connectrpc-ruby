# frozen_string_literal: true

module ConnectRPC
  module Async
    class ServerSession
      include Internal::Affinity

      def initialize(application:, tls: nil, tls_provider: nil, max_connections: 128, max_requests: 128,
                     handshake_timeout: 10, idle_timeout: 60, refresh_interval: 60, refresh_jitter: 0.1,
                     refresh_timeout: 10, on_tls_error: nil, alpn_protocols: ["h2"], http1: nil, stop_accepting: nil)
        initialize_affinity
        raise ArgumentError, "application must be callable" unless application.respond_to?(:call)
        raise ArgumentError, "stop_accepting must be callable" if stop_accepting && !stop_accepting.respond_to?(:call)

        @application = application
        @http1 = http1
        @stop_accepting = stop_accepting
        @selector = ::Async::HTTP::Protocol::HTTP.new if http1
        @max_connections = Internal.positive_integer(max_connections, "max_connections")
        @max_requests = Internal.positive_integer(max_requests, "max_requests")
        @handshake_timeout = Internal.seconds(handshake_timeout, "handshake_timeout")
        @idle_timeout = Internal.seconds(idle_timeout, "idle_timeout") unless idle_timeout.nil?
        @tls = TLSConfiguration.new(tls: tls, tls_provider: tls_provider, refresh_interval: refresh_interval,
                                    refresh_jitter: refresh_jitter, refresh_timeout: refresh_timeout,
                                    on_error: on_tls_error, alpn_protocols: alpn_protocols)
        @retirement_floor = 0
        @admitted = {}
        @identities = {}
        install_owner_guard
      end

      def inspect = "#<#{self.class} state=#{@state} tls_generation=#{tls_generation.inspect}>"

      def start
        check_affinity!
        return self if @state == :running
        raise LifecycleError, "Server session cannot start after close" unless @state == :new

        @tls.start
        @state = :running
        self
      end

      def reload_tls(certificate_chain:, private_key:, client_ca:)
        check_running_tls!
        @tls.reload(certificate_chain: certificate_chain, private_key: private_key, client_ca: client_ca)
      end

      def tls_generation = @tls.generation
      def tls_status = @tls.status
      def refresh_tls = @tls.refresh

      def close(grace: 5)
        check_affinity!
        @tls.send(:check_recursion!)
        super
      end

      def abort
        check_affinity!
        @tls.send(:check_recursion!)
        abort_owned
        self
      end

      def drain_tls(grace: 5)
        check_running_tls!
        Internal.seconds(grace, "grace", zero: true)
        cutoff = tls_generation
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

      # Accepted work must outlive cancellation of a listener's accept loop.
      def schedule(&)
        check_affinity!
        @lifetime.async(finished: false, &)
      end

      # Hosting SPI. Capture must happen in the accepting task before it
      # schedules any handshake work. The returned handle owns this one peer.
      def accept(raw)
        check_affinity!
        @resources.reject!(&:finished?)
        unless @state == :running && @resources.size < @max_connections
          Internal.raw_close(raw)
          return
        end
        capture = @tls.send(:capture, raw)
        resource = Internal::Resource.new(progress: @progress, generation: capture.generation, snapshot: capture)
        resource.raw = raw
        @resources << resource
        Accepted.new(self, resource, @progress)
      rescue LifecycleError
        raise
      rescue StandardError
        Internal.raw_close(raw)
        nil
      end

      class Accepted
        def initialize(session, resource, progress)
          @session = session
          @resource = resource
          @progress = progress
        end

        def serve
          @session.send(:serve, @resource) { |resource| @resource = resource }
        end

        def close
          @resource.abort unless @resource.finishing?
        end

        def wait_closed
          @progress.wait until @resource.finished?
        end

        def timeout=(value)
          @resource.raw.timeout = value if @resource.raw.respond_to?(:timeout=)
        end

        def remote_address = @resource.raw.remote_address
        def inspect = "#<#{self.class} accepted connection>"
      end
      private_constant :Accepted

      private

      def check_running_tls!
        check_affinity!
        raise LifecycleError, "A running TLS host is required" unless @state == :running && tls_generation
      end

      def serve(resource)
        resource.task = ::Async::Task.current
        expires = Internal.now + @handshake_timeout
        setup = resource.track(::Async::Task.current.async(finished: false) { setup_connection(resource) })
        resource = Internal.wait(setup, expires)
        yield resource if block_given?
        return if resource.draining || (tls_generation && resource.generation < @retirement_floor)

        resource.ready = true
        resource.changed
        resource.connection.start_connection if resource.connection.respond_to?(:start_connection)
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
        socket = resource.snapshot.handshake { |socket| resource.socket = socket }
        resource.socket = socket
        stream = ::IO::Stream(socket)
        if @http1 && ((!resource.snapshot.tls? &&
                       @selector.protocol_for(stream) == ::Async::HTTP::Protocol::HTTP1) ||
                      (resource.snapshot.tls? && socket.alpn_protocol != "h2"))
          return resource if resource.draining || (tls_generation && resource.generation < @retirement_floor)

          replacement = @http1.call(stream, progress: @progress, generation: resource.generation)
          replacement.raw = resource.raw
          replacement.socket = socket
          replacement.snapshot = resource.snapshot
          replacement.task = resource.task
          @resources[@resources.index(resource)] = replacement
          return replacement
        end
        if resource.snapshot.tls? && socket.alpn_protocol != "h2"
          raise OpenSSL::SSL::SSLError, "HTTP/2 ALPN is required"
        end

        connection = Internal::ServerConnection.new(stream)
        resource.connection = connection
        connection.resource = resource
        connection.read_connection_preface(Internal.settings(@max_requests))
        resource
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
        @admitted.delete_if { |stream, completed| completed && (!stream.respond_to?(:closed?) || stream.closed?) }
        key = request.respond_to?(:stream) ? request.stream : request
        if @state != :running || resource.draining || @admitted.size >= @max_requests
          return ::Protocol::HTTP::Response[503, {}, nil]
        end

        @admitted[key] = false
        begin
          handler = resource.track(@lifetime.async(finished: false) do |task|
            @identities[task] = resource.snapshot.authenticated_leaf
            begin
              @application.call(request)
            ensure
              @identities.delete(task)
            end
          end)
          response = handler.wait
          if !request.respond_to?(:stream) && response
            response.body = Internal::AdmissionBody.new(response.body) do
              @admitted.delete(key)
              resource.changed
            end
            deferred = true
          end
          response
        ensure
          unless deferred
            @admitted[key] = true
            @admitted.delete(key) if !key.respond_to?(:closed?) || key.closed?
          end
          resource.changed
        end
      end

      def drain_resources(resources, expires)
        tasks = resources.map do |resource|
          @lifetime.async(finished: false) { resource.drain(@lifetime, expires) }
        end
        tasks.map(&:wait).all?
      end

      def stop_accepting
        @stop_accepting&.call
      end

      def close_owned(expires)
        stop_accepting
        @tls.close
        @resources.each { |resource| resource.draining = true }
        finish_closed(drain_resources(@resources, expires))
      end

      def abort_owned
        return if @aborting

        @aborting = true
        @state = :closing unless @state == :closed
        stop_accepting
        @resources.each(&:abort)
        @tls.close
        finish_closed(false) unless @state == :closed
        Internal.cancel(@lifetime)
      end
    end
  end
end
