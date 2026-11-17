# frozen_string_literal: true

module ConnectRPC
  module Async
    # Strict HTTP/2 listener composing the same worker session as other hosts.
    # An optional native fallback serves requests outside registered services.
    class Host
      BIND_ATTEMPTS = 5
      private_constant :BIND_ATTEMPTS

      # The Host owns no tunnels or socket handoff, so its fallback receives no
      # CONNECT request and cannot switch protocols. Plain CONNECT has no path
      # and fails in core routing before reaching this guard.
      class FallbackGuard
        def initialize(fallback)
          @fallback = fallback
        end

        def call(request)
          return ::Protocol::HTTP::Response[501, {}, nil] if request.method == "CONNECT" || request.protocol

          response = @fallback.call(request)
          return response unless response

          # The fallback response contract does not include protocol.
          switching = response.status == 101 || (response.respond_to?(:protocol) && response.protocol)
          return response unless switching

          response.close
          ::Protocol::HTTP::Response[501, {}, nil]
        end
      end
      private_constant :FallbackGuard

      attr_reader :port, :addresses

      def initialize(server:, host: "127.0.0.1", port: 0, tls: nil, tls_provider: nil,
                     max_connections: 1024, max_requests: 128, handshake_timeout: 10, idle_timeout: 60,
                     refresh_interval: 60, refresh_jitter: 0.1, refresh_timeout: 10, on_tls_error: nil,
                     keepalive: ServerKeepalive.new, fallback: nil, max_concurrent_streams: 100,
                     request_body_timeout: 60, response_body_timeout: 60, frame_write_timeout: 60,
                     stream_receive_window: 65_536,
                     connection_receive_window: 16_777_216)
        @host = host
        @requested_port = port
        @addresses = [].freeze
        @owner = ::Async::Task.current?
        @listeners = []
        @completion = ::Async::Promise.new
        # Application validates the server and a non-callable fallback.
        fallback = FallbackGuard.new(fallback) if fallback.respond_to?(:call)
        application = Application.new(server: server, fallback: fallback)
        @session = build_session(
          application: application, lifecycle_factory: application.method(:start_call),
          tls: tls, tls_provider: tls_provider,
          max_connections: max_connections, max_requests: max_requests,
          handshake_timeout: handshake_timeout, idle_timeout: idle_timeout,
          refresh_interval: refresh_interval, refresh_jitter: refresh_jitter,
          refresh_timeout: refresh_timeout, on_tls_error: on_tls_error, stop_accepting: method(:stop_accepting),
          keepalive: keepalive, max_concurrent_streams: max_concurrent_streams,
          request_body_timeout: request_body_timeout, response_body_timeout: response_body_timeout,
          frame_write_timeout: frame_write_timeout,
          stream_receive_window: stream_receive_window,
          connection_receive_window: connection_receive_window
        )
      end

      def inspect = "#<#{self.class} port=#{@port.inspect} session=#{@session.inspect}>"

      def start
        @session.send(:check_affinity!)
        raise LifecycleError, "Server session cannot start after close" if @closing

        # Caller-context rejection must not enter startup cleanup or retire a peer.
        @session.send(:preflight_start)
        # An overlapping caller must not rebind, reset @starting or abort the first.
        raise LifecycleError, "Host startup is already in progress" if @starting

        begin
          @starting = true
          @session.start
          return self if @bound

          @bound = bind
          raise LifecycleError, "Host closed during startup" if @closing

          @addresses = @bound.sockets.map(&:local_address).freeze
          @port = @addresses.first.ip_port
          @bound.sockets.each do |listener|
            break if @failure || @closing

            @session.schedule do |task|
              @listeners << task
              accept_loop(listener)
            end
          end
          raise @failure if @failure
          raise LifecycleError, "Host closed during startup" if @closing

          self
        rescue Exception => e # rubocop:disable Lint/RescueException -- retain the first startup cause through obligatory cleanup
          cause = @failure || e
          cleanup = exit_task
          begin
            Internal.join_complete(cleanup, failure: true) { cleanup.run if cleanup.status == :initialized }
          rescue Exception # rubocop:disable Lint/RescueException -- joins retain later caller interruptions until actual finalization
            raise if cleanup.failed?
          end
          raise cause
        ensure
          @starting = false
        end
      end

      # Ordinary shutdown waits only for listener stop, not session retirement.
      # Terminal failure waits for cooperative abort/join, then raises its cause.
      # Owner interruption is retained through cleanup; the cause still survives.
      def wait
        @session.send(:check_affinity!)
        raise LifecycleError, "Start or close the host before waiting" unless @bound || @closing

        error = @completion.wait
        if error
          Internal.join_complete(@exit_task, failure: true)
          raise error
        end

        nil
      end

      def close(**) = @session.close(**)
      def reload_tls(**) = @session.reload_tls(**)
      def refresh_tls = @session.refresh_tls
      def drain_tls(**) = @session.drain_tls(**)
      def tls_generation = @session.tls_generation
      def tls_status = @session.tls_status
      def peer_certificate = @session.peer_certificate

      private

      def build_session(**)
        ServerSession.new(**)
      end

      # Every resolved address shares one port. With port 0 the kernel picks
      # it for the first address; a later address that already has that port
      # in use restarts the whole bind, a bounded number of times.
      def bind
        addresses = Addrinfo.getaddrinfo(@host, @requested_port, nil, :STREAM)
        ephemeral = addresses.first.ip_port.zero?
        attempt = 0
        begin
          attempt += 1
          bind_addresses(addresses)
        rescue Errno::EADDRINUSE
          raise unless ephemeral && addresses.size > 1 && attempt < BIND_ATTEMPTS

          retry
        end
      end

      def bind_addresses(addresses)
        sockets = []
        port = 0
        addresses.each do |address|
          address = Addrinfo.tcp(address.ip_address, port) unless port.zero?
          socket = ::IO::Endpoint::Wrapper.default.bind(address)
          sockets << socket
          # Exec'd children must not inherit and keep serving the listener.
          socket.close_on_exec = true
          port = socket.local_address.ip_port
        end
        ::IO::Endpoint::BoundEndpoint.new(::IO::Endpoint.tcp(@host, port), sockets)
      rescue Exception # rubocop:disable Lint/RescueException -- no listener outlives a failed bind
        sockets.each { |socket| Internal.raw_close(socket) }
        raise
      end

      def accept_loop(listener)
        backoff = method(:accept_backoff)
        loop do
          raw, = @session.send(:accept_raw, listener, backoff)
          accepted = @session.accept(raw)
          @session.schedule { accepted.serve } if accepted
        end
      rescue StandardError => e
        unless @closing
          @failure ||= e
          exit_task.run unless @starting
        end
      end

      def accept_backoff(delay)
        ::Async::Task.current.sleep(delay)
      end

      def exit_task
        return @exit_task if @exit_task

        # Prior shared close owns its peers and grace. Decide before marking our
        # own teardown, so shutdown errors cannot become a new terminal cause.
        force = @failure || !@closing
        @closing = true
        # Store identity before eager execution; callers never own this sequence.
        @exit_task = ::Async::Task.new(@owner, transient: true, finished: false) do
          @session.abort if force
          stop_accepting
          @completion.resolve(@failure)
        end
      end

      def join_listeners
        @listeners.each { |listener| Internal.join_complete(listener) }
      end

      def stop_accepting
        @closing = true
        @bound&.close
        @listeners.each { |listener| Internal.cancel(listener) }
        join_listeners
        @completion.resolve(nil) unless @failure || @starting
      end
    end
  end
end
