# frozen_string_literal: true

module ConnectRPC
  module Async
    # Strict HTTP/2 listener composing the same worker session as other hosts.
    class Host
      ACCEPT_RETRY_DELAYS = [0.05, 0.1, 0.2, 0.4, 0.5].freeze
      ACCEPT_RETRY_ERRORS = [Errno::EMFILE, Errno::ENFILE, Errno::ENOBUFS, Errno::ENOMEM,
                             Errno::ECONNABORTED, Errno::EINTR].freeze
      private_constant :ACCEPT_RETRY_DELAYS, :ACCEPT_RETRY_ERRORS

      attr_reader :port, :addresses

      def initialize(server:, host: "127.0.0.1", port: 0, tls: nil, tls_provider: nil,
                     max_connections: 128, max_requests: 128, handshake_timeout: 10, idle_timeout: 60,
                     refresh_interval: 60, refresh_jitter: 0.1, refresh_timeout: 10, on_tls_error: nil)
        @host = host
        @requested_port = port
        @addresses = [].freeze
        @owner = ::Async::Task.current?
        @listeners = []
        @completion = ::Async::Promise.new
        application = Application.new(server: server)
        @session = build_session(
          application: application, lifecycle_factory: application.method(:start_call),
          tls: tls, tls_provider: tls_provider,
          max_connections: max_connections, max_requests: max_requests,
          handshake_timeout: handshake_timeout, idle_timeout: idle_timeout,
          refresh_interval: refresh_interval, refresh_jitter: refresh_jitter,
          refresh_timeout: refresh_timeout, on_tls_error: on_tls_error, stop_accepting: method(:stop_accepting)
        )
      end

      def inspect = "#<#{self.class} port=#{@port.inspect} session=#{@session.inspect}>"

      def start
        @session.send(:check_affinity!)
        raise LifecycleError, "Server session cannot start after close" if @closing

        # Caller-context rejection must not enter startup cleanup or retire a peer.
        @session.send(:preflight_start)

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

      def bind
        ::IO::Endpoint.tcp(@host, @requested_port).bound
      end

      def accept_loop(listener)
        failures = 0
        loop do
          begin
            raw, = listener.accept
          rescue *ACCEPT_RETRY_ERRORS
            raise if failures == ACCEPT_RETRY_DELAYS.size
            return if @closing

            accept_backoff(ACCEPT_RETRY_DELAYS.fetch(failures))
            failures += 1
            next
          end
          failures = 0
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
