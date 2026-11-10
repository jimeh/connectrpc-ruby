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
        begin
          @session.start
        rescue StandardError
          @session.abort
          raise
        end
        return self if @bound

        begin
          @starting = true
          @bound = bind
          @addresses = @bound.sockets.map(&:local_address).freeze
          @port = @addresses.first.ip_port
          @bound.sockets.each do |listener|
            break if @failure

            @listeners << @session.schedule { accept_loop(listener) }
          end
          @starting = false
          if @failure
            supervise_failure
            wait
          end
          self
        rescue StandardError => e
          @starting = false
          if @failure
            supervise_failure
            wait
          else
            @session.abort
            join_listeners
            @completion.resolve(nil)
          end
          raise e
        end
      end

      # Ordinary shutdown waits only for listener stop, not session retirement.
      # Terminal failure waits for cooperative abort/join, then raises its cause.
      # Owner cancellation can interrupt that cleanup; the cause still survives.
      def wait
        @session.send(:check_affinity!)
        raise LifecycleError, "Start or close the host before waiting" unless @bound || @closing

        error = @completion.wait
        raise error if error

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
          supervise_failure unless @starting
        end
      end

      def accept_backoff(delay)
        ::Async::Task.current.sleep(delay)
      end

      def supervise_failure
        return if @supervising

        @supervising = true
        # This task must survive session-lifetime and start-caller cancellation.
        @owner.async(finished: false) do
          @session.abort
          join_listeners
        ensure
          @completion.resolve(@failure)
        end
      end

      def join_listeners
        @listeners.each { |listener| Internal.join(listener) }
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
