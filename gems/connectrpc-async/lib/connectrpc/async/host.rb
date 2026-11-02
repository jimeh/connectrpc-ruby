# frozen_string_literal: true

module ConnectRPC
  module Async
    # Strict HTTP/2 listener composing the same worker session as other hosts.
    class Host
      attr_reader :port, :addresses

      def initialize(server:, host: "127.0.0.1", port: 0, tls: nil, tls_provider: nil,
                     max_connections: 128, max_requests: 128, handshake_timeout: 10, idle_timeout: 60,
                     refresh_interval: 60, refresh_jitter: 0.1, refresh_timeout: 10, on_tls_error: nil)
        @host = host
        @requested_port = port
        @addresses = [].freeze
        @session = build_session(
          application: Application.new(server: server), tls: tls, tls_provider: tls_provider,
          max_connections: max_connections, max_requests: max_requests,
          handshake_timeout: handshake_timeout, idle_timeout: idle_timeout,
          refresh_interval: refresh_interval, refresh_jitter: refresh_jitter,
          refresh_timeout: refresh_timeout, on_tls_error: on_tls_error, stop_accepting: method(:stop_accepting)
        )
      end

      def inspect = "#<#{self.class} port=#{@port.inspect} session=#{@session.inspect}>"

      def start
        @session.start
        return self if @bound

        @bound = ::IO::Endpoint.tcp(@host, @requested_port).bound
        @addresses = @bound.sockets.map(&:local_address).freeze
        @port = @addresses.first.ip_port
        @listeners = @bound.sockets.map do |listener|
          @session.schedule { accept_loop(listener) }
        end
        self
      rescue StandardError
        @session.abort
        raise
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

      def accept_loop(listener)
        loop do
          raw, = listener.accept
          accepted = @session.accept(raw)
          @session.schedule { accepted.serve } if accepted
        end
      rescue IOError, SystemCallError
        raise unless @closing
      end

      def stop_accepting
        @closing = true
        @bound&.close
        @listeners&.each(&:stop)
      end
    end
  end
end
