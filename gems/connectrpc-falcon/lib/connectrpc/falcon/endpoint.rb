# frozen_string_literal: true

module ConnectRPC
  module Falcon
    # HTTPS describes the listener without compiling TLS in the parent process.
    class Endpoint < ::Async::HTTP::Endpoint
      def initialize(url, endpoint = nil, **options)
        unless %w[http https].include?(url.scheme) && url.hostname && !url.userinfo && !url.query && !url.fragment &&
               ["",
                "/"].include?(url.path) && !options.keys.intersect?(%i[ssl_context tls_configuration scheme protocol])
          raise ArgumentError, "Expected a raw HTTP(S) listener endpoint"
        end

        super
      end

      def build_endpoint(endpoint = nil)
        endpoint || tcp_endpoint
      end
    end

    # socket_accept runs before Wrapper schedules the accepted task, including
    # connections with no ClientHello yet. Never select a generation via SNI.
    class AcceptWrapper < ::IO::Endpoint::Wrapper
      def initialize(session, on_reject:)
        super()
        @session = session
        @on_reject = on_reject
        @listeners = {}
      end

      def socket_accept(server)
        @listeners[::Async::Task.current] = true
        loop do
          raw, address = super
          accepted = @session.accept(raw)
          return [accepted, address] if accepted

          @on_reject.call
        end
      end

      def schedule(&)
        if @listeners.key?(::Async::Task.current)
          @session.schedule(&)
        else
          super
        end
      end

      def stop
        @listeners.each_key(&:stop)
        @listeners.clear
      end
    end
    private_constant :AcceptWrapper
  end
end
