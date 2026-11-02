# frozen_string_literal: true

require "English"

module ConnectRPC
  module Falcon
    class Service < ::Falcon::Service::Server
      def run(_instance, evaluator, listener)
        # This enclosing task's ensure executes on Falcon's ordinary managed
        # Interrupt route before its children are cancelled by the container.
        ready = ::Async::Queue.new
        ::Async::Task.current.async do |task|
          server = evaluator.make_server(listener.endpoint)
          server.run
          ready.enqueue([server, nil])
          announced = true
          task.children&.to_a&.each(&:wait)
        rescue StandardError => e
          raise if announced

          ready.enqueue([nil, e])
        ensure
          if $ERROR_INFO && !managed_stop?($ERROR_INFO)
            server&.abort
          else
            server&.close
          end
        end
        server, error = ready.dequeue
        raise error if error

        # Service::Server keeps this for its normal statistics/process title,
        # and cannot declare readiness until the worker's initial load succeeds.
        server
      end

      private

      def managed_stop?(error)
        while error
          return true if error.is_a?(Interrupt)

          error = error.cause
        end
        false
      end
    end

    module Environment
      include ::Falcon::Environment::Server

      def service_class = Service
      def endpoint = Endpoint.parse(url, **endpoint_options)
      def rack_app = nil
      def tls = nil
      def tls_provider = nil
      def server_options = {}

      def connectrpc_server
        raise NotImplementedError, "Define the worker-local connectrpc_server"
      end

      def make_server(bound)
        Server.new(server: connectrpc_server, endpoint: bound, rack_app: rack_app, tls: tls,
                   tls_provider: tls_provider, utilization_registry: self[:utilization_registry], **server_options)
      end
    end
  end
end
