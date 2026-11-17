# frozen_string_literal: true

module ConnectRPC
  module Falcon
    class Service < ::Falcon::Service::Server
      def start
        unless @evaluator.endpoint.is_a?(Endpoint)
          raise ArgumentError, "#{name} endpoint is not a ConnectRPC::Falcon::Endpoint; " \
                               "include ConnectRPC::Falcon::Environment after Falcon's environments"
        end

        super
      end

      def run(_instance, evaluator, listener)
        # This enclosing task's ensure executes on Falcon's ordinary managed
        # Interrupt route before its children are cancelled by the container.
        ready = ::Async::Queue.new
        ::Async::Task.current.async do |task|
          grace = ConnectRPC::Async.const_get(:Internal).seconds(evaluator.connectrpc_close_grace,
                                                                 "connectrpc_close_grace", zero: true)
          server = evaluator.make_server(listener.endpoint)
          server.run
          ready.enqueue([server, nil])
          announced = true
          task.children&.to_a&.each(&:wait)
        rescue StandardError => e
          if announced
            failure = e
            raise
          end

          ready.enqueue([nil, e])
        rescue Exception => e # rubocop:disable Lint/RescueException -- only a managed stop closes gracefully
          failure = e
          raise
        ensure
          if failure && !managed_stop?(failure)
            server&.abort
          else
            server&.close(grace: grace)
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
      # Keeps a Falcon Rack facet's config.ru application when included after it.
      def rack_app = defined?(super) ? super : nil
      def tls = nil
      def tls_provider = nil
      def server_options = {}
      def connectrpc_close_grace = 5

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
