# frozen_string_literal: true

module ConnectRPC
  module Falcon
    class Server < ::Falcon::Server
      def initialize(server:, endpoint:, rack_app: nil, tls: nil, tls_provider: nil,
                     utilization_registry: nil, max_connections: 128, max_requests: 128,
                     handshake_timeout: 10, idle_timeout: 60, refresh_interval: 60,
                     refresh_jitter: 0.1, refresh_timeout: 10, on_tls_error: nil)
        configured = if endpoint.is_a?(::IO::Endpoint::BoundEndpoint)
                       endpoint.endpoint
                     else
                       endpoint
                     end
        unless configured.is_a?(Endpoint) && (configured.scheme == "https") == (!tls.nil? || !tls_provider.nil?)
          raise ArgumentError, "Listener scheme and TLS configuration must agree"
        end

        fallback = ::Falcon::Server.rack_middleware(RackGuard.new(rack_app)) unless rack_app.nil?
        application = ::ConnectRPC::Async::Application.new(server: server, fallback: fallback)
        listener_application = ListenerApplication.new(application, configured.scheme)
        @listener_application = listener_application
        utilization_registry ||= ::Async::Utilization::Registry.new
        @connection_total = utilization_registry.metric(:connections_total)
        @connection_active = utilization_registry.metric(:connections_active)
        @request_total = utilization_registry.metric(:requests_total)
        @request_active = utilization_registry.metric(:requests_active)
        @session = ::ConnectRPC::Async::ServerSession.new(
          application: method(:call), lifecycle_factory: application.method(:start_call),
          tls: tls, tls_provider: tls_provider,
          max_connections: max_connections, max_requests: max_requests,
          handshake_timeout: handshake_timeout, idle_timeout: idle_timeout,
          refresh_interval: refresh_interval, refresh_jitter: refresh_jitter,
          refresh_timeout: refresh_timeout, on_tls_error: on_tls_error,
          alpn_protocols: %w[h2 http/1.1], http1: HTTP1Resource.method(:new),
          stop_accepting: method(:stop_accepting), on_request_rejected: -> { @request_total.increment }
        )
        # Preserve parent-bound descriptors while installing a worker-local
        # accept wrapper through IO::Endpoint's supported wrapper option.
        @accept_wrapper = AcceptWrapper.new(@session, on_reject: -> { @connection_total.increment })
        endpoint = @owned_bound = endpoint.bound unless endpoint.is_a?(::IO::Endpoint::BoundEndpoint)
        wrapped = endpoint.with(wrapper: @accept_wrapper)
        super(listener_application, wrapped, protocol: configured.protocol, scheme: configured.scheme,
                                             utilization_registry: utilization_registry)
      rescue StandardError
        @session&.abort
        raise
      end

      def run
        @session.start
        @run_task = super
      end

      def accept(peer, _address, **)
        @connection_total.increment
        @session.schedule { @connection_active.track { peer.wait_closed } }
        peer.serve
      end

      def close(grace: 5)
        @session.close(grace: grace)
      end

      def abort = @session.abort

      def call(request, lifecycle: nil)
        @request_total.increment
        @request_active.increment
        response = @listener_application.call(request, lifecycle: lifecycle)
        if response&.body
          # Empty bodies can still own completion callbacks. Preserve their
          # wire length while retaining the metric until close finishes.
          response.body = ::Falcon::Body::RequestFinished.new(response.body, @request_active)
          wrapped = true
        end
        response
      ensure
        @request_active.decrement unless wrapped
      end

      def reload_tls(**) = @session.reload_tls(**)
      def refresh_tls = @session.refresh_tls
      def drain_tls(**) = @session.drain_tls(**)
      def tls_generation = @session.tls_generation
      def tls_status = @session.tls_status
      def peer_certificate = @session.peer_certificate

      private

      def stop_accepting
        # Stop this worker's accept tasks after session validation. Accepted
        # peers have an independent lifetime; parent-bound listeners stay open.
        @accept_wrapper&.stop
        @owned_bound&.close
      end
    end
  end
end
