# frozen_string_literal: true

module Conformance
  class NativeHTTP1Observation < NativeObservation
    def response(connection, socket)
      Telemetry.record("native_http1_response", name: @name, version: connection.response_version,
                                                tls: socket.is_a?(OpenSSL::SSL::SSLSocket),
                                                alpn: socket.respond_to?(:alpn_protocol) ? socket.alpn_protocol : nil)
    end
  end

  # H1 has no H2 response.stream. Observe its actual leases, writer/setup and
  # operation tasks, framed EOF, raw sockets and status-line/ALPN boundaries.
  class ObservedNativeHTTP1Transport < ConnectRPC::Async::HTTP1Transport
    def initialize(observation:, **)
      @observation = observation
      @operations = []
      super(**)
    end

    def call(...)
      @observation.observe(:transport) { super }
    end

    def open_stream(...)
      @observation.observe(:transport) { super }
    end

    def closed_facts
      resources = (@resources + @operations.filter_map { |operation| operation[:resource] }).uniq
      { state: @state, calls: @calls.size, leases: @leases.size, idle: @idle.size,
        tasks_finished: @operations.all? { |item| operation_finished?(item) },
        operations_retired: @operations.all? { |item| item[:cleaned] },
        resources_finished: resources.all?(&:finished?),
        raw_closed: raw_closed_fact(resources),
        lifetime_children_finished: @lifetime.children.to_a.all?(&:finished?), socket_interruption: nil }
    end

    private

    def operation_finished?(item)
      [item[:writer], item[:resource]&.setup, *item.fetch(:tasks).keys].compact.all?(&:finished?)
    end

    def raw_closed_fact(resources)
      return if resources.empty?

      resources.all? { |resource| resource.raw.nil? || resource.raw.closed? }
    end

    def with_operation(request, &)
      @observation.observe(:admission) { super }
    end

    def dispatch(operation, **)
      @operations << operation
      @observation.observe(:dispatch, deadline: !operation.fetch(:request).deadline.nil?) { super }
    end

    def operate(operation, **, &)
      @observation.observe(:operation) { super }
    end

    def response_head(operation, **)
      had_head = operation[:head]
      super.tap do
        @observation.response(operation.fetch(:connection), operation.fetch(:resource).socket) unless had_head
      end
    end

    def cleanup(operation, **)
      @observation.observe(:cleanup) { super }
    end
  end
end
