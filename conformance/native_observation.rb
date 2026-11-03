# frozen_string_literal: true

require_relative "cancellation_observation"

module Conformance
  # Observe the real native receive boundary too: two official malformed-header
  # requests are rejected by the pinned backend before application dispatch.
  module NativeHeaderArrival
    def receive_initial_headers(headers, end_stream)
      name = headers.find { |key, _| key == "x-test-case-name" }&.last
      if name
        Telemetry.record("native_server_wire_headers", name: name, stream: id,
                                                       path: headers.find { |key, _| key == ":path" }&.last)
      end
      super
    end
  end
  ConnectRPC::Async.const_get(:Internal)::RequestStream.prepend(NativeHeaderArrival)

  module WebHTTP1Arrival
    def next_request
      request = super
      if request
        name = request.headers.header.find { |key, _| key.casecmp?("x-test-case-name") }&.last
        Telemetry.record("native_server_wire_headers", name: name, stream: nil, path: request.path) if name
      end
      request
    end
  end

  # Test-only telemetry around real library entry points. Returned close calls
  # never establish socket interruption or peer receipt.
  class NativeObservation
    def initialize(name)
      @name = name
      @sequence = 0
      @counts = Hash.new { |all, phase| all[phase] = { entries: 0, exits: 0 } }
    end

    def observe(phase, **fields)
      id = @sequence += 1
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      @counts[phase][:entries] += 1
      Telemetry.record("native_scope_enter", name: @name, phase: phase, id: id, **fields)
      yield
    rescue StandardError => e
      failure = e
      raise
    ensure
      @counts[phase][:exits] += 1
      cause = failure&.cause
      Telemetry.record("native_scope_exit", name: @name, phase: phase, id: id,
                                            duration: Process.clock_gettime(Process::CLOCK_MONOTONIC) - started,
                                            code: failure.is_a?(ConnectRPC::Error) ? failure.code : nil,
                                            cause: cause&.class&.name, error_class: failure&.class&.name)
    end

    def complete(**facts)
      Telemetry.record("native_closed_lifetime", name: @name, counts: @counts, **facts)
    end

    def cancel(source, timing)
      changed = source.cancel
      Telemetry.record("native_source_transition", name: @name, timing: timing, changed: changed,
                                                   reason: source.token.reason)
    end
  end

  class ObservedNativeTransport < ConnectRPC::Async::Transport
    def initialize(observation:, **)
      @observation = observation
      @operations = []
      super(**)
    end

    def open_grpc_stream(request, &)
      @observation.observe(:transport) { super }
    end

    def open_stream(request, &)
      @observation.observe(:transport) { super }
    end

    def closed_facts
      { state: @state, calls: @calls.size, acquiring: @acquiring.size,
        tasks_finished: @operations.all? { |item| item.fetch(:tasks).keys.all?(&:finished?) },
        streams_closed: streams_closed?,
        resources_finished: @resources.all?(&:finished?),
        raw_closed: @resources.empty? ? nil : @resources.all? { |resource| resource.raw.closed? },
        lifetime_children_finished: @lifetime.children.to_a.all?(&:finished?), socket_interruption: nil }
    end

    private

    def streams_closed?
      @operations.all? { |item| !item[:response] || item[:response].stream.closed? }
    end

    def dispatch_stream(operation, body)
      @operations << operation
      @observation.observe(:dispatch, deadline: !operation.fetch(:request).deadline.nil?) { super }
    end

    def stream_operation(operation, **, &)
      @observation.observe(:operation) { super }
    end

    def cleanup(operation)
      @observation.observe(:cleanup) { super }
    end
  end

  class ObservedNativeClient < ConnectRPC::Client
    def initialize(observation:, **)
      @observation = observation
      super(**)
    end

    def call(...)
      @observation.observe(:library) { super }
    end

    def server_stream(...)
      @observation.observe(:library) { super }
    end

    def client_stream(...)
      @observation.observe(:library) { super }
    end

    def bidi_stream(...)
      @observation.observe(:library) { super }
    end
  end

  class NativeSubscriber
    def initialize
      @sequence = 0
    end

    def start(call)
      NativeObserver.new(call, @sequence += 1)
    end
  end

  class ObservedNativeServer < ConnectRPC::Server
    def call(**options)
      return super if ConnectRPC.const_get(:GRPCWeb).intent?(options.fetch(:headers))

      observe_dispatch(options) { super }
    end

    def call_grpc(**options)
      observe_dispatch(options) { super }
    end

    def call_grpc_web(**options)
      observe_dispatch(options) { super }
    end

    def call_stream(**options)
      observe_dispatch(options) { super }
    end

    private

    def observe_dispatch(options)
      @dispatch_sequence = (@dispatch_sequence || 0) + 1
      id = @dispatch_sequence
      name = options.fetch(:headers)["x-test-case-name"]&.first
      Telemetry.record("native_server_dispatch_enter", id: id, name: name)
      yield
    ensure
      Telemetry.record("native_server_dispatch_exit", id: id, name: name)
    end
  end

  class NativeObserver
    def initialize(call, id)
      @id = id
      @name = call.carrier["x-test-case-name"]&.first
      Telemetry.record("native_server_admitted", name: @name, id: @id, method: call.method&.path)
    end

    def processing_finished(outcome)
      Telemetry.record("native_server_processing", name: @name, id: @id, code: outcome.code,
                                                   error_class: outcome.error&.class&.name)
    end

    def finished(completion)
      Telemetry.record("native_server_finished", name: @name, id: @id, delivery: completion.delivery,
                                                 processing: completion.processing_state)
    end
  end
end
