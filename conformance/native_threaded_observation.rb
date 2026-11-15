# frozen_string_literal: true

module Conformance
  # Scopes enter on the adapter's caller thread and on the transport's owner
  # thread, so count updates are serialized.
  class ThreadedObservation < NativeObservation
    def initialize(name)
      super
      @guard = Mutex.new
    end

    def observe(phase, **fields)
      id = @guard.synchronize do
        @counts[phase][:entries] += 1
        @sequence += 1
      end
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      Telemetry.record("native_scope_enter", name: @name, phase: phase, id: id, thread: thread_role, **fields)
      yield
    rescue StandardError => e
      failure = e
      raise
    ensure
      @guard.synchronize { @counts[phase][:exits] += 1 }
      record_exit(phase, id, started, failure)
    end

    def complete(**facts)
      counts = @guard.synchronize { @counts.transform_values(&:dup) }
      Telemetry.record("native_closed_lifetime", name: @name, counts: counts, **facts)
    end

    private

    def thread_role = Fiber.scheduler ? "owner" : "caller"

    def record_exit(phase, id, started, failure)
      cause = failure&.cause
      Telemetry.record("native_scope_exit", name: @name, phase: phase, id: id,
                                            duration: Process.clock_gettime(Process::CLOCK_MONOTONIC) - started,
                                            code: failure.is_a?(ConnectRPC::Error) ? failure.code : nil,
                                            cause: cause&.class&.name, error_class: failure&.class&.name)
    end
  end

  # Observes the actual inner H2 Transport that the owner thread builds.
  # The owner's thread variable names the front that started it.
  module ThreadedInnerObservation
    OWNER = ConnectRPC::Async::ThreadedTransport.const_get(:OWNER)

    def initialize(**)
      front = Thread.current.thread_variable_get(OWNER)
      @observation = front.instance_variable_get(:@observation) if front.is_a?(ObservedThreadedTransport)
      @operations = []
      super
    end

    def closed_facts
      { state: @state, calls: @calls.size, acquiring: @acquiring.size, tasks_finished: tasks_finished?,
        streams_closed: @operations.all? { |item| !item[:response] || item[:response].stream.closed? },
        resources_finished: @resources.all?(&:finished?), raw_closed: raw_closed,
        lifetime_children_finished: @lifetime.children.to_a.all?(&:finished?) }
    end

    private

    # Unary operations own one task; stream operations own a task per operation.
    def tasks_finished?
      @operations.all? { |item| [item[:task], *item.fetch(:tasks, {}).keys].compact.all?(&:finished?) }
    end

    def raw_closed = @resources.empty? ? nil : @resources.all? { |resource| resource.raw.closed? }

    def dispatch_stream(operation, body)
      return super unless @observation

      @operations << operation
      @observation.observe(:dispatch, deadline: !operation.fetch(:request).deadline.nil?) { super }
    end

    def exchange(operation)
      return super unless @observation

      @operations << operation
      @observation.observe(:dispatch, deadline: !operation.fetch(:request).deadline.nil?) { super }
    end

    def stream_operation(operation, **, &)
      return super unless @observation

      @observation.observe(:operation) { super }
    end

    def cleanup(operation)
      return super unless @observation

      @observation.observe(:cleanup) { super }
    end
  end
  ConnectRPC::Async::ThreadedTransport.const_get(:Inner).prepend(ThreadedInnerObservation)

  class ObservedThreadedTransport < ConnectRPC::Async::ThreadedTransport
    def initialize(observation:, **)
      @observation = observation
      super(**)
    end

    def call(...)
      @observation.observe(:transport) { super }
    end

    def open_stream(...)
      @observation.observe(:transport) { super }
    end

    def open_grpc_stream(...)
      @observation.observe(:transport) { super }
    end

    # Front facts and the inner Transport's, read after the owner exits. A
    # transport whose owner never started has no inner Transport.
    def closed_facts
      engine = @engine
      return unstarted_facts unless engine

      exited = !engine.thread.join(5).nil?
      inner = engine.inner
      facts = inner ? inner.closed_facts : unstarted_facts
      facts.merge(front_state: front_state, admitted: @admission.count, mailboxes: engine.calls.size,
                  owner_started: true, owner_exited: exited, socket_interruption: nil)
    end

    private

    # No owner ever started, so no inner Transport or admission exists.
    def unstarted_facts
      { state: front_state, calls: 0, acquiring: 0, tasks_finished: true, streams_closed: true,
        resources_finished: true, raw_closed: nil, lifetime_children_finished: true, front_state: front_state,
        admitted: 0, mailboxes: 0, owner_started: false, owner_exited: true, socket_interruption: nil }
    end
  end
end
