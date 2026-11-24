# frozen_string_literal: true

# Cancels a connection's cleanup task while it joins a held handler, over a
# `RetirementHelpers#with_retirement_connection` fixture.
module CleanupInterruption
  # The handler and every task above it.
  def lineage(task)
    tasks = []
    while task
      tasks << task
      task = task.parent
    end
    tasks
  end

  # Whether the current task is the connection's cleanup task joining one of
  # `tasks`.
  def cleanup_joining?(resource, target, tasks)
    cleanup = resource.instance_variable_get(:@cleanup)
    return false unless cleanup&.current?

    tasks.any? { |owned| owned.equal?(target) }
  end

  # A trace that resolves `joining` with the connection's cleanup task once
  # that task starts joining `handler` or one of its ancestors.
  def cleanup_join_trace(resource, handler, joining)
    tasks = lineage(handler)
    completion = ConnectRPC::Async.const_get(:Internal).const_get(:CompletionJoin)
    TracePoint.new(:call) do |event|
      next unless event.self.equal?(completion) && event.method_id == :call
      next unless cleanup_joining?(resource, event.binding.local_variable_get(:task), tasks)

      joining.resolve(resource.instance_variable_get(:@cleanup))
    end
  end

  # Retirement still completes once the handler leaves, the descriptor closes
  # only after the handler has finished, and the cleanup task ends cancelled.
  def expect_cancelled_cleanup_to_retire(task, state)
    resource, gate, handler = state.values_at(:resource, :gate, :handler)
    raw = resource.raw
    closed_after_handler = []
    raw.define_singleton_method(:close) do
      closed_after_handler << handler.finished? if closed_after_handler.empty?
      super()
    end
    joining = Async::Promise.new
    closer = cleanup = nil
    cleanup_join_trace(resource, handler, joining).enable do
      closer = task.async(finished: false) { state.fetch(:accepted).close }
      expect(gate.entered.dequeue).to equal(handler)
      cleanup = joining.wait
    end
    waiter = task.async(finished: false) { state.fetch(:accepted).wait_closed }
    AsyncHelpers::INTERNAL.cancel(cleanup)
    expect(cleanup).not_to be_finished
    expect(raw).not_to be_closed
    gate.release
    wait_until(task, "the cleanup task to end") { cleanup.finished? }
    expect(retirement_complete?(resource)).to be(true)
    expect(raw).to be_closed
    expect(closed_after_handler).to eq([true])
    expect(cleanup).to be_cancelled
    join_retirement_tasks(task, closer, waiter, handler)
  end
end
