# frozen_string_literal: true

require "spec_helper"
require_relative "support/retirement_preemption"

RSpec.describe "Internal completion preemption" do
  def completion_join(task, **, &)
    AsyncHelpers::INTERNAL.join_complete(task, **, &)
  end

  it_behaves_like "settled completion join"
end

RSpec.describe "Internal interruptible completion join" do
  it "raises the joiner's interruption at once and leaves the target running" do
    reactor do |task|
      reader, writer = Socket.pair(:UNIX, :STREAM, 0)
      child = nil
      root = task.async(finished: false) do |parent|
        child = parent.async(finished: false) { reader.readpartial(1) }
      end
      first = RuntimeError.new("interruptible join interruption")
      # The joiner suspends on the root's live child before this returns.
      joiner = task.async(finished: false) do
        AsyncHelpers::INTERNAL.join_complete(root, interruptible: true)
      rescue StandardError => e
        e
      end
      expect(joiner).not_to be_finished
      Fiber.scheduler.raise(joiner.fiber, first)
      expect(joiner).to be_finished
      expect(joiner.result).to equal(first)
      expect(root).not_to be_finished
      expect(child).to be_alive
      writer.write("x")
      AsyncHelpers::INTERNAL.join_complete(root)
      expect([root, child]).to all(be_finished)
    ensure
      reader&.close
      writer&.close
    end
  end

  # The target has settled, so the joiner waits on its child's unwind.
  it "raises an interruption at once while it waits on a settled target's unwinding child" do
    reactor do |task|
      entered = Async::Promise.new
      release = Async::Queue.new
      waits = []
      trace = TracePoint.new(:call) do |event|
        waits << [Fiber.current, event.self] if event.method_id == :wait && event.defined_class == Async::Task
      end
      root = task.async(finished: false) do |parent|
        parent.async(finished: false) do |current|
          entered.resolve(current)
          Async::Condition.new.wait
        ensure
          release.dequeue
        end
        Async::Condition.new.wait
      end
      child = entered.wait
      first = RuntimeError.new("unwinding child interruption")
      joiner = Async::Task.new(task, finished: false) do
        AsyncHelpers::INTERNAL.join_complete(root, interruptible: true) { root.cancel }
      rescue StandardError => e
        e
      end
      trace.enable(target_thread: Thread.current)
      joiner.run
      joiner_fiber = joiner.fiber
      wait_until(task, "the joiner to wait on the unwinding child") do
        waits.any? { |fiber, target| fiber.equal?(joiner_fiber) && target.equal?(child) } || joiner.finished?
      end
      trace.disable
      expect(root).to be_cancelled
      expect(joiner).not_to be_finished
      Fiber.scheduler.raise(joiner_fiber, first)
      expect(joiner).to be_finished
      expect(joiner.result).to equal(first)
      expect(child).to be_alive
      release.enqueue(true)
      AsyncHelpers::INTERNAL.join_complete(root)
      expect(child).to be_cancelled
    ensure
      trace&.disable
      release&.enqueue(true)
    end
  end
end
