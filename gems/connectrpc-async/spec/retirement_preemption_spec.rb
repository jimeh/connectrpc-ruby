# frozen_string_literal: true

require "spec_helper"
require_relative "support/retirement_preemption"

RSpec.describe "Internal completion preemption" do
  def completion_join(task, **, &)
    AsyncHelpers::INTERNAL.join_complete(task, **, &)
  end

  def completion_state(task)
    task.instance_variable_get(:@connectrpc_async_completion)
  end

  it_behaves_like "R3 original-source finalization orderings"
  it_behaves_like "completion progress observation"
end

RSpec.describe "Internal completion latch interruption" do
  it "runs on_interrupt once for the first latch-wait interruption" do
    reactor do |task|
      reader, writer = Socket.pair(:UNIX, :STREAM, 0)
      started = Async::Promise.new
      root = task.async(finished: false) do |parent|
        parent.async(finished: false) { reader.readpartial(1) }
        started.resolve(true)
        Async::Condition.new.wait
      end
      started.wait
      first = RuntimeError.new("first latch interruption")
      requests = []
      joiner = Async::Task.new(task, finished: false) do
        AsyncHelpers::INTERNAL.join_complete(root, on_interrupt: -> { requests << first }) { root.cancel }
      rescue StandardError => e
        e
      end
      waiter = task.async(finished: false) do
        root.wait
        state = root.instance_variable_get(:@connectrpc_async_completion)
        expect(state).not_to be_nil
        expect(state).not_to be_terminal
        expect(root).to be_alive
        Fiber.scheduler.raise(joiner.fiber, first)
      end
      joiner.run
      expect(joiner.wait).to equal(first)
      expect(requests).to eq([first])
      expect([root, joiner, waiter]).to all(be_finished)
    ensure
      reader&.close
      writer&.close
    end
  end
end
