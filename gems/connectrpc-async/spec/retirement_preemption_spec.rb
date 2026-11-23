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
end
