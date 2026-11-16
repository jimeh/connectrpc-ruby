# frozen_string_literal: true

require "spec_helper"
require_relative "../../connectrpc-async/spec/support/retirement_preemption"

RSpec.describe "Falcon local completion preemption" do
  def completion_join(task, **, &)
    ConnectRPC::Falcon.const_get(:HTTP1Resource).allocate.send(:join, task, **, &)
  end

  def completion_state(task)
    task.instance_variable_get(:@connectrpc_async_completion)
  end

  it_behaves_like "R3 original-source finalization orderings"
  it_behaves_like "completion progress observation"
end

RSpec.describe "Falcon and Async completion observation" do
  it "shares one per-instance observation between Falcon and Async joins on the same task" do
    reactor do |task|
      reader, writer = Socket.pair(:UNIX, :STREAM, 0)
      started = Async::Promise.new
      root = task.async(finished: false) do |parent|
        parent.async(finished: false) { reader.readpartial(1) }
        started.resolve(true)
        Async::Condition.new.wait
      end
      started.wait
      shared = task.async(finished: false) { AsyncHelpers::INTERNAL.join_complete(root) }
      local = task.async(finished: false) do
        ConnectRPC::Falcon.const_get(:HTTP1Resource).allocate.send(:join, root) { root.cancel }
      end
      expect(local.wait).to be_nil
      expect(shared.wait).to be_nil
      expect(root.instance_variables.grep(/completion/)).to eq([:@connectrpc_async_completion])
      expect(root.instance_variable_get(:@connectrpc_async_completion)).to be_terminal
      consume = AsyncHelpers::INTERNAL.const_get(:CompletionJoin).const_get(:Consume)
      expect(root.singleton_class).to include(consume)
      expect([root, local, shared]).to all(be_finished)
    ensure
      reader&.close
      writer&.close
    end
  end
end
