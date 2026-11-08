# frozen_string_literal: true

require_relative "retirement_helpers"

RSpec.shared_examples "retirement close corrections" do |protocol|
  include RetirementHelpers

  it "allows #{protocol} resource cleanup reentry from the actual raw close boundary" do
    reactor do |task|
      with_retirement_connection(task, protocol: protocol) do |state|
        resource = state.fetch(:resource)
        allow(resource.raw).to receive(:close).and_wrap_original do |close|
          resource.abort
          close.call
        end
        aborter = task.async(finished: false) { resource.abort }
        state.fetch(:gate).entered.dequeue
        expect(resource.raw).to be_closed
        expect(aborter).not_to be_finished
        state.fetch(:gate).release
        join_retirement_tasks(task, aborter, state.fetch(:handler))
        expect(retirement_complete?(resource)).to be(true)
      end
    end
  end

  it "requests owned #{protocol} close reentry after cancellation without joining itself" do
    reactor do |task|
      with_retirement_connection(task, protocol: protocol) do |state|
        returned = false
        state[:on_cleanup] = lambda do
          state.fetch(:session).close(grace: 0)
          returned = true
        rescue Async::Cancel
          # The original implementation needs this explicit fixture unblock.
          nil
        end
        aborter = task.async(finished: false) { state.fetch(:session).abort }
        competitor = task.async(finished: false) { state.fetch(:host).close(grace: 0) }
        begin
          entered = task.with_timeout(2) { state.fetch(:gate).entered.dequeue }
        rescue Async::TimeoutError
          # Preserve the intended reentry assertion when the old wait cycles.
          nil
        end
        expect(returned).to be(true), "cancelled handler close must return before its cleanup gate"
        expect(entered).to equal(state.fetch(:handler))
        expect([aborter, competitor]).to all(satisfy { |entry| !entry.finished? })
        state.fetch(:gate).release
        join_retirement_tasks(task, aborter, competitor, state.fetch(:handler))
        expect(competitor.result).to be(false)
      ensure
        if !returned && state.fetch(:handler).alive?
          Fiber.scheduler.raise(state.fetch(:handler).fiber, Async::Cancel.new("fixture unblock"))
        end
        state.fetch(:gate).release
      end
    end
  end

  it "forces #{protocol} retirement on external interruption during actual nonzero drain" do
    reactor do |task|
      with_retirement_connection(task, protocol: protocol) do |state|
        session, resource = state.values_at(:session, :resource)
        first = RuntimeError.new("first graceful close interruption")
        later = Async::Cancel.new("later graceful close interruption")
        joiner = task.async(finished: false) do
          session.close(grace: 3600)
        rescue Exception => e # rubocop:disable Lint/RescueException -- exact caller exception identity
          e
        end
        wait_until(task, "real nonzero resource drain started") { resource.instance_variable_get(:@drain_task) }
        expect(resource).not_to be_finished
        expect(state.fetch(:handler)).to be_alive
        Fiber.scheduler.raise(joiner.fiber, first)
        task.yield
        expect(resource).to be_finished, "external interruption must force retirement without grace expiry"
        state.fetch(:gate).entered.dequeue
        competitor = task.async(finished: false) { session.close(grace: 0) }
        Fiber.scheduler.raise(joiner.fiber, later)
        expect([joiner, competitor]).to all(satisfy { |entry| !entry.finished? })
        expect(retirement_complete?(resource)).to be(false)
        state.fetch(:gate).release
        join_retirement_tasks(task, joiner, competitor, state.fetch(:handler))
        expect(joiner.result).to equal(first)
        expect(competitor.result).to be(false)
      ensure
        state.fetch(:gate).release
        # A failing pre-correction assertion must not wait for the long grace.
        session&.abort
      end
    end
  end

  it "blocks the #{protocol} completion join on actual cleanup descendants" do
    reactor do |task|
      with_retirement_connection(task, protocol: protocol) do |state|
        child_gate = RetirementHelpers::CleanupGate.new
        root_release = Async::Queue.new
        root = task.async(finished: false) do |current|
          current.async(finished: false) { child_gate.hold }
          root_release.dequeue
        end
        child = child_gate.entered.dequeue
        root_release.enqueue(true)
        root.wait
        expect(root).not_to be_finished
        resource = state.fetch(:resource)
        joiner = Async::Task.new(task, finished: false) do
          if protocol == :h1
            resource.send(:join, root)
          else
            AsyncHelpers::INTERNAL.join_complete(root)
          end
        end
        yields = 0
        trace = TracePoint.new(:call) do |event|
          yields += 1 if event.self.equal?(joiner) && event.method_id == :yield
        end
        trace.enable
        joiner.run
        task.yield
        task.yield
        expect(yields).to eq(0), "completion must wait on live descendants instead of ready-queue polling"
        expect(joiner).not_to be_finished
        child_gate.release
        join_retirement_tasks(task, joiner, child, root)
      ensure
        trace&.disable
        root_release&.enqueue(true)
        child_gate&.release
      end
    end
  end
end
