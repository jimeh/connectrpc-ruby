# frozen_string_literal: true

require_relative "retirement_helpers"

RSpec.shared_examples "retirement close corrections" do |protocol|
  include RetirementHelpers

  it "joins immediate #{protocol} cancellation finalization after promise resolution" do
    reactor do |task|
      with_retirement_connection(task, protocol: protocol) do |state|
        reader, writer = Socket.pair(:UNIX, :STREAM, 0)
        root = task.async(finished: false) do |parent|
          parent.async(finished: false) { reader.readpartial(1) }
          Async::Condition.new.wait
        end
        child = root.children.to_a.fetch(0)
        original_fibers = [root.fiber, child.fiber]
        root.cancel
        expect(root.wait).to be_nil
        expect(root).to be_alive
        expect(root.children.to_a).to be_empty
        puts "F1 #{protocol}: resolved=true alive=#{root.alive?} children=#{root.children&.size || 0}"
        yields = 0
        trace = TracePoint.new(:call) do |event|
          yields += 1 if event.self.equal?(task) && event.method_id == :yield
        end
        trace.enable
        if protocol == :h1
          state.fetch(:resource).send(:join, root)
        else
          AsyncHelpers::INTERNAL.join_complete(root)
        end
        trace.disable
        expect(yields).to eq(0)
        expect(root).to be_finished
        expect(child).to be_finished
        expect(original_fibers).to all(satisfy { |fiber| !fiber.alive? })
        state.fetch(:gate).release
        state.fetch(:resource).abort
        expect(retirement_complete?(state.fetch(:resource))).to be(true)
      ensure
        trace&.disable
        reader&.close
        writer&.close
        task.yield
        join_retirement_tasks(task, root, child)
      end
    end
  end

  it "retains the first #{protocol} joiner interruption during finalization observation" do
    reactor do |task|
      with_retirement_connection(task, protocol: protocol) do |state|
        reader, writer = Socket.pair(:UNIX, :STREAM, 0)
        root = task.async(finished: false) do |parent|
          parent.async(finished: false) { reader.readpartial(1) }
          Async::Condition.new.wait
        end
        child = root.children.to_a.fetch(0)
        first = Async::Cancel.new("first finalization interruption")
        joiner = Async::Task.new(task, finished: false) do
          root.cancel
          expect(root.wait).to be_nil
          expect(root).to be_alive
          expect(root.children.to_a).to be_empty
          if protocol == :h1
            state.fetch(:resource).send(:join, root)
          else
            AsyncHelpers::INTERNAL.join_complete(root)
          end
        rescue Exception => e # rubocop:disable Lint/RescueException -- exact caller interruption identity
          e
        end
        interrupter = task.async(finished: false) do
          root.wait
          Fiber.scheduler.raise(joiner.fiber, first)
        end
        yields = 0
        trace = TracePoint.new(:call) do |event|
          yields += 1 if event.self.equal?(joiner) && event.method_id == :yield
        end
        trace.enable
        joiner.run
        join_retirement_tasks(task, joiner, interrupter, root, child)
        trace.disable
        expect(yields).to eq(0)
        expect(joiner.result).to equal(first)
      ensure
        trace&.disable
        reader&.close
        writer&.close
        AsyncHelpers::INTERNAL.cancel(interrupter) if interrupter && !interrupter.finished?
      end
    end
  end

  it "disposes the #{protocol} resource after immediate owned-root cancellation finalizes" do
    reactor do |task|
      with_retirement_connection(task, protocol: protocol) do |state|
        resource = state.fetch(:resource)
        reader, writer = Socket.pair(:UNIX, :STREAM, 0)
        root = Async::Task.new(state.fetch(:session).instance_variable_get(:@lifetime), finished: false) do |parent|
          parent.async(finished: false) { reader.readpartial(1) }
          Async::Condition.new.wait
        end
        resource.track(root)
        root.run
        child = root.children.to_a.fetch(0)
        state.fetch(:gate).release
        resource.abort
        expect(root).to be_finished
        expect(child).to be_finished
        expect(resource.raw).to be_closed
        expect(retirement_complete?(resource)).to be(true)
        expect(resource.instance_variable_get(:@cleanup)).to be_finished
      ensure
        reader&.close
        writer&.close
      end
    end
  end

  it "ignores an early stray wake at the #{protocol} post-raw-close scheduling hint" do
    reactor do |task|
      with_retirement_connection(task, protocol: protocol) do |state|
        resource = state.fetch(:resource)
        hint_done = Async::Promise.new
        trace = TracePoint.new(:call, :return) do |event|
          next unless event.self.equal?(resource.instance_variable_get(:@cleanup)) && event.method_id == :yield

          if event.event == :call
            Fiber.scheduler.push(event.self.fiber)
          else
            hint_done.resolve(raw_closed: resource.raw.closed?, handler_finished: state.fetch(:handler).finished?)
            trace.disable
          end
        end
        trace.enable
        state.fetch(:commands).enqueue(->(_) { resource.abort })
        state.fetch(:gate).entered.dequeue
        expect(hint_done.wait).to eq(raw_closed: true, handler_finished: false)
        expect(retirement_complete?(resource)).to be(false)
        expect(resource.instance_variable_get(:@cleanup)).not_to be_finished
        external = task.async(finished: false) { state.fetch(:session).abort }
        expect(external).not_to be_finished
        expect(state.fetch(:session).instance_variable_get(:@state)).to eq(:closing)
        state.fetch(:gate).release
        join_retirement_tasks(task, external, state.fetch(:handler), resource.instance_variable_get(:@cleanup))
        expect(retirement_complete?(resource)).to be(true)
        expect(state.fetch(:session).instance_variable_get(:@state)).to eq(:closed)
      ensure
        trace&.disable
        state.fetch(:gate).release
      end
    end
  end

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

  it "keeps an already-decided #{protocol} drained outcome through pending reentry and interruption" do
    reactor do |task|
      with_retirement_connection(task, protocol: protocol) do |state|
        session = state.fetch(:session)
        entered = Async::Queue.new
        release = Async::Queue.new
        reentries = []
        allow(session).to receive(:stop_accepting).and_wrap_original do |stop|
          stop.call
          if session.instance_variable_get(:@close_decided)
            reentries << session.close(grace: 0)
            entered.enqueue(true)
            release.dequeue
          end
        end
        first = RuntimeError.new("first interruption after drained decision")
        joiner = task.async(finished: false) do
          state.fetch(:host).close(grace: 5)
        rescue Exception => e # rubocop:disable Lint/RescueException -- exact waiter exception after an existing decision
          e
        end
        state.fetch(:commands).enqueue(->(_) {})
        state.fetch(:gate).entered.dequeue
        state.fetch(:gate).release
        expect(state.fetch(:call).wait.message.name).to eq("held")
        state.fetch(:transport).close(grace: 0)
        entered.dequeue
        expect(session.instance_variable_get(:@close_outcome)).to be(true)
        expect(session.instance_variable_get(:@state)).to eq(:closing)
        expect(reentries).to eq([nil])
        Fiber.scheduler.raise(joiner.fiber, first)
        competitor = task.async(finished: false) { session.close(grace: 0) }
        Fiber.scheduler.raise(joiner.fiber, Async::Cancel.new("later decided-close interruption"))
        expect([joiner, competitor]).to all(satisfy { |entry| !entry.finished? })
        release.enqueue(true)
        join_retirement_tasks(task, joiner, competitor, state.fetch(:handler))
        expect(joiner.result).to equal(first)
        expect(competitor.result).to be(true)
        expect(state.fetch(:host).close(grace: 0)).to be(true)
      ensure
        release&.enqueue(true)
      end
    end
  end

  it "requests owned #{protocol} close reentry after cancellation without joining itself" do
    reactor do |task|
      with_retirement_connection(task, protocol: protocol) do |state|
        returned = false
        state[:on_cleanup] = lambda do
          expect(state.fetch(:session).close(grace: 0)).to be_nil
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
