# frozen_string_literal: true

require_relative "retirement_helpers"

RSpec.shared_examples "retirement close corrections" do |protocol|
  include RetirementHelpers

  # Retirement shuts raw TCP down first and closes it once its tasks have
  # joined, for H2 and HTTP/1 alike.
  def raw_retired?(resource) = resource.send(:raw_failed?)

  def protocol_join(protocol, state, task)
    if protocol == :h1
      state.fetch(:resource).send(:join, task)
    else
      AsyncHelpers::INTERNAL.join_complete(task)
    end
  end

  # Async resolves a cancelled task's promise before it stops the task's
  # children and clears its fiber. The join returns at that settled boundary
  # without suspending; the fiber is reclaimed afterwards, with no signal.
  it "joins an immediately cancelled #{protocol} task at its settled boundary without suspending" do
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
        switches = 0
        trace = TracePoint.new(:fiber_switch) { switches += 1 }
        trace.enable(target_thread: Thread.current)
        protocol_join(protocol, state, root)
        trace.disable
        expect(switches).to eq(0)
        expect(original_fibers.first).to be_alive
        wait_until(task, "the cancelled task's fiber to be reclaimed") { root.finished? }
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

  # Retirement waits on the member, whose cancel then ends it with a child
  # to stop. Async wakes that wait when the member settles, before it stops
  # the child and clears its fiber, with no signal in between. A loop that
  # waited for Task#finished? would spin without yielding, so the passes are
  # bounded and the bound fails the example.
  it "completes #{protocol} retirement while a settled member's fiber is still unwinding" do
    reactor do |task|
      with_retirement_connection(task, protocol: protocol) do |state|
        resource = state.fetch(:resource)
        member_gate = RetirementHelpers::CleanupGate.new
        root = Async::Task.new(state.fetch(:session).instance_variable_get(:@lifetime), finished: false) do |current|
          member_gate.hold
        rescue Async::Cancel
          # A child whose own cancel ends at once, without suspending.
          current.async(finished: false) { Async::Condition.new.wait }
          raise
        end
        resource.track(root)
        root.run
        member_gate.entered.dequeue
        original = root.fiber
        completion_join = AsyncHelpers::INTERNAL.const_get(:CompletionJoin)
        limit = 1_000
        passes = 0
        waited = []
        unwinding_at_join = []
        trace = TracePoint.new(:call, :return) do |event|
          if event.event == :call
            if event.method_id == :wait && event.defined_class == Async::Task
              waited << event.self
            elsif event.self.equal?(resource) && event.method_id == :capture_retirement
              passes += 1
              raise "retirement made #{passes} passes without yielding" if passes > limit
            end
          elsif event.self.equal?(completion_join) && event.method_id == :call &&
                event.binding.local_variable_get(:task).equal?(root)
            unwinding_at_join << original.alive?
          end
        end
        trace.enable(target_thread: Thread.current)
        state.fetch(:gate).release
        aborter = task.async(finished: false) { resource.abort }
        wait_until(task, "retirement to wait on the member") { waited.any? { |entry| entry.equal?(root) } }
        member_gate.release
        task.with_timeout(2) { aborter.wait }
        trace.disable
        expect(passes).to be <= limit
        expect(retirement_complete?(resource)).to be(true)
        expect(unwinding_at_join).to include(true)
        wait_until(task, "the member's fiber to be reclaimed") { root.finished? }
      ensure
        trace&.disable
        member_gate&.release
      end
    end
  end

  it "keeps #{protocol} raw open until a task created in a cancelled member's ensure leaves" do
    reactor do |task|
      with_retirement_connection(task, protocol: protocol) do |state|
        resource = state.fetch(:resource)
        late_gate = RetirementHelpers::CleanupGate.new
        state[:on_cleanup] = -> { Async::Task.current.async(finished: false) { late_gate.hold } }
        waited = []
        trace = TracePoint.new(:call) do |event|
          waited << event.self if event.method_id == :wait && event.defined_class == Async::Task
        end
        trace.enable(target_thread: Thread.current)
        aborter = task.async(finished: false) { resource.abort }
        late = late_gate.entered.dequeue
        expect(state.fetch(:gate).entered.dequeue).to equal(state.fetch(:handler))
        # The member's block ends; its cancel cascade reaches the late task,
        # whose unwind holds.
        state.fetch(:gate).release
        wait_until(task, "retirement to wait on the late task or close raw") do
          waited.any? { |entry| entry.equal?(late) } || resource.raw.closed?
        end
        expect(resource.raw).not_to be_closed
        expect(retirement_complete?(resource)).to be(false)
        expect(late).to be_alive
        late_gate.release
        join_retirement_tasks(task, aborter, state.fetch(:handler), late)
        expect(resource.raw).to be_closed
        expect(retirement_complete?(resource)).to be(true)
      ensure
        trace&.disable
        late_gate&.release
        state.fetch(:gate).release
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

  it "ignores an early stray wake at the #{protocol} post-raw-failure scheduling hint" do
    reactor do |task|
      with_retirement_connection(task, protocol: protocol) do |state|
        resource = state.fetch(:resource)
        hint_done = Async::Promise.new
        trace = TracePoint.new(:call, :return) do |event|
          next unless event.self.equal?(resource.instance_variable_get(:@cleanup)) && event.method_id == :yield

          if event.event == :call
            Fiber.scheduler.push(event.self.fiber)
          else
            hint_done.resolve(raw_retired: raw_retired?(resource), raw_closed: resource.raw.closed?,
                              handler_finished: state.fetch(:handler).finished?)
            trace.disable
          end
        end
        trace.enable
        state.fetch(:commands).enqueue(->(_) { resource.abort })
        state.fetch(:gate).entered.dequeue
        # The descriptor stays open until every member that may block on it
        # has joined.
        expect(hint_done.wait).to eq(raw_retired: true, raw_closed: false, handler_finished: false)
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

  it "allows #{protocol} resource cleanup reentry from the actual raw failure boundary" do
    reactor do |task|
      with_retirement_connection(task, protocol: protocol) do |state|
        resource = state.fetch(:resource)
        allow(resource.raw).to receive(:shutdown).and_wrap_original do |original, *args|
          resource.abort
          original.call(*args)
        end
        aborter = task.async(finished: false) { resource.abort }
        state.fetch(:gate).entered.dequeue
        expect(raw_retired?(resource)).to be(true)
        expect(resource.raw).not_to be_closed
        expect(aborter).not_to be_finished
        state.fetch(:gate).release
        join_retirement_tasks(task, aborter, state.fetch(:handler))
        expect(retirement_complete?(resource)).to be(true)
        expect(resource.raw).to be_closed
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
        expect(joiner).to be_finished
        expect(joiner.result).to equal(first)
        competitor = task.async(finished: false) { session.close(grace: 0) }
        expect(competitor).not_to be_finished
        release.enqueue(true)
        join_retirement_tasks(task, competitor, state.fetch(:handler))
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

  it "leaves the #{protocol} drain running when an external close caller is interrupted" do
    reactor do |task|
      with_retirement_connection(task, protocol: protocol) do |state|
        session, resource = state.values_at(:session, :resource)
        first = RuntimeError.new("graceful close interruption")
        joiner = task.async(finished: false) do
          session.close(grace: 3600)
        rescue Exception => e # rubocop:disable Lint/RescueException -- exact caller exception identity
          e
        end
        wait_until(task, "real nonzero resource drain started") { resource.instance_variable_get(:@drain_task) }
        Fiber.scheduler.raise(joiner.fiber, first)
        expect(joiner).to be_finished
        expect(joiner.result).to equal(first)
        expect(resource).not_to be_finished
        expect(state.fetch(:handler)).to be_alive
        expect(session.instance_variable_get(:@close_decided)).to be_nil
        # A later positive-grace close joins the running drain, which
        # finishes once its admitted RPC does.
        follower = task.async(finished: false) { session.close(grace: 1) }
        state.fetch(:commands).enqueue(->(_) {})
        state.fetch(:gate).entered.dequeue
        expect(follower).not_to be_finished
        state.fetch(:gate).release
        expect(state.fetch(:call).wait.message.name).to eq("held")
        state.fetch(:transport).close(grace: 0)
        join_retirement_tasks(task, follower, state.fetch(:handler))
        expect(follower.result).to be(true)
        expect(retirement_complete?(resource)).to be(true)
      ensure
        state.fetch(:gate).release
        # A failing assertion must not wait for the long grace.
        session&.abort
      end
    end
  end

  it "forces the #{protocol} drain an interrupted close caller left with a later zero-grace close" do
    reactor do |task|
      with_retirement_connection(task, protocol: protocol) do |state|
        session, resource = state.values_at(:session, :resource)
        joiner = task.async(finished: false) { session.close(grace: 3600) }
        wait_until(task, "real nonzero resource drain started") { resource.instance_variable_get(:@drain_task) }
        joiner.cancel
        expect(joiner).to be_cancelled
        expect(resource).not_to be_finished
        expect(state.fetch(:handler)).to be_alive
        forcer = task.async(finished: false) { session.close(grace: 0) }
        expect(resource.send(:raw_failed?)).to be(true), "zero grace must force retirement without grace expiry"
        expect(state.fetch(:gate).entered.dequeue).to equal(state.fetch(:handler))
        expect(forcer).not_to be_finished
        expect(retirement_complete?(resource)).to be(false)
        state.fetch(:gate).release
        join_retirement_tasks(task, forcer, state.fetch(:handler))
        expect(forcer.result).to be(false)
        expect(retirement_complete?(resource)).to be(true)
        expect(state.fetch(:call).wait).to be_a(ConnectRPC::Error)
      ensure
        state.fetch(:gate).release
        session&.abort
      end
    end
  end

  # The first close can still be stopping listeners when its grace runs out,
  # before any connection drain starts, so expiry alone cannot mean forcing.
  it "forces #{protocol} connections with zero grace after the first close's grace ran out before its drain" do
    reactor do |task|
      with_retirement_connection(task, protocol: protocol) do |state|
        session, resource = state.values_at(:session, :resource)
        entered = Async::Queue.new
        release = Async::Queue.new
        calls = 0
        allow(session).to receive(:stop_accepting).and_wrap_original do |stop|
          calls += 1
          if calls == 1
            entered.enqueue(true)
            release.dequeue
          end
          stop.call
        end
        expires = nil
        allow(session).to receive(:close_owned).and_wrap_original do |close_owned, expiry|
          expires = expiry
          close_owned.call(expiry)
        end
        joiner = task.async(finished: false) { session.close(grace: 0.01) }
        entered.dequeue
        joiner.cancel
        wait_until(task, "the first close's grace to run out") { AsyncHelpers::INTERNAL.now >= expires }
        expect(resource.instance_variable_get(:@drain_task)).to be_nil
        forcer = task.async(finished: false) { session.close(grace: 0) }
        expect(resource.send(:raw_failed?)).to be(true), "zero grace must force before the stalled drain starts"
        expect(state.fetch(:gate).entered.dequeue).to equal(state.fetch(:handler))
        release.enqueue(true)
        state.fetch(:gate).release
        join_retirement_tasks(task, forcer, state.fetch(:handler))
        expect(forcer.result).to be(false)
        expect(retirement_complete?(resource)).to be(true)
      ensure
        release&.enqueue(true)
        state.fetch(:gate).release
        session&.abort
      end
    end
  end

  it "lets a #{protocol} handler force a pending drain with a zero-grace close" do
    reactor do |task|
      with_retirement_connection(task, protocol: protocol) do |state|
        session, resource = state.values_at(:session, :resource)
        closer = task.async(finished: false) { session.close(grace: 3600) }
        wait_until(task, "real nonzero resource drain started") { resource.instance_variable_get(:@drain_task) }
        returned = :pending
        state.fetch(:commands).enqueue(->(_) { returned = session.close(grace: 0) })
        expect(state.fetch(:gate).entered.dequeue).to equal(state.fetch(:handler))
        expect(returned).to be_nil
        expect(resource.send(:raw_failed?)).to be(true)
        expect(closer).not_to be_finished
        state.fetch(:gate).release
        join_retirement_tasks(task, closer, state.fetch(:handler))
        expect(closer.result).to be(false)
        expect(retirement_complete?(resource)).to be(true)
      ensure
        state.fetch(:gate).release
        session&.abort
      end
    end
  end

  it "lets an interrupted #{protocol} accepted-handle close leave while retirement finishes" do
    reactor do |task|
      with_retirement_connection(task, protocol: protocol) do |state|
        accepted, resource = state.values_at(:accepted, :resource)
        first = RuntimeError.new("accepted close interruption")
        joiner = task.async(finished: false) do
          accepted.close
        rescue Exception => e # rubocop:disable Lint/RescueException -- exact caller exception identity
          e
        end
        expect(state.fetch(:gate).entered.dequeue).to equal(state.fetch(:handler))
        Fiber.scheduler.raise(joiner.fiber, first)
        expect(joiner).to be_finished
        expect(joiner.result).to equal(first)
        expect(retirement_complete?(resource)).to be(false)
        waiter = task.async(finished: false) { accepted.wait_closed }
        expect(waiter).not_to be_finished
        state.fetch(:gate).release
        join_retirement_tasks(task, waiter, state.fetch(:handler))
        expect(retirement_complete?(resource)).to be(true)
      ensure
        state.fetch(:gate).release
        state.fetch(:session)&.abort
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
