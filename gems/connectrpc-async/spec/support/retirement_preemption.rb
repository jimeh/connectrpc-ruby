# frozen_string_literal: true

RSpec.shared_examples "R3 original-source finalization orderings" do
  def preemption_tasks(task)
    sockets = Array.new(5) { Socket.pair(:UNIX, :STREAM, 0) }
    started = Async::Queue.new
    children = []
    readers = []
    root = task.async(finished: false) do |parent|
      2.times do |index|
        children << parent.async(finished: false) do |child|
          readers << child.async(finished: false) { sockets.fetch(index).first.readpartial(1) }
          started.enqueue(:child)
          sockets.fetch(index + 2).first.readpartial(1)
        end
      end
      started.enqueue(:root)
      sockets.last.first.readpartial(1)
    end
    3.times { started.dequeue }
    expect([children.size, readers.size]).to eq([2, 2])
    [sockets, root, children, readers]
  end

  def preemption_waiter(task, target, joiner, first, interrupt:)
    task.async(finished: false) do
      target.wait
      # Preserve the original promise wake while transferring directly. Its
      # stale ready entry can resume the joiner at a later blocking site.
      if interrupt
        Fiber.scheduler.raise(joiner.fiber, first)
      else
        Fiber.scheduler.resume(joiner.fiber)
      end
    end
  end

  def close_preemption_sockets(sockets)
    sockets&.flatten&.each { |socket| socket.close unless socket.closed? }
  end

  def preempt_finalization(interrupt:)
    reactor do |task|
      sockets, root, children, readers = preemption_tasks(task)
      fibers = [root, *children, *readers].map(&:fiber)
      first = Async::Cancel.new("first R3 interruption")
      boundary = nil
      joiner = Async::Task.new(task, finished: false) do
        completion_join(root) { root.cancel }
      rescue Exception => e # rubocop:disable Lint/RescueException -- observe caller identity before fixture cleanup
        e
      ensure
        boundary = { root_finished: root.finished?, fibers_alive: fibers.map(&:alive?),
                     tasks: [root, *children, *readers].map { |entry| [entry.status, entry.finished?] } }
        puts "R3 boundary #{interrupt ? 'interrupt' : 'stray'}: #{boundary.inspect}"
      end
      waiter = preemption_waiter(task, children.last, joiner, first, interrupt: interrupt)
      joiner.run
      result = joiner.wait
      puts "R3 result: #{result.class}: #{result} first_identity=#{result.equal?(first)}"
      expect(result).to equal(interrupt ? first : nil)
      expect(boundary.fetch(:root_finished)).to be(true)
      expect(boundary.fetch(:fibers_alive)).to eq([false] * fibers.length)
      expect([root, *children, *readers]).to all(be_finished)
      waiter.wait
    ensure
      close_preemption_sockets(sockets)
    end
  end

  it "preserves the first interruption through promise-waiter preemption" do
    preempt_finalization(interrupt: true)
  end

  it "joins actual finalization across a queued stray wake" do
    preempt_finalization(interrupt: false)
  end
end

RSpec.shared_examples "completion progress observation" do
  def residual_root(task, reader)
    started = Async::Promise.new
    root = task.async(finished: false) do |parent|
      parent.async(finished: false) { reader.readpartial(1) }
      started.resolve(true)
      Async::Condition.new.wait
    end
    started.wait
    root
  end

  it "keeps independent interruptions and stored failure through actual child cleanup" do
    reactor do |task|
      entered = Async::Queue.new
      releases = Array.new(2) { Async::Queue.new }
      failure = RuntimeError.new("stored target failure")
      children = []
      root = task.async(finished: false) do |parent|
        releases.each do |release|
          children << parent.async(finished: false) do
            entered.enqueue(true)
            release.dequeue
          end
        end
        raise failure
      end
      2.times { entered.dequeue }
      first = RuntimeError.new("first independent interruption")
      joiner = task.async(finished: false) do
        completion_join(root, failure: true)
      rescue Exception => e # rubocop:disable Lint/RescueException -- preserve each caller's own outcome
        e
      end
      competitor = task.async(finished: false) do
        completion_join(root, failure: true)
      rescue StandardError => e
        e
      end
      Fiber.scheduler.raise(joiner.fiber, first)
      Fiber.scheduler.raise(joiner.fiber, Async::Cancel.new("later independent interruption"))
      expect([joiner, competitor]).to all(satisfy { |entry| !entry.finished? })
      releases.last.enqueue(true)
      completion_join(children.last)
      expect([joiner, competitor]).to all(satisfy { |entry| !entry.finished? })
      releases.first.enqueue(true)
      expect(joiner.wait).to equal(first)
      expect(competitor.wait).to equal(failure)
      expect(root).to be_finished
      expect(completion_join(root)).to equal(failure)
      expect { completion_join(root, failure: true) }.to(raise_error { |error| expect(error).to equal(failure) })
      expect(completion_state(root)).to be_nil
    ensure
      releases&.each { |release| release.enqueue(true) }
    end
  end

  it "retains consume-time child progress across removal reparenting and later transient flags" do
    reactor do |task|
      reader, writer = Socket.pair(:UNIX, :STREAM, 0)
      root = residual_root(task, reader)
      original = root.fiber
      releases = Array.new(2) { Async::Queue.new }
      required = []
      competitor = nil
      captured = Async::Promise.new
      # This controller is parked until the root enters its actual finish!.
      # That non-yielding tail captures the new children before the controller.
      controller = task.async(finished: false) do
        Fiber.scheduler.transfer
        state = completion_state(root)
        expect(original).not_to be_alive
        expect(state.required.keys).to include(*required)
        expect(state).not_to be_terminal
        required.first.parent = task
        required.last.parent = nil
        required.each { |child| child.transient = true }
        competitor = task.async(finished: false) { completion_join(root) }
        required.each(&:run)
        captured.resolve(true)
      end
      trace = TracePoint.new(:call) do |event|
        next unless event.self.equal?(root) && event.method_id == :finish! && completion_state(root)

        trace.disable
        required = releases.map do |release|
          Async::Task.new(root, finished: false) { release.dequeue }
        end
        Fiber.scheduler.push(controller.fiber)
      end
      joiner = task.async(finished: false) do
        trace.enable
        completion_join(root) { root.cancel }
      end
      captured.wait
      expect(root).to be_finished
      expect([joiner, competitor]).to all(satisfy { |entry| !entry.finished? })
      state = completion_state(root)
      state.promise.wait until state.terminal?
      expect(state.instance_variable_get(:@fiber)).to be_nil
      expect(state.instance_variable_get(:@scheduler)).to be_nil
      # Terminal observation cannot freeze membership while retained joins live.
      late_release = Async::Queue.new
      late = Async::Task.new(root, finished: false) { late_release.dequeue }
      late.run
      pulse = Async::Task.new(root, finished: false) { :consumed }
      pulse.run
      pulse.wait
      expect(state.required.keys).to include(late)
      late.parent = task
      late.transient = true
      releases.last.enqueue(true)
      completion_join(required.last)
      expect([joiner, competitor]).to all(satisfy { |entry| !entry.finished? })
      releases.first.enqueue(true)
      completion_join(required.first)
      expect([joiner, competitor]).to all(satisfy { |entry| !entry.finished? })
      late_release.enqueue(true)
      expect(joiner.wait).to be_nil
      expect(competitor.wait).to be_nil
      expect([root, *required, late, pulse, controller]).to all(be_finished)
      expect(original).not_to be_alive
      state = completion_state(root)
      expect(state).to be_terminal
      expect(state.instance_variable_get(:@fiber)).to be_nil
      expect(state.instance_variable_get(:@scheduler)).to be_nil
      generation = state.generation
      root.consume
      expect(state.generation).to eq(generation), "released per-instance forwarding must be inert"
    ensure
      trace&.disable
      releases&.each { |release| release.enqueue(true) }
      late_release&.enqueue(true)
      reader&.close
      writer&.close
    end
  end

  it "latches terminal notification before the first wait and rechecks a finished task's new work" do
    reactor do |task|
      reader, writer = Socket.pair(:UNIX, :STREAM, 0)
      root = residual_root(task, reader)
      original = root.fiber
      joiner = nil
      notified_before_wait = false
      controller = task.async(finished: false) do
        Fiber.scheduler.transfer
        completion_state(root).promise.wait
        notified_before_wait = completion_state(root).terminal? && !original.alive?
        Fiber.scheduler.resume(joiner.fiber)
      end
      trace = TracePoint.new(:call) do |event|
        state = completion_state(root)
        next unless state && event.self.equal?(state.promise) && event.method_id == :wait

        trace.disable
        Fiber.scheduler.push(controller.fiber)
        Fiber.scheduler.transfer
      end
      joiner = Async::Task.new(task, finished: false) do
        trace.enable
        completion_join(root) { root.cancel }
      end
      joiner.run
      expect(joiner.wait).to be_nil
      expect(notified_before_wait).to be(true)
      expect([root, controller, joiner]).to all(be_finished)
      release = Async::Queue.new
      child = Async::Task.new(root, finished: false) { release.dequeue }
      child.run
      expect(root).not_to be_finished
      later = task.async(finished: false) { completion_join(root) }
      expect(later).not_to be_finished
      child.parent = task
      child.transient = true
      competitor = task.async(finished: false) { completion_join(root) }
      expect([later, competitor]).to all(satisfy { |entry| !entry.finished? })
      release.enqueue(true)
      expect(later.wait).to be_nil
      expect(competitor.wait).to be_nil
      expect([root, child, later, competitor]).to all(be_finished)
    ensure
      trace&.disable
      release&.enqueue(true)
      reader&.close
      writer&.close
    end
  end

  it "reports a nil-fiber unfinished target with attached finished children explicitly" do
    reactor do |task|
      root = Async::Task.new(task, finished: false) { :never_run }
      root.cancel
      finished = task.async(finished: false) { :finished }
      finished.wait
      finished.parent = root
      expect(root).not_to be_finished
      expect(root.fiber).to be_nil
      expect do
        completion_join(root)
      end.to raise_error(RuntimeError, /Completion observation violation:.*attached_children=.*finished: true/)
      expect(completion_state(root)).to be_nil
    ensure
      finished.parent = nil if finished
      root&.consume
    end
  end
end
