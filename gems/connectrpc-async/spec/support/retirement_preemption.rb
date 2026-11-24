# frozen_string_literal: true

# Each including group defines completion_join(task, **, &) for its join.
RSpec.shared_examples "settled completion join" do
  # A settled task's block, ensure included, has returned.
  def settled?(task) = task.completed? || task.failed? || task.cancelled?

  # Records [fiber, task] for every Task#wait on this thread, so an example
  # can synchronize on the task a joiner actually waits on.
  def recording_waits
    waits = []
    trace = TracePoint.new(:call) do |event|
      waits << [Fiber.current, event.self] if event.method_id == :wait && event.defined_class == Async::Task
    end
    trace.enable(target_thread: Thread.current)
    yield waits
  ensure
    trace&.disable
  end

  def waits_on?(waits, fiber, target) = waits.any? { |waiter, task| waiter.equal?(fiber) && task.equal?(target) }

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

  def preempt_join(interrupt:)
    reactor do |task|
      sockets, root, children, readers = preemption_tasks(task)
      tasks = [root, *children, *readers]
      first = Async::Cancel.new("first R3 interruption")
      settled_at_return = nil
      joiner = Async::Task.new(task, finished: false) do
        completion_join(root) { root.cancel }
      rescue Exception => e # rubocop:disable Lint/RescueException -- observe caller identity before fixture cleanup
        e
      ensure
        settled_at_return = tasks.map { |entry| settled?(entry) }
      end
      waiter = preemption_waiter(task, children.last, joiner, first, interrupt: interrupt)
      joiner.run
      expect(joiner.wait).to equal(interrupt ? first : nil)
      expect(settled_at_return).to eq([true] * tasks.length)
      wait_until(task, "every joined fiber to be reclaimed") { tasks.all?(&:finished?) }
      waiter.wait
    ensure
      close_preemption_sockets(sockets)
    end
  end

  it "preserves the first interruption through promise-waiter preemption" do
    preempt_join(interrupt: true)
  end

  it "joins every settled task across a queued stray wake" do
    preempt_join(interrupt: false)
  end

  # The target's own failure is its task's to report: no join raises it.
  it "keeps independent interruptions through actual child cleanup without raising the target's failure" do
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
        completion_join(root)
      rescue Exception => e # rubocop:disable Lint/RescueException -- preserve each caller's own outcome
        e
      end
      competitor = task.async(finished: false) do
        completion_join(root)
        :joined
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
      expect(competitor.wait).to eq(:joined)
      expect(root).to be_finished
      expect { completion_join(root) }.not_to raise_error
    ensure
      releases&.each { |release| release.enqueue(true) }
    end
  end

  # Each raise lands while the joiner waits, so it has handled the raise by
  # the time Fiber.scheduler.raise returns.
  it "keeps a pre-join interruption over a later one raised while it waits" do
    reactor do |task|
      release = Async::Queue.new
      root = task.async(finished: false) { release.dequeue }
      first = RuntimeError.new("pre-join interruption")
      joiner = task.async(finished: false) do
        completion_join(root) { raise first }
      rescue StandardError => e
        e
      end
      expect(joiner).not_to be_finished
      Fiber.scheduler.raise(joiner.fiber, RuntimeError.new("later interruption"))
      expect(joiner).not_to be_finished
      release.enqueue(true)
      expect(joiner.wait).to equal(first)
      expect(root).to be_completed
    ensure
      release&.enqueue(true)
    end
  end

  # A snapshot of the subtree, like Task#wait_all, misses this task (Async
  # 2.46.0): it does not exist until the cancel cascade reaches the child.
  it "joins a task created in a cancelled child's ensure whose own unwind suspends" do
    reactor do |task|
      entered = Async::Promise.new
      release = Async::Queue.new
      late = nil
      root = task.async(finished: false) do |parent|
        parent.async(finished: false) do |child|
          entered.resolve(true)
          Async::Condition.new.wait
        ensure
          late = child.async(finished: false) do
            Async::Condition.new.wait
          ensure
            release.dequeue
          end
        end
        Async::Condition.new.wait
      end
      entered.wait
      recording_waits do |waits|
        settled_at_return = nil
        joiner = task.async(finished: false) do
          completion_join(root)
          settled_at_return = settled?(late)
        end
        joiner_fiber = joiner.fiber
        root.cancel
        wait_until(task, "the joiner to wait on the late task or return") do
          (late && waits_on?(waits, joiner_fiber, late)) || joiner.finished?
        end
        expect(joiner).not_to be_finished
        expect(settled?(root)).to be(true)
        expect(late).to be_alive
        release.enqueue(true)
        joiner.wait
        expect(settled_at_return).to be(true)
      end
    ensure
      release&.enqueue(true)
    end
  end

  it "keeps children seen while it waits joined after they are reparented away or flagged transient" do
    reactor do |task|
      releases = Array.new(3) { Async::Queue.new }
      started = Async::Promise.new
      root = task.async(finished: false) do |parent|
        started.resolve(releases.first(2).map { |release| parent.async(finished: false) { release.dequeue } })
        releases[2].dequeue
      end
      children = started.wait
      recording_waits do |waits|
        settled_at_return = nil
        joiner = task.async(finished: false) do
          completion_join(root)
          settled_at_return = children.map { |child| settled?(child) }
        end
        joiner_fiber = joiner.fiber
        expect(waits_on?(waits, joiner_fiber, root)).to be(true)
        children.first.parent = task
        children.last.transient = true
        releases[2].enqueue(true)
        wait_until(task, "the joiner to wait on a detached child or return") do
          children.any? { |child| waits_on?(waits, joiner_fiber, child) } || joiner.finished?
        end
        expect(joiner).not_to be_finished
        releases[0].enqueue(true)
        wait_until(task, "the joiner to wait on the transient child or return") do
          waits_on?(waits, joiner_fiber, children.last) || joiner.finished?
        end
        expect(joiner).not_to be_finished
        releases[1].enqueue(true)
        joiner.wait
        expect(settled_at_return).to eq([true, true])
      end
    ensure
      releases&.each { |release| release.enqueue(true) }
    end
  end

  it "raises ArgumentError for a self-join without blocking" do
    reactor do |task|
      expect { completion_join(task) }.to raise_error(ArgumentError, "Cannot join the current task")
    end
  end

  # The parent waits on the child, so a join that waited on the parent would
  # never return.
  it "raises ArgumentError when a child joins a live ancestor that waits on it" do
    reactor do |task|
      release = Async::Queue.new
      parent = task.async(finished: false) do |current|
        child = current.async(finished: false) do
          release.dequeue
          completion_join(current)
        rescue ArgumentError => e
          e
        end
        child.wait
      end
      release.enqueue(true)
      outcome = begin
        task.with_timeout(2) { parent.wait }
      rescue Async::TimeoutError
        :blocked
      end
      expect(outcome).to be_a(ArgumentError)
      expect(outcome.message).to eq("Cannot join the current task")
    ensure
      AsyncHelpers::INTERNAL.cancel(parent) if parent && !parent.finished?
    end
  end

  it "waits on an initialized task until another fiber starts it" do
    reactor do |task|
      staged = Async::Task.new(task, finished: false) { :staged }
      joiner = task.async(finished: false) { completion_join(staged) }
      expect(joiner).not_to be_finished
      expect(staged.status).to eq(:initialized)
      staged.run
      expect(joiner.wait).to eq(:staged)
    ensure
      staged.run if staged&.status == :initialized
    end
  end
end
