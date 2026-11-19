# frozen_string_literal: true

# RB_NOGVL_OFFLOAD_SAFE lets async's worker pool (Ruby 4.0, io-event's
# blocking-operation hook) run a call's native wait off the reactor thread.
# Close waits on a helper thread on a fiber scheduler's non-blocking fiber.
# Each case runs in its own process, with or without the pool.
RSpec.describe "Inside an Async reactor" do
  offload = Gem::Version.new(RUBY_VERSION) >= Gem::Version.new("4.0")

  def reactor_child(code, pool:)
    ruby_child(<<~RUBY, env: { "ASYNC_SCHEDULER_WORKER_POOL" => pool ? "true" : nil }, timeout: 60)
      require "async"
      def active_waits = ConnectRPC::Hyper.const_get(:Native).active_waits
      # Yields to the reactor until `count` native waits are parked, after
      # `ready` (when given) has a value.
      def parked(count = 1, ready: nil)
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10
        until active_waits >= count && (ready.nil? || !ready.empty?)
          raise "never parked: \#{active_waits}" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
          Fiber.scheduler ? sleep(0.001) : Thread.pass
        end
      end
      def wait_bodies = ConnectRPC::Hyper.const_get(:Native).wait_bodies
      # Yields to the reactor until `count` native wait bodies run (GVL
      # released, unblocking function registered), or `done` (when given)
      # finished.
      def running(count = 1, done: nil)
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10
        until wait_bodies[1] >= count || done&.finished?
          raise "never running: \#{wait_bodies}" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
          sleep(0.001)
        end
      end
      # Polls until the block is truthy or 10 seconds passed; the block's
      # last value.
      def eventually
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10
        sleep(0.001) until (met = yield) || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
        met
      end
      def ticks_during(task)
        ticks = 0
        ticker = task.async { loop { sleep 0.01; ticks += 1 } }
        yield
        ticker.stop
        ticks
      end
      #{code}
    RUBY
  end

  # A thread's client stream keeps one call in flight, waiting in Ruby, until
  # `release` has a value.
  def held_call = <<~RUBY
    transport = ConnectRPC::Hyper::Transport.new
    driver = transport.instance_variable_get(:@driver)
    sent = Queue.new
    release = Queue.new
    held = Thread.new do
      client(#{peer['grpc_h2c'].dump}, transport).client_stream(timeout: 30) do |writer|
        writer.send_message(MSG.new(value: "a"))
        sent << true
        release.pop
      end.message.value
    rescue ConnectRPC::Error => e
      e.message
    end
    sent.pop
  RUBY

  # Thread 1 closes a transport whose held stream outlasts the grace, then an
  # Async task on the main thread closes it too and is stopped while it
  # waits. Every closer waits for the first teardown's published outcome; the
  # task waits on a helper thread, so the stop lands while it waits and is
  # raised only after the outcome is published.
  def concurrent_closers(pool:)
    output, status = reactor_child(<<~RUBY, pool: pool)
      transport = ConnectRPC::Hyper::Transport.new
      driver = transport.instance_variable_get(:@driver)
      ready = Queue.new
      held = Thread.new do
        client(#{peer['grpc_h2c'].dump}, transport).server_stream(MSG.new(value: "hold"), timeout: 30) do |reader|
          reader.receive
          ready << true
          reader.receive
        end
      rescue ConnectRPC::Error => e
        e.message
      end
      parked(ready: ready)
      first = Thread.new { transport.close(grace: 2) }
      # The held read and the first closer's wait.
      parked(2)
      second = Async do |task|
        closer = task.async do
          transport.close(grace: 2)
        rescue Async::Stop
          [:stopped, driver.close_outcome]
        end
        # The held read and both closers' waits.
        parked(3)
        closer.stop
        closer.wait
      end.wait
      p [second, first.value, transport.close(grace: 5), held.value, driver.stats[1..2]]
    RUBY
    expect(status).to be_success
    eval(output.lines.last) # rubocop:disable Security/Eval
  end

  it "#{offload ? 'lets sibling tasks run' : 'blocks the reactor'} while a call waits, with the worker pool" do
    output, status = reactor_child(<<~RUBY, pool: true)
      peer_client = client(#{peer['grpc_h2c'].dump}, ConnectRPC::Hyper::Transport.new)
      Async do |task|
        result = nil
        ticks = ticks_during(task) do
          result = begin
            task.async { peer_client.unary(MSG.new(value: "hang"), timeout: 0.3) }.wait
          rescue ConnectRPC::Error => e
            e.code
          end
        end
        p [result, ticks]
      end
    RUBY
    expect(status).to be_success
    result, ticks = eval(output.lines.last) # rubocop:disable Security/Eval
    expect(result).to eq("deadline_exceeded")
    expect(ticks).to(offload ? be >= 10 : be <= 1)
  end

  # Ruby 3.4 gives a scheduler's hook a plain proc, and a cancelled scheduler
  # could unwind the waiting frame while the body still runs, so waits offer
  # the hook only on Ruby 4.0. The pool records each hand-off and runs it.
  it "#{offload ? 'hands' : 'never hands'} a native wait to a scheduler's blocking_operation_wait hook" do
    output, status = reactor_child(<<~RUBY, pool: false)
      handed = 0
      pool = Object.new
      pool.define_singleton_method(:call) do |work|
        handed += 1
        Thread.new { work.call }.join
      end
      pool.define_singleton_method(:close) { nil }
      peer_client = client(#{peer['grpc_h2c'].dump}, ConnectRPC::Hyper::Transport.new)
      Fiber.set_scheduler(Async::Scheduler.new(worker_pool: pool))
      task = Async { peer_client.unary(MSG.new(value: "hook"), timeout: 5).message.value }
      Fiber.set_scheduler(nil)
      p [task.result, handed.positive?]
    RUBY
    expect(status).to be_success
    expect(eval(output.lines.last)).to eq(["hook", offload]) # rubocop:disable Security/Eval
  end

  it "blocks the reactor without the worker pool" do
    output, status = reactor_child(<<~RUBY, pool: false)
      peer_client = client(#{peer['grpc_h2c'].dump}, ConnectRPC::Hyper::Transport.new)
      Async do |task|
        result = nil
        ticks = ticks_during(task) do
          result = begin
            task.async { peer_client.unary(MSG.new(value: "hang"), timeout: 0.3) }.wait
          rescue ConnectRPC::Error => e
            e.code
          end
        end
        p [result, ticks]
      end
    RUBY
    expect(status).to be_success
    expect(eval(output.lines.last)).to match(["deadline_exceeded", be <= 1]) # rubocop:disable Security/Eval
  end

  [true, false].each do |pool|
    # A sibling fiber's call is in flight on the closer's reactor and can only
    # finish after the closer started waiting. The closer's wait must leave the
    # reactor thread and the worker pool (one worker by default) free, or the
    # sibling cannot drain and the grace expires into forced teardown.
    it "lets a sibling fiber's call drain while another fiber waits in close, #{pool ? 'with' : 'without'} " \
       "the worker pool" do
      output, status = reactor_child(<<~RUBY, pool: pool)
        transport = ConnectRPC::Hyper::Transport.new
        driver = transport.instance_variable_get(:@driver)
        outcome = Async do |task|
          sent = Thread::Queue.new
          release = Thread::Queue.new
          order = []
          sibling = task.async do
            client(#{peer['grpc_h2c'].dump}, transport).client_stream(timeout: 30) do |writer|
              writer.send_message(MSG.new(value: "a"))
              sent << true
              release.pop
            end.message.value
          rescue ConnectRPC::Error => e
            e.message
          ensure
            order << :sibling
          end
          sent.pop
          closer = task.async do
            transport.close(grace: 10)
          ensure
            order << :closer
          end
          # The close wait runs; a closer that held the reactor thread instead
          # has returned before this task resumes.
          running(done: closer)
          release << true
          [closer.wait, sibling.wait, order]
        end.wait
        p [outcome, transport.close(grace: 5), driver.stats[1..2]]
      RUBY
      expect(status).to be_success
      expect(eval(output.lines.last)) # rubocop:disable Security/Eval
        .to eq([[true, "a", %i[sibling closer]], true, [0, 0]])
    end

    # The held call is a sibling fiber on the closer's reactor, so it can only
    # finish while the stopped closer keeps waiting on its helper thread. Close
    # drains, and the stop reaches the closer once teardown published its
    # outcome.
    it "lets a sibling fiber's call drain after Task#stop lands on a closing task, #{pool ? 'with' : 'without'} " \
       "the worker pool" do
      output, status = reactor_child(<<~RUBY, pool: pool)
        transport = ConnectRPC::Hyper::Transport.new
        driver = nil
        outcome = Async do |task|
          sent = Thread::Queue.new
          release = Thread::Queue.new
          sibling = task.async do
            client(#{peer['grpc_h2c'].dump}, transport).client_stream(timeout: 30) do |writer|
              writer.send_message(MSG.new(value: "a"))
              sent << true
              release.pop
            end.message.value
          rescue ConnectRPC::Error => e
            e.message
          end
          sent.pop
          driver = transport.instance_variable_get(:@driver)
          closer = task.async do
            transport.close(grace: 5)
          rescue Async::Stop
            [:stopped, driver.close_outcome]
          end
          parked
          closer.stop
          release << true
          [closer.wait, sibling.wait]
        end.wait
        p [outcome, transport.close(grace: 5), driver.stats[1..2]]
      RUBY
      expect(status).to be_success
      expect(eval(output.lines.last)).to eq([[[:stopped, true], "a"], true, [0, 0]]) # rubocop:disable Security/Eval
    end
  end

  # The held call is the only one in flight, so the close wait is the only
  # native wait. Killing a closer's fiber while it waits on its helper thread
  # must not record an outcome before teardown publishes one: a later closer
  # waits for the same teardown and gets the published outcome, which the
  # transport then keeps.
  it "makes a later closer wait for the published outcome after a waiting closer's fiber is killed" do
    output, status = reactor_child(<<~RUBY, pool: false)
      #{held_call}
      # A killed scheduled fiber hands control to the root fiber, so the root
      # fiber kills it; a sibling task that killed it would never resume.
      Fiber.set_scheduler(Async::Scheduler.new)
      closer = Fiber.schedule { transport.close(grace: 10) }
      running
      closer.kill
      Fiber.set_scheduler(nil)
      returned = Queue.new
      # Releases the held call once this thread waits in close too.
      releaser = Thread.new do
        eventually { wait_bodies[1] >= 2 || !returned.empty? }
        release << true
      end
      later = [transport.close(grace: 5), driver.close_outcome]
      returned << true
      releaser.join
      done = held.value
      eventually { !driver.close_outcome.nil? }
      p [later, done, transport.close(grace: 5), driver.close_outcome, driver.stats[1..2]]
    RUBY
    expect(status).to be_success
    expect(eval(output.lines.last)).to eq([[true, true], "a", true, true, [0, 0]]) # rubocop:disable Security/Eval
  end

  # Thread interrupt masks are per thread and Thread.new copies them, so a
  # closer must not hold one while its fiber yields to the reactor.
  it "lets Thread#raise reach a thread a sibling fiber starts while another fiber waits in close" do
    output, status = reactor_child(<<~RUBY, pool: false)
      #{held_call}
      outcome = Async do |task|
        closer = task.async { transport.close(grace: 10) }
        running(done: closer)
        started = Thread::Queue.new
        blocker = Thread.new do
          started << true
          Thread::Queue.new.pop(timeout: 5)
          :not_interrupted
        rescue RuntimeError => e
          e.message
        end
        started.pop
        blocker.raise("interrupted")
        raised = blocker.join(2) ? blocker.value : :still_blocked
        release << true
        [raised, closer.wait]
      end.wait
      p [outcome, held.value]
    RUBY
    expect(status).to be_success
    expect(eval(output.lines.last)).to eq([["interrupted", true], "a"]) # rubocop:disable Security/Eval
  end

  # Mutex#unlock wakes only the first waiter. A sibling fiber waits for the
  # transport's lock before the closer does; a closer that waited as a
  # blocking fiber would sleep the reactor thread, so the unlock's scheduler
  # wake-up of the sibling could never run and the closer would never wake.
  it "closes while a sibling fiber waits for the transport's lock" do
    output, status = reactor_child(<<~RUBY, pool: false)
      transport = ConnectRPC::Hyper::Transport.new
      driver = transport.send(:driver)
      lock = transport.instance_variable_get(:@lock)
      held = Thread::Queue.new
      closing = Thread::Queue.new
      holder = Thread.new do
        lock.synchronize do
          held << true
          reactor = closing.pop
          # The closer waits for the lock by the time the reactor thread sleeps.
          eventually { reactor.status == "sleep" } || raise("the reactor thread never slept")
        end
      end
      held.pop
      reactor = Thread.new do
        Async do |task|
          # Runs until it waits for the lock.
          sibling = task.async do
            transport.send(:driver).equal?(driver)
          rescue ConnectRPC::Error => e
            e.message
          end
          closing << Thread.current
          closer = task.async { transport.close(grace: 1) }
          [closer.wait, sibling.wait]
        end.wait
      end
      if reactor.join(10)
        p [reactor.value, holder.value]
      else
        p :hung
        exit!(0)
      end
    RUBY
    expect(status).to be_success
    expect(eval(output.lines.last)).to eq([[true, true], true]) # rubocop:disable Security/Eval
  end

  # A closer that waits for a contended lock yields to its reactor, so a
  # sibling fiber runs meanwhile; Thread.new copies the reactor thread's
  # interrupt mask into the thread the sibling starts.
  it "lets Thread#raise reach a thread a sibling fiber starts while a closer waits for the transport's lock" do
    output, status = reactor_child(<<~RUBY, pool: false)
      transport = ConnectRPC::Hyper::Transport.new
      lock = transport.instance_variable_get(:@lock)
      held = Thread::Queue.new
      closing = Thread::Queue.new
      checked = Thread::Queue.new
      holder = Thread.new do
        lock.synchronize do
          held << true
          reactor = closing.pop
          # Released after the sibling's check, or once the reactor thread
          # sleeps, so a closer that blocks the reactor thread cannot deadlock.
          eventually { !checked.empty? || reactor.status == "sleep" } || raise("never released")
        end
      end
      held.pop
      outcome = Async do |task|
        closing << Thread.current
        closer = task.async { transport.close(grace: 1) }
        sibling = task.async do
          started = Thread::Queue.new
          blocker = Thread.new do
            started << true
            Thread::Queue.new.pop(timeout: 5)
            :not_interrupted
          rescue RuntimeError => e
            e.message
          end
          started.pop
          blocker.raise("interrupted")
          blocker.join(2) ? blocker.value : :still_blocked
        ensure
          checked << true
        end
        [closer.wait, sibling.wait]
      end.wait
      p [outcome, holder.value]
    RUBY
    expect(status).to be_success
    expect(eval(output.lines.last)).to eq([[true, "interrupted"], true]) # rubocop:disable Security/Eval
  end

  # A frozen ThreadGroup makes Thread.new raise ThreadError on the reactor
  # thread, so the closer waits on it instead, as without a scheduler.
  it "waits on the calling thread when a closer cannot start its helper thread" do
    output, status = reactor_child(<<~RUBY, pool: false)
      #{held_call}
      releaser = Thread.new do
        eventually { wait_bodies[1] >= 1 }
        release << true
      end
      outcome = Async do
        group = ThreadGroup.new
        group.add(Thread.current)
        group.freeze
        transport.close(grace: 10)
      rescue ThreadError => e
        [:thread_error, e.message]
      end.wait
      releaser.join
      p [outcome, driver.close_outcome, held.value]
    RUBY
    expect(status).to be_success
    expect(eval(output.lines.last)).to eq([true, true, "a"]) # rubocop:disable Security/Eval
  end

  # The first closer's helper thread is held before native close until a
  # later closer that asks for no grace waits in native close, so the later
  # closer starts teardown. It must start it with the first closer's grace,
  # and the held call drains.
  it "starts teardown with the first closer's grace when a later closer reaches native close first" do
    output, status = reactor_child(<<~RUBY, pool: false)
      #{held_call}
      graces = Thread::Queue.new
      ConnectRPC::Hyper.const_get(:Native)::Driver.prepend(Module.new do
        define_method(:close) do |grace|
          first = graces.empty?
          graces << grace
          eventually { wait_bodies[1] >= 1 || !close_outcome.nil? } if first
          super(grace)
        end
      end)
      outcome = Async do |task|
        first = task.async { transport.close(grace: 10) }
        eventually { !graces.empty? } || raise("the first closer never reached native close")
        later = Thread.new { transport.close(grace: 0) }
        # Both closers wait in native close, or teardown already ended.
        eventually { wait_bodies[1] >= 2 || !driver.close_outcome.nil? }
        release << true
        [first.wait, later.value]
      end.wait
      p [outcome, held.value, Array.new(graces.size) { graces.pop }]
    RUBY
    expect(status).to be_success
    expect(eval(output.lines.last)).to eq([[true, true], "a", [10.0, 10.0]]) # rubocop:disable Security/Eval
  end

  it "still forces teardown after the grace when Task#stop lands on a closing task" do
    from = peer.mark
    output, status = reactor_child(<<~RUBY, pool: true)
      transport = ConnectRPC::Hyper::Transport.new
      ready = Queue.new
      held = Thread.new do
        client(#{peer['grpc_h2c'].dump}, transport).server_stream(MSG.new(value: "hold"), timeout: 30) do |reader|
          reader.receive
          ready << true
          reader.receive
        end
      rescue ConnectRPC::Error => e
        [e.code, e.message]
      end
      parked(ready: ready)
      driver = transport.instance_variable_get(:@driver)
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      pending_at_stop = nil
      outcome = Async do |task|
        # The grace leaves ample time to observe both waits and deliver the
        # stop; the held stream outlasts it, so teardown is forced.
        closer = task.async do
          transport.close(grace: 3)
        rescue Async::Stop
          :stopped
        end
        # The held read and the close wait on its helper thread.
        parked(2)
        closer.stop
        pending_at_stop = driver.close_outcome.nil?
        closer.wait
      end.wait
      stopped_in = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
      p [outcome, pending_at_stop, held.value, transport.close(grace: 5), driver.stats[1..2], stopped_in < 15]
    RUBY
    expect(status).to be_success
    expect(eval(output.lines.last)).to eq( # rubocop:disable Security/Eval
      [:stopped, true, ["unavailable", "HTTP transport failed: connection closed"], false, [0, 0], true]
    )
    peer.wait_event(/grpc ss-hold-done context canceled/, from: from)
  end

  # The held call waits in Ruby, so the only native wait is the close on its
  # helper thread; the closer is stopped there and the call drains cleanly
  # afterwards.
  it "keeps a clean drain's true outcome when Task#stop lands on a closing task" do
    output, status = reactor_child(<<~RUBY, pool: true)
      transport = ConnectRPC::Hyper::Transport.new
      sent = Queue.new
      release = Queue.new
      held = Thread.new do
        client(#{peer['grpc_h2c'].dump}, transport).client_stream(timeout: 30) do |writer|
          writer.send_message(MSG.new(value: "a"))
          sent << true
          release.pop
        end.message.value
      end
      sent.pop
      outcome = Async do |task|
        closer = task.async do
          transport.close(grace: 10)
        rescue Async::Stop
          :stopped
        end
        parked
        closer.stop
        release << true
        closer.wait
      end.wait
      driver = transport.instance_variable_get(:@driver)
      p [outcome, held.value, transport.close(grace: 5), driver.stats[1..2]]
    RUBY
    expect(status).to be_success
    expect(eval(output.lines.last)).to eq([:stopped, "a", true, [0, 0]]) # rubocop:disable Security/Eval
  end

  [true, false].each do |pool|
    it "raises Task#stop on a concurrent closer only after the first closer's outcome is published, " \
       "#{pool ? 'with' : 'without'} the worker pool" do
      expect(concurrent_closers(pool: pool))
        .to eq([[:stopped, false], false, false, "HTTP transport failed: connection closed", [0, 0]])
    end
  end

  context "with the worker pool on Ruby 4.0" do
    before { skip "the worker pool needs Ruby 4.0" unless offload }

    it "lands Task#stop, cancellation, the deadline, Thread#raise and Thread#kill on a parked call" do
      from = peer.mark
      output, status = reactor_child(<<~RUBY, pool: true)
        peer_client = client(#{peer['grpc_h2c'].dump}, ConnectRPC::Hyper::Transport.new)
        # Parks in a read of a held stream once its first message arrived.
        hold = lambda do |ready, timeout: 20, **options|
          peer_client.server_stream(MSG.new(value: "hold"), timeout: timeout, **options) do |reader|
            reader.receive
            ready << true
            reader.receive
          end
        end
        results = []
        Async do |task|
          ready = Queue.new
          caller = task.async do
            hold.call(ready)
            :finished
          rescue Async::Stop
            :stopped
          end
          parked(ready: ready)
          caller.stop
          results << caller.wait
          source = ConnectRPC::CancellationSource.new
          ready = Queue.new
          canceled = task.async do
            hold.call(ready, cancellation: source.token)
          rescue ConnectRPC::Error => e
            e.code
          end
          parked(ready: ready)
          source.cancel
          results << canceled.wait
          expired = task.async do
            hold.call(Queue.new, timeout: 0.2)
          rescue ConnectRPC::Error => e
            e.code
          end
          results << expired.wait
        end
        [[:raise, RuntimeError], [:kill, nil]].each do |action, error|
          outcome = Queue.new
          ready = Queue.new
          thread = Thread.new do
            Thread.current.report_on_exception = false
            Async { hold.call(ready) }
          rescue Exception => e
            outcome << e.class.name
          ensure
            outcome << :ensure
          end
          parked(ready: ready)
          action == :raise ? thread.raise(error, "x") : thread.kill
          thread.join(5) || raise("thread did not exit")
          got = []
          got << outcome.pop until outcome.empty?
          results << got
        end
        results << Async { peer_client.unary(MSG.new(value: "after")).message.value }.wait
        p results
      RUBY
      expect(status).to be_success
      expect(eval(output.lines.last)).to eq( # rubocop:disable Security/Eval
        [:stopped, "canceled", "deadline_exceeded", ["RuntimeError", :ensure], [:ensure], "after"]
      )
      # Every parked stream reached the peer as cancellation or its deadline.
      peer.wait_count(/grpc ss-hold-done context (canceled|deadline exceeded)/, 5, from: from)
    end

    # async's default worker pool has one worker, which runs parked waits one
    # at a time; two workers let the read and the write wait together.
    it "parks a read and a write of one bidi stream in two fibers, then stops both" do
      from = peer.mark
      output, status = reactor_child(<<~RUBY, pool: false)
        peer_client = client(#{peer['grpc_h2c'].dump}, ConnectRPC::Hyper::Transport.new)
        outcome = nil
        Fiber.set_scheduler(Async::Scheduler.new(worker_pool: IO::Event::WorkerPool.new(maximum_worker_count: 2)))
        Async do |task|
          call = task.async do
            peer_client.bidi(timeout: 20) do |stream|
              stream.send_message(MSG.new(value: "pause:offload"))
              writer = task.async { loop { stream.send_message(MSG.new(payload: "x".b * 65_536)) } }
              reader = task.async { stream.receive }
              [writer, reader].each(&:wait)
            end
            :finished
          rescue Async::Stop
            :stopped
          end
          # The reader waits for a message that never comes; the writer waits on
          # flow control once the paused peer's window is spent. Both wait
          # bodies run, one on each worker.
          running(2)
          call.stop
          outcome = call.wait
        end
        Fiber.set_scheduler(nil)
        p outcome
      RUBY
      expect(status).to be_success
      expect(output.lines.last.chomp).to eq(":stopped")
      peer.wait_event(/grpc bidi-paused offload/, from: from)
      peer.wait_event(/grpc bidi-done context canceled/, from: from)
      peer.command("release offload")
    end
  end
end
