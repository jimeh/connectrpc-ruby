# frozen_string_literal: true

# RB_NOGVL_OFFLOAD_SAFE lets async's worker pool (Ruby 4.0, io-event's
# blocking-operation hook) run a native wait off the reactor thread. Each
# case runs in its own process, with and without the pool.
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
        started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        outcome = Async do |task|
          closer = task.async do
            transport.close(grace: 0.5)
          rescue Async::Stop
            :stopped
          end
          # The held read and the offloaded close wait.
          parked(2)
          closer.stop
          closer.wait
        end.wait
        stopped_in = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
        driver = transport.instance_variable_get(:@driver)
        p [outcome, held.value, transport.close(grace: 5), driver.stats[1..2], stopped_in < 10]
      RUBY
      expect(status).to be_success
      expect(eval(output.lines.last)).to eq( # rubocop:disable Security/Eval
        [:stopped, ["unavailable", "HTTP transport failed: connection closed"], false, [0, 0], true]
      )
      peer.wait_event(/grpc ss-hold-done context canceled/, from: from)
    end

    it "parks a read and a write of one bidi stream in two fibers, then stops both" do
      from = peer.mark
      output, status = reactor_child(<<~RUBY, pool: true)
        peer_client = client(#{peer['grpc_h2c'].dump}, ConnectRPC::Hyper::Transport.new)
        outcome = nil
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
          # flow control once the paused peer's window is spent.
          parked(2)
          call.stop
          outcome = call.wait
        end
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
