# frozen_string_literal: true

RSpec.describe "HTTP/2 connection backoff" do
  def backoff_transport(origin, events, wait_for_ready: false, base: 1, connection_limit: 1)
    backoff = ConnectRPC::Async::Backoff.new(base: base, multiplier: 2, jitter: 0.5, max: 5)
    policy = ConnectRPC::Async::ConnectionPolicy.new(backoff: backoff, wait_for_ready: wait_for_ready)
    recording_transport(origin, events, connection_policy: policy, connection_limit: connection_limit)
  end

  def manager_failures(transport) = connection_manager(transport).instance_variable_get(:@failures)

  it "grows, caps and jitters delays, survives bursts and idle, and resets only at readiness" do
    reactor do |task|
      reserved = refusing_socket
      origin = "http://127.0.0.1:#{reserved.local_address.ip_port}"
      events = []
      transport = backoff_transport(origin, events)
      clock = install_clock(transport, random: ConnectionPeer::SequenceRandom.new(0.0, 0.5, 0.999, 0.0, 0.2, 0.5))
      delays = []
      5.times do |round|
        expect { transport.call(request("#{origin}/fail")) }.to unavailable_error
        delays << clock.next_delay
        expect(transport.connection_state).to eq(:transient_failure)
        next unless round.zero?

        # A burst during backoff fails fast without dialing or touching the timer.
        timer = connection_manager(transport).instance_variable_get(:@timer)
        3.times { expect { transport.call(request("#{origin}/burst")) }.to unavailable_error }
        expect(attempt_outcomes(events)).to eq([:refused])
        expect(connection_manager(transport).instance_variable_get(:@timer)).to equal(timer)
        # A timer that fires with no waiter starts nothing; failures persist.
      ensure
        clock.fire
        wait_until(task, "idle after backoff") { transport.connection_state == :idle }
      end
      expect(delays.map { |delay| delay.round(6) }).to eq([1.0, 1.5, 2.002, 5.0, 4.5])
      expect(attempt_outcomes(events)).to eq([:refused] * 5)
      expect(clock.delays.size).to eq(5)
      expect(manager_failures(transport)).to eq(5)
      expect(state_changes(events)).to eq(%i[connecting transient_failure idle] * 5)

      reserved.listen(8)
      peer = ConnectionPeer::RecordingServer.new(task, reserved)
      expect(transport.call(request("#{origin}/ready")).body).to eq("ok")
      expect(manager_failures(transport)).to eq(0)
      peer.stop
      reserved.close
      wait_until(task, "idle after peer close") { transport.connection_state == :idle }
      expect { transport.call(request("#{origin}/again")) }.to unavailable_error
      # Readiness reset the count, so the next failure backs off from base.
      expect(clock.next_delay).to eq(0.75)
      expect(attempt_outcomes(events)).to eq(([:refused] * 5) + %i[ready refused])
    ensure
      transport&.close(grace: 0)
      peer&.stop
      reserved&.close unless reserved&.closed?
    end
  end

  it "keeps the published state ready while a failed expansion backs off" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      release = Async::Queue.new
      settings = { Protocol::HTTP2::Settings::MAXIMUM_CONCURRENT_STREAMS => 1 }
      peer = ConnectionPeer::RecordingServer.new(task, bound.sockets.first, settings: settings,
                                                                            refuse: [2]) do |incoming|
        release.dequeue
        incoming.send_response(Protocol::HTTP::Response[200, {}, [incoming.path]])
      end
      origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
      events = []
      transport = backoff_transport(origin, events, base: 2, connection_limit: 2)
      clock = install_clock(transport)
      first = task.async { transport.call(request("#{origin}/first")) }
      wait_until(task, "first dispatched") { peer.headers_frames.size == 1 }
      second = task.async { transport.call(request("#{origin}/second")) }
      expect(clock.next_delay).to eq(2.0)
      expect(attempt_outcomes(events)).to eq(%i[ready closed])
      expect(transport.connection_state).to eq(:ready)
      third = task.async { transport.call(request("#{origin}/third")) }
      wait_until(task, "two capacity waiters") { transport.instance_variable_get(:@acquiring).size == 2 }
      3.times { release.enqueue(true) }
      expect([first, second, third].map { |call| call.wait.body }).to eq(%w[/first /second /third])
      expect(peer.accepted).to eq(2)
      expect(state_changes(events)).to eq(%i[connecting ready])
      clock.fire
      wait_until(task, "timer cleared") { connection_manager(transport).instance_variable_get(:@timer).nil? }
      expect(peer.accepted).to eq(2)
      expect(transport.connection_state).to eq(:ready)
    ensure
      3.times { release&.enqueue(true) }
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  it "shares paced attempts among wait-for-ready callers and dispatches only survivors once" do
    reactor do |task|
      reserved = refusing_socket
      origin = "http://127.0.0.1:#{reserved.local_address.ip_port}"
      events = []
      transport = backoff_transport(origin, events, wait_for_ready: true)
      clock = install_clock(transport)
      fast = ConnectRPC::Async::Transport.new(origin: origin)
      install_clock(fast)
      survivors = %w[a b].map { |name| task.async { transport.call(request("#{origin}/#{name}")) } }
      expect(clock.next_delay).to eq(1.0)
      source = ConnectRPC::CancellationSource.new
      cancelled = request("#{origin}/cancelled")
      cancelled.cancellation = source.token
      leavers = [task.async(finished: false) { transport.call(cancelled) },
                 task.async(finished: false) do
                   transport.call(request("#{origin}/expired", deadline: ConnectRPC::Deadline.new(0.05)))
                 end]
      wait_until(task, "four wait-for-ready callers") { transport.instance_variable_get(:@acquiring).size == 4 }
      expect(transport.connection_state).to eq(:transient_failure)
      expect { fast.call(request("#{origin}/fast")) }.to unavailable_error
      expect(fast.connection_state).to eq(:transient_failure)
      source.cancel
      expect { leavers.first.wait }.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("canceled") }
      expect { leavers.last.wait }.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("deadline_exceeded") }
      reserved.listen(8)
      peer = ConnectionPeer::RecordingServer.new(task, reserved) do |incoming|
        incoming.send_response(Protocol::HTTP::Response[200, {}, [incoming.path]])
      end
      clock.fire
      expect(survivors.map { |call| call.wait.body }).to match_array(%w[/a /b])
      expect(peer.accepted).to eq(1)
      expect(2.times.map { peer.requests.dequeue.path }).to match_array(%w[/a /b])
      expect(peer.requests).to be_empty
      streams = peer.headers_frames.map { |_, stream_id, _| stream_id }
      expect(streams.size).to eq(2)
      expect(peer.data_frames.map { |_, stream_id, _| stream_id } - streams).to be_empty
      expect(attempt_outcomes(events)).to eq(%i[refused ready])
      # The timer starts the next attempt directly, without an idle event.
      expect(state_changes(events)).to eq(%i[connecting transient_failure connecting ready])
      expect(clock.delays).to eq([1.0])
    ensure
      transport&.close(grace: 0)
      fast&.close(grace: 0)
      peer&.stop
      reserved&.close
    end
  end

  it "holds readiness and the failure count until the peer's first SETTINGS" do
    reactor do |task|
      reserved = refusing_socket
      origin = "http://127.0.0.1:#{reserved.local_address.ip_port}"
      events = []
      transport = backoff_transport(origin, events, wait_for_ready: true)
      clock = install_clock(transport)
      call = task.async { transport.call(request("#{origin}/gated")) }
      clock.next_delay
      reserved.listen(8)
      gate = Async::Queue.new
      peer = ConnectionPeer::RecordingServer.new(task, reserved, gate: gate)
      clock.fire
      wait_until(task, "gated connection accepted") { peer.accepted == 1 }
      expect(transport.connection_state).to eq(:connecting)
      expect(manager_failures(transport)).to eq(1)
      expect(attempt_outcomes(events)).to eq([:refused])
      gate.enqueue(true)
      expect(call.wait.body).to eq("ok")
      expect(manager_failures(transport)).to eq(0)
      expect(attempt_outcomes(events)).to eq(%i[refused ready])
    ensure
      gate&.enqueue(true)
      transport&.close(grace: 0)
      peer&.stop
      reserved&.close
    end
  end

  it "fails every caller joined to one dial with backoff disabled and redials only for a new call" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      listener = bound.sockets.first
      accepted = Async::Queue.new
      acceptor = task.async { loop { accepted.enqueue(listener.accept.first) } }
      origin = "http://127.0.0.1:#{listener.local_address.ip_port}"
      events = []
      transport = recording_transport(origin, events,
                                      connection_policy: ConnectRPC::Async::ConnectionPolicy.new(backoff: nil))
      calls = 3.times.map { |index| task.async(finished: false) { transport.call(request("#{origin}/#{index}")) } }
      raw = accepted.dequeue
      wait_until(task, "three joined callers") { transport.instance_variable_get(:@acquiring).size == 3 }
      raw.close
      calls.each { |call| expect { call.wait }.to unavailable_error }
      expect(attempt_outcomes(events)).to eq([:closed])
      expect(transport.connection_state).to eq(:idle)
      manager = connection_manager(transport)
      expect([manager.instance_variable_get(:@attempt), manager.instance_variable_get(:@timer)]).to eq([nil, nil])
      expect(accepted).to be_empty
      next_call = task.async(finished: false) { transport.call(request("#{origin}/next")) }
      accepted.dequeue.close
      expect { next_call.wait }.to unavailable_error
      expect(attempt_outcomes(events)).to eq(%i[closed closed])
      expect(state_changes(events)).to eq(%i[connecting idle connecting idle])
    ensure
      transport&.close(grace: 0)
      acceptor&.stop
      bound&.close
    end
  end

  it "wakes wait-for-ready callers with unavailable at close and cancels the pending timer" do
    reactor do |task|
      reserved = refusing_socket
      origin = "http://127.0.0.1:#{reserved.local_address.ip_port}"
      events = []
      transport = backoff_transport(origin, events, wait_for_ready: true)
      clock = install_clock(transport)
      call = task.async(finished: false) { transport.call(request("#{origin}/waiting")) }
      clock.next_delay
      timer = connection_manager(transport).instance_variable_get(:@timer)
      expect(transport.close(grace: 1)).to be(true)
      expect { call.wait }.to unavailable_error
      expect(timer).to be_finished
      clock.fire
      reserved.listen(8)
      expect(transport.connection_state).to eq(:closed)
      expect(state_changes(events)).to eq(%i[connecting transient_failure closing closed])
      expect(attempt_outcomes(events)).to eq([:refused])
      expect(reserved.accept_nonblock(exception: false)).to eq(:wait_readable)
    ensure
      transport&.close(grace: 0)
      reserved&.close
    end
  end

  %i[timer attempt].each do |race|
    it "leaves no late dial, registration or state change when close races #{race} completion" do
      reactor do |task|
        reserved = refusing_socket
        origin = "http://127.0.0.1:#{reserved.local_address.ip_port}"
        events = []
        transport = backoff_transport(origin, events, wait_for_ready: true)
        clock = install_clock(transport)
        call = task.async(finished: false) { transport.call(request("#{origin}/waiting")) }
        clock.next_delay
        reserved.listen(8)
        gate = Async::Queue.new
        peer = ConnectionPeer::RecordingServer.new(task, reserved, gate: gate)
        clock.fire
        if race == :attempt
          wait_until(task, "gated attempt accepted") { peer.accepted == 1 }
          gate.enqueue(true)
        end
        # Close in the same scheduling turn as the competing completion.
        closer = task.async { transport.close(grace: 1) }
        closer.wait
        expect { call.wait }.to unavailable_error
        expect(connection_manager(transport).connections).to be_empty
        expect(transport.instance_variable_get(:@resources)).to all(be_finished)
        closing = state_changes(events).index(:closing)
        expect(state_changes(events)[closing..]).to eq(%i[closing closed])
        expect(attempt_outcomes(events)).to eq([:refused])
        expect(peer.accepted).to eq(race == :attempt ? 1 : 0)
      ensure
        gate&.enqueue(true)
        transport&.close(grace: 0)
        peer&.stop
        reserved&.close
      end
    end
  end
end
