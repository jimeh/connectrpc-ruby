# frozen_string_literal: true

RSpec.describe "HTTP/2 connection growth and peer capacity" do
  def streams_setting(count) = { Protocol::HTTP2::Settings::MAXIMUM_CONCURRENT_STREAMS => count }

  def holding_peer(task, listener, release, settings)
    ConnectionPeer::RecordingServer.new(task, listener, settings: settings) do |incoming|
      release.dequeue
      incoming.send_response(Protocol::HTTP::Response[200, {}, [incoming.path]])
    end
  end

  it "opens a second connection only when the first is full and never a third" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      release = Async::Queue.new
      peer = holding_peer(task, bound.sockets.first, release, streams_setting(2))
      origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
      events = []
      transport = recording_transport(origin, events, connection_limit: 2)
      calls = 4.times.map do |index|
        call = task.async { transport.call(request("#{origin}/#{index}")) }
        wait_until(task, "call #{index} dispatched") { peer.headers_frames.size == index + 1 }
        call
      end
      expect(peer.accepted).to eq(2)
      expect(connection_manager(transport).connections.values).to eq([2, 2])
      calls << task.async { transport.call(request("#{origin}/4")) }
      wait_until(task, "fifth call waits for capacity") { transport.instance_variable_get(:@acquiring).size == 1 }
      expect(peer.accepted).to eq(2)
      expect(attempt_outcomes(events)).to eq(%i[ready ready])
      5.times { release.enqueue(true) }
      expect(calls.map { |call| call.wait.body }).to eq(%w[/0 /1 /2 /3 /4])
      expect(peer.accepted).to eq(2)
      expect(lease_usage(transport)).to eq(0)
    ensure
      5.times { release&.enqueue(true) }
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  it "wakes a waiting call when the peer raises its stream limit and keeps a lease when it lowers it" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      release = Async::Queue.new
      peer = holding_peer(task, bound.sockets.first, release, streams_setting(0))
      origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      held = task.async { transport.call(request("#{origin}/held")) }
      wait_until(task, "waiting on zero capacity") do
        transport.connection_state == :ready && transport.instance_variable_get(:@acquiring).size == 1
      end
      server = peer.connections.first
      server.send_settings(streams_setting(1))
      wait_until(task, "dispatched after SETTINGS") { peer.headers_frames.size == 1 }
      server.send_settings(streams_setting(0))
      second = task.async { transport.call(request("#{origin}/second")) }
      wait_until(task, "second waits") { transport.instance_variable_get(:@acquiring).size == 1 }
      release.enqueue(true)
      expect(held.wait.body).to eq("/held")
      expect(peer.headers_frames.size).to eq(1)
      server.send_settings(streams_setting(1))
      release.enqueue(true)
      expect(second.wait.body).to eq("/second")
      expect(peer.accepted).to eq(1)
    ensure
      2.times { release&.enqueue(true) }
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  it "with max_draining 0 keeps a GOAWAY connection in its slot until a waiting call takes the freed slot" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      release = Async::Queue.new
      peer = holding_peer(task, bound.sockets.first, release, {})
      origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
      events = []
      policy = ConnectRPC::Async::ConnectionPolicy.new(max_draining: 0)
      transport = recording_transport(origin, events, connection_policy: policy)
      long = task.async { transport.call(request("#{origin}/long")) }
      wait_until(task, "long stream dispatched") { peer.headers_frames.size == 1 }
      _, stream_id, = peer.headers_frames.first
      goaway = Protocol::HTTP2::GoawayFrame.new
      goaway.pack(stream_id, Protocol::HTTP2::Error::NO_ERROR, "")
      peer.connections.first.write_frame(goaway)
      wait_until(task, "GOAWAY observed") { transport.connection_state == :idle }
      idle = events.reverse.find { |event| event.type == :state_changed }
      expect([idle.previous_state, idle.active, idle.draining]).to eq([:ready, 0, 1])
      waiting = task.async { transport.call(request("#{origin}/waiting")) }
      wait_until(task, "caller waits for the draining slot") { transport.instance_variable_get(:@acquiring).size == 1 }
      expect(peer.accepted).to eq(1)
      2.times { release.enqueue(true) }
      expect(long.wait.body).to eq("/long")
      expect(waiting.wait.body).to eq("/waiting")
      expect(peer.accepted).to eq(2)
      expect(state_changes(events)).to eq(%i[connecting ready idle connecting ready])
    ensure
      2.times { release&.enqueue(true) }
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  it "fails an attempt whose connection reaches EOF right after SETTINGS without publishing ready" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      listener = bound.sockets.first
      peer = task.async do
        raw, = listener.accept
        Async::HTTP::Protocol::HTTP2::Server.new(IO::Stream(raw)).read_connection_preface({})
        raw.close
      end
      origin = "http://127.0.0.1:#{listener.local_address.ip_port}"
      events = []
      transport = recording_transport(origin, events)
      expect { transport.call(request("#{origin}/early")) }.to unavailable_error
      peer.wait
      expect(attempt_outcomes(events)).to eq([:closed])
      expect(state_changes(events)).to eq(%i[connecting transient_failure])
      expect(connection_manager(transport).connections).to be_empty
    ensure
      transport&.close(grace: 0)
      bound&.close
    end
  end

  # Every reconcile trigger runs before the woken waiter resumes, while another
  # connection already has spare capacity. None may dial a third connection.
  %i[retire unusable timer].each do |trigger|
    it "does not expand on #{trigger} reconcile while another connection has spare capacity" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        refuse = trigger == :timer ? [2] : []
        peer = ConnectionPeer::RecordingServer.new(task, bound.sockets.first, settings: streams_setting(1),
                                                                              refuse: refuse)
        origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
        events = []
        policy = ConnectRPC::Async::ConnectionPolicy.new(wait_for_ready: trigger == :timer)
        transport = recording_transport(origin, events, connection_limit: 2, connection_policy: policy)
        manager = connection_manager(transport)
        waiters = manager.instance_variable_get(:@waiters)
        first = manager.acquire
        if trigger == :timer
          clock = install_clock(transport)
          waiting = task.async { manager.acquire }
          # The closed expansion backs off; the wait-for-ready waiter keeps demand.
          clock.next_delay
          expect(attempt_outcomes(events)).to eq(%i[ready closed])
          progress = transport.instance_variable_get(:@progress)
          wait_until(task, "waiter blocked during backoff") { progress.waiting_count == 1 }
          # Fire first so the timer's reconcile runs before the released waiter.
          clock.fire
          manager.release(first)
          expected = first
        else
          second = manager.acquire
          waiting = task.async { manager.acquire }
          wait_until(task, "capacity waiter blocked") { waiters.size == 1 }
          manager.release(second)
          if trigger == :retire
            first.close!
            manager.release(first)
          else
            manager.release(first)
            # The GOAWAY notification hook, run before the woken waiter resumes.
            first.resource.unusable
          end
          expected = second
        end
        expect(waiting.wait).to equal(expected)
        manager.release(expected)
        wait_until(task, "no attempt in flight") { manager.instance_variable_get(:@attempt).nil? }
        expect(attempt_outcomes(events)).to eq(trigger == :timer ? %i[ready closed] : %i[ready ready])
        expect(peer.accepted).to eq(2)
      ensure
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end
  end
end
