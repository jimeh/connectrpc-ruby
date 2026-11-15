# frozen_string_literal: true

RSpec.describe "HTTP/2 GOAWAY draining and replacement" do
  def holds = Hash.new { |hash, path| hash[path] = Async::Queue.new }

  # Records each incoming request by path, holds /long paths until the
  # example releases them and refuses the first /refused stream above the
  # GOAWAY last stream id.
  def drain_peer(task, listener, held, seen)
    ConnectionPeer::RecordingServer.new(task, listener) do |incoming|
      first = !seen.key?(incoming.path)
      seen[incoming.path] = incoming
      if incoming.path == "/refused" && first
        goaway(incoming.stream.connection, incoming.stream.id - 2)
      else
        held[incoming.path].dequeue if incoming.path.start_with?("/long")
        begin
          incoming.send_response(Protocol::HTTP::Response[200, {}, [incoming.path]])
        rescue Protocol::HTTP2::Error, IOError
          # The client aborted a held stream; there is nothing to answer.
          nil
        end
      end
    end
  end

  def goaway(server, last_stream_id, code = Protocol::HTTP2::Error::NO_ERROR)
    frame = Protocol::HTTP2::GoawayFrame.new
    frame.pack(last_stream_id, code, "")
    server.write_frame(frame)
  end

  # The client acknowledges a PING after processing every earlier frame.
  def ping_through(task, peer, server)
    pings = peer.frames.count { |type, *| type == Protocol::HTTP2::PingFrame::TYPE }
    server.send_ping("drained!")
    wait_until(task, "PING acknowledged") do
      peer.frames.count { |type, *| type == Protocol::HTTP2::PingFrame::TYPE } > pings
    end
  end

  def draining(transport) = connection_manager(transport).instance_variable_get(:@draining)
  def attempt(transport) = connection_manager(transport).instance_variable_get(:@attempt)
  def acquiring(transport) = transport.instance_variable_get(:@acquiring)
  def resources(transport) = transport.instance_variable_get(:@resources)
  def policy(**) = ConnectRPC::Async::ConnectionPolicy.new(**)

  def headers_for(peer, seen, path)
    ids = peer.headers_frames.map { |_, stream_id, _| stream_id }
    ids.count { |id| id == seen.fetch(path).stream.id }
  end

  it "opens a replacement while a graceful GOAWAY connection drains its accepted stream" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      held = holds
      seen = {}
      peer = drain_peer(task, bound.sockets.first, held, seen)
      origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
      events = []
      transport = recording_transport(origin, events)
      meter = ConnectionPeer::SocketMeter.new(transport)
      long = task.async { transport.call(request("#{origin}/long")) }
      wait_until(task, "long stream accepted") { seen.key?("/long") }
      old = resources(transport).first
      goaway(peer.connections.first, seen.fetch("/long").stream.id)
      wait_until(task, "connection draining") { draining(transport).size == 1 }
      # GOAWAY alone never dials.
      expect([peer.accepted, meter.live, attempt(transport)]).to eq([1, 1, nil])
      calls = 3.times.map { |index| task.async { transport.call(request("#{origin}/new-#{index}")).body } }
      expect(calls.map(&:wait)).to eq(%w[/new-0 /new-1 /new-2])
      expect([peer.accepted, meter.live]).to eq([2, 2])
      expect(long).not_to be_finished
      held["/long"].enqueue(true)
      expect(long.wait.body).to eq("/long")
      wait_until(task, "drained connection closed") { old.raw.closed? && draining(transport).empty? }
      expect([meter.live, meter.peak]).to eq([1, 2])
      expect(attempt_outcomes(events)).to eq(%i[ready ready])
      expect(connection_manager(transport).instance_variable_get(:@failures)).to eq(0)
      expect(peer.headers_frames.size).to eq(4)
      expect(lease_usage(transport)).to eq(0)
    ensure
      held&.each_value { |queue| queue.enqueue(true) }
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  it "keeps a removed connection's slot reserved until its raw socket closes" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      peer = drain_peer(task, bound.sockets.first, holds, {})
      origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      meter = ConnectionPeer::SocketMeter.new(transport)
      expect(transport.call(request("#{origin}/warm")).body).to eq("/warm")
      resource = resources(transport).first
      entered = Async::Queue.new
      gate = Async::Queue.new
      gated = false
      # Hold the first abort before its raw close, after accounting removal.
      resource.define_singleton_method(:abort) do |*args|
        unless gated
          gated = true
          entered.enqueue(true)
          gate.dequeue
        end
        super(*args)
      end
      goaway(peer.connections.first, 1)
      entered.dequeue
      expect(connection_manager(transport).connections).to be_empty
      expect(resource.raw).not_to be_closed
      call = task.async { transport.call(request("#{origin}/next")).body }
      wait_until(task, "call waits for a slot") { acquiring(transport).size == 1 }
      expect(attempt(transport)).to be_nil
      expect(peer.accepted).to eq(1)
      gate.enqueue(true)
      expect(call.wait).to eq("/next")
      expect(resource.raw).to be_closed
      expect([peer.accepted, meter.peak]).to eq([2, 1])
      expect(lease_usage(transport)).to eq(0)
    ensure
      gate&.enqueue(true)
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  %i[draining slot].each do |released|
    it "keeps a second GOAWAY connection in its slot while the set is full until the #{released} connection retires" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        held = holds
        seen = {}
        peer = drain_peer(task, bound.sockets.first, held, seen)
        origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
        events = []
        transport = recording_transport(origin, events)
        meter = ConnectionPeer::SocketMeter.new(transport)
        first = task.async { transport.call(request("#{origin}/long-a")).body }
        wait_until(task, "first long stream accepted") { seen.key?("/long-a") }
        goaway(peer.connections[0], seen.fetch("/long-a").stream.id)
        wait_until(task, "first connection draining") { draining(transport).size == 1 }
        second = task.async { transport.call(request("#{origin}/long-b")).body }
        wait_until(task, "second long stream accepted") { seen.key?("/long-b") }
        connection_a, connection_b = resources(transport).map(&:connection)
        goaway(peer.connections[1], seen.fetch("/long-b").stream.id)
        wait_until(task, "no usable connection") { transport.connection_state == :idle }
        expect(draining(transport).keys).to eq([connection_a])
        expect(connection_manager(transport).connections.keys).to contain_exactly(connection_a, connection_b)
        third = task.async { transport.call(request("#{origin}/third")).body }
        wait_until(task, "third call waits") { acquiring(transport).size == 1 }
        expect([attempt(transport), peer.accepted, meter.live]).to eq([nil, 2, 2])
        if released == :draining
          held["/long-a"].enqueue(true)
          expect(first.wait).to eq("/long-a")
          expect(third.wait).to eq("/third")
          # The slot holder joined the set when the draining member retired.
          expect(draining(transport).keys).to eq([connection_b])
          held["/long-b"].enqueue(true)
        else
          held["/long-b"].enqueue(true)
          expect(second.wait).to eq("/long-b")
          expect(third.wait).to eq("/third")
          expect(draining(transport).keys).to eq([connection_a])
          held["/long-a"].enqueue(true)
        end
        expect([first.wait, second.wait]).to eq(%w[/long-a /long-b])
        wait_until(task, "drains finished") { draining(transport).empty? }
        expect([peer.accepted, meter.peak]).to eq([3, 2])
        expect(attempt_outcomes(events)).to eq(%i[ready ready ready])
        expect(lease_usage(transport)).to eq(0)
      ensure
        held&.each_value { |queue| queue.enqueue(true) }
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end
  end

  it "with max_draining 0 opens a replacement only in a free slot under connection_limit" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      held = holds
      seen = {}
      peer = drain_peer(task, bound.sockets.first, held, seen)
      origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin, connection_limit: 2,
                                                   connection_policy: policy(max_draining: 0))
      meter = ConnectionPeer::SocketMeter.new(transport)
      long = task.async { transport.call(request("#{origin}/long")).body }
      wait_until(task, "long stream accepted") { seen.key?("/long") }
      goaway(peer.connections.first, seen.fetch("/long").stream.id)
      wait_until(task, "GOAWAY observed") { transport.connection_state == :idle }
      expect(draining(transport)).to be_empty
      expect(transport.call(request("#{origin}/next")).body).to eq("/next")
      expect([peer.accepted, meter.live]).to eq([2, 2])
      held["/long"].enqueue(true)
      expect(long.wait).to eq("/long")
      expect(meter.peak).to eq(2)
      expect(lease_usage(transport)).to eq(0)
    ensure
      held&.each_value { |queue| queue.enqueue(true) }
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  it "with max_draining 0 holds a refused retry until the GOAWAY connection retires" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      held = holds
      seen = {}
      peer = drain_peer(task, bound.sockets.first, held, seen)
      origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin, connection_policy: policy(max_draining: 0))
      meter = ConnectionPeer::SocketMeter.new(transport)
      long = task.async { transport.call(request("#{origin}/long")).body }
      wait_until(task, "long stream accepted") { seen.key?("/long") }
      refused = task.async { transport.call(request("#{origin}/refused")).body }
      wait_until(task, "refused retry waits for the only slot") do
        seen.key?("/refused") && acquiring(transport).size == 1
      end
      expect([attempt(transport), peer.accepted, headers_for(peer, seen, "/refused")]).to eq([nil, 1, 1])
      held["/long"].enqueue(true)
      expect(long.wait).to eq("/long")
      expect(refused.wait).to eq("/refused")
      expect(peer.headers_frames.size).to eq(3)
      expect([peer.accepted, meter.peak]).to eq([2, 1])
      expect(lease_usage(transport)).to eq(0)
    ensure
      held&.each_value { |queue| queue.enqueue(true) }
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  [1, 0].each do |max_draining|
    it "aborts a GOAWAY connection at drain_timeout without resending its stream (max_draining #{max_draining})" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        held = holds
        seen = {}
        peer = drain_peer(task, bound.sockets.first, held, seen)
        origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
        transport = ConnectRPC::Async::Transport.new(
          origin: origin, connection_policy: policy(max_draining: max_draining, drain_timeout: 5)
        )
        clock = install_clock(transport)
        long = task.async(finished: false) { transport.call(request("#{origin}/long")) }
        wait_until(task, "long stream accepted") { seen.key?("/long") }
        resource = resources(transport).first
        server = peer.connections.first
        goaway(server, seen.fetch("/long").stream.id)
        wait_until(task, "drain timer armed") { clock.sleeper(5) }
        # A repeated GOAWAY does not restart the timer.
        goaway(server, seen.fetch("/long").stream.id)
        ping_through(task, peer, server)
        expect([clock.delays, clock.sleepers.size]).to eq([[5], 1])
        clock.sleeper(5).fire
        expect { long.wait }.to unavailable_error
        expect(resource).to be_finished
        expect(peer.headers_frames.size).to eq(1)
        expect(transport.call(request("#{origin}/after")).body).to eq("/after")
        expect([peer.accepted, peer.headers_frames.size]).to eq([2, 2])
        expect(lease_usage(transport)).to eq(0)
      ensure
        held&.each_value { |queue| queue.enqueue(true) }
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end
  end

  %i[retirement close].each do |ending|
    it "cancels the drain timer at #{ending}" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        held = holds
        seen = {}
        peer = drain_peer(task, bound.sockets.first, held, seen)
        origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin, connection_policy: policy(drain_timeout: 5))
        clock = install_clock(transport)
        manager = connection_manager(transport)
        long = task.async { transport.call(request("#{origin}/long")).body }
        wait_until(task, "long stream accepted") { seen.key?("/long") }
        resource = resources(transport).first
        goaway(peer.connections.first, seen.fetch("/long").stream.id)
        wait_until(task, "drain timer armed") { clock.sleeper(5) }
        if ending == :retirement
          held["/long"].enqueue(true)
          expect(long.wait).to eq("/long")
          wait_until(task, "timer cancelled") { clock.sleepers.empty? }
          expect(manager.instance_variable_get(:@drain_timers)).to be_empty
          expect(transport.call(request("#{origin}/after")).body).to eq("/after")
          expect(resources(transport).last).not_to be_finished
          expect(peer.accepted).to eq(2)
        else
          closer = task.async { transport.close(grace: 5) }
          wait_until(task, "timer cancelled by close") do
            clock.sleepers.empty? && manager.instance_variable_get(:@drain_timers).empty?
          end
          expect([long.finished?, resource.finished?]).to eq([false, false])
          held["/long"].enqueue(true)
          expect(long.wait).to eq("/long")
          expect(closer.wait).to be(true)
        end
        expect(lease_usage(transport)).to eq(0)
      ensure
        held&.each_value { |queue| queue.enqueue(true) }
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end
  end
end
