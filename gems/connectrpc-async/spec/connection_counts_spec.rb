# frozen_string_literal: true

# Published active, draining and pending-call counts. Every event carries the
# counts; counts_changed fires only when one changed without another event.
RSpec.describe "HTTP/2 connection count publication" do
  def counts(event) = [event.active, event.draining, event.pending_calls]

  # Consecutive distinct published counts.
  def transitions(events) = events.map { |event| counts(event) }.chunk_while { |a, b| a == b }.map(&:first)

  def gated_server(entered, release)
    server(echo: lambda { |message, _|
      entered.enqueue(message.name)
      release.dequeue if message.name.start_with?("held")
      message
    })
  end

  def holding_peer(task, listener, held)
    ConnectionPeer::RecordingServer.new(task, listener) do |incoming|
      held[incoming.path].dequeue if incoming.path.start_with?("/long")
      incoming.send_response(Protocol::HTTP::Response[200, {}, [incoming.path]])
    rescue Protocol::HTTP2::Error, IOError
      nil
    end
  end

  def goaway(server, last_stream_id)
    frame = Protocol::HTTP2::GoawayFrame.new
    frame.pack(last_stream_id, Protocol::HTTP2::Error::NO_ERROR, "")
    server.write_frame(frame)
  end

  def acquiring(transport) = transport.instance_variable_get(:@acquiring)

  # Blocks the first event the selector matches until the example releases it.
  def gated_observer(events, entered, release, &selected)
    armed = nil
    observer = lambda do |event|
      events << event
      next unless armed && selected.call(event)

      armed = false
      entered.enqueue(event)
      release.dequeue
    end
    [observer, -> { armed = true }]
  end

  it "publishes open, waiting and closed counts against a real host and nothing on the no-wait path" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      host = ConnectRPC::Async::Host.new(server: gated_server(entered, release), max_requests: 1).start
      origin = "http://127.0.0.1:#{host.port}"
      events = []
      transport = recording_transport(origin, events)
      typed = client(transport, origin)
      held = task.async { typed.echo(AsyncHelpers::MESSAGE.new(name: "held")) }
      expect(entered.dequeue).to eq("held")
      expect(counts(events.last)).to eq([1, 0, 0])
      waiting = task.async { typed.echo(AsyncHelpers::MESSAGE.new(name: "waiting")) }
      wait_until(task, "second call waits for the one stream") { acquiring(transport).size == 1 }
      expect(events.last.type).to eq(:counts_changed)
      expect(counts(events.last)).to eq([1, 0, 1])
      release.enqueue(true)
      expect(held.wait.message.name).to eq("held")
      expect(waiting.wait.message.name).to eq("waiting")
      expect(counts(events.last)).to eq([1, 0, 0])
      published = events.size
      expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "fast")).message.name).to eq("fast")
      expect(events.size).to eq(published)
      expect(transport.close(grace: 1)).to be(true)
      expect(transitions(events)).to eq([[0, 0, 1], [1, 0, 1], [1, 0, 0], [1, 0, 1], [1, 0, 0], [0, 0, 0]])
      expect(events.select { |event| event.type == :counts_changed }.map { |event| counts(event) }).to eq(
        [[1, 0, 0], [1, 0, 1], [1, 0, 0], [0, 0, 0]]
      )
    ensure
      2.times { release&.enqueue(true) }
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  [nil, 0].each do |max_draining|
    it "counts a GOAWAY connection as draining until its held call ends with max_draining #{max_draining.inspect}" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        held = Hash.new { |hash, path| hash[path] = Async::Queue.new }
        peer = holding_peer(task, bound.sockets.first, held)
        origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
        events = []
        options = max_draining.nil? ? {} : { max_draining: max_draining }
        transport = recording_transport(origin, events, connection_limit: 2,
                                                        connection_policy: ConnectRPC::Async::ConnectionPolicy.new(
                                                          **options
                                                        ))
        long = task.async { transport.call(request("#{origin}/long")).body }
        request = peer.requests.dequeue
        expect(counts(events.last)).to eq([1, 0, 0])
        goaway(peer.connections.first, request.stream.id)
        wait_until(task, "GOAWAY observed") { counts(events.last) == [0, 1, 0] }
        # A replacement becomes active while the drained connection finishes.
        expect(transport.call(request("#{origin}/next")).body).to eq("/next")
        expect(counts(events.last)).to eq([1, 1, 0])
        held["/long"].enqueue(true)
        expect(long.wait).to eq("/long")
        wait_until(task, "drained connection removed") { counts(events.last) == [1, 0, 0] }
        expect(events.last.type).to eq(:connection_closed)
        expect(transport.close(grace: 1)).to be(true)
        expect(counts(events.last)).to eq([0, 0, 0])
        expect(peer.accepted).to eq(2)
      ensure
        held&.each_value { |queue| queue.enqueue(true) }
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end
  end

  it "publishes zero active connections when the peer aborts a connection with a call in flight" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      held = Hash.new { |hash, path| hash[path] = Async::Queue.new }
      peer = holding_peer(task, bound.sockets.first, held)
      origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
      events = []
      transport = recording_transport(origin, events)
      long = task.async { transport.call(request("#{origin}/long")) }
      peer.requests.dequeue
      expect(counts(events.last)).to eq([1, 0, 0])
      AsyncHelpers::INTERNAL.raw_close(peer.raws.first)
      expect { long.wait }.to unavailable_error
      wait_until(task, "aborted connection removed") { counts(events.last) == [0, 0, 0] }
      expect(connection_manager(transport).connections).to be_empty
      expect(events.map(&:type)).to include(:connection_closed)
    ensure
      held&.each_value { |queue| queue.enqueue(true) }
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  it "ends at zero counts after a forced close with a call in flight and its cleanup joined" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      host = ConnectRPC::Async::Host.new(server: gated_server(entered, release)).start
      origin = "http://127.0.0.1:#{host.port}"
      events = []
      transport = recording_transport(origin, events)
      call = task.async(finished: false) { client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "held")) }
      entered.dequeue
      expect(transport.close(grace: 0)).to be(false)
      expect { call.wait }.to raise_error(ConnectRPC::Error)
      expect(transport.instance_variable_get(:@calls)).to be_empty
      expect(connection_manager(transport).connections).to be_empty
      expect(counts(events.last)).to eq([0, 0, 0])
      expect(events.map(&:state)).to include(:closed)
    ensure
      release&.enqueue(true)
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  it "takes capacity released while its own pending publication yields, with no further wakeup" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      host = ConnectRPC::Async::Host.new(server: gated_server(entered, release), max_requests: 1).start
      origin = "http://127.0.0.1:#{host.port}"
      events = []
      observed = Async::Queue.new
      resume = Async::Queue.new
      observer, arm = gated_observer(events, observed, resume) { |event| event.pending_calls == 1 }
      transport = ConnectRPC::Async::Transport.new(origin: origin, connection_observer: observer)
      typed = client(transport, origin)
      held = task.async { typed.echo(AsyncHelpers::MESSAGE.new(name: "held")) }
      entered.dequeue
      arm.call
      waiting = task.async { typed.echo(AsyncHelpers::MESSAGE.new(name: "waiting")) }
      expect(counts(observed.dequeue)).to eq([1, 0, 1])
      # Capacity is released and signalled while the waiter is still in its publication.
      release.enqueue(true)
      expect(held.wait.message.name).to eq("held")
      wait_until(task, "held lease released") { lease_usage(transport).zero? }
      resume.enqueue(true)
      wait_until(task, "waiter took the released capacity") { waiting.finished? }
      expect(waiting.wait.message.name).to eq("waiting")
      expect(counts(events.last)).to eq([1, 0, 0])
      expect(lease_usage(transport)).to eq(0)
    ensure
      2.times { resume&.enqueue(true) }
      2.times { release&.enqueue(true) }
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  it "holds no lease when an acquisition is cancelled during its pending-count decrement" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      host = ConnectRPC::Async::Host.new(server: gated_server(entered, release), max_requests: 1).start
      origin = "http://127.0.0.1:#{host.port}"
      events = []
      observed = Async::Queue.new
      resume = Async::Queue.new
      observer, arm = gated_observer(events, observed, resume) do |event|
        event.type == :counts_changed && event.pending_calls.zero?
      end
      transport = ConnectRPC::Async::Transport.new(origin: origin, connection_observer: observer)
      typed = client(transport, origin)
      manager = connection_manager(transport)
      held = task.async { typed.echo(AsyncHelpers::MESSAGE.new(name: "held")) }
      entered.dequeue
      connection = manager.connections.keys.first
      acquirer = task.async(finished: false) { manager.acquire }
      wait_until(task, "acquisition waits") { counts(events.last) == [1, 0, 1] }
      arm.call
      release.enqueue(true)
      expect(held.wait.message.name).to eq("held")
      expect(counts(observed.dequeue)).to eq([1, 0, 0])
      acquirer.cancel
      resume.enqueue(true)
      expect { acquirer.wait }.not_to raise_error
      expect(acquirer).to be_cancelled
      expect(manager.connections.to_a).to eq([[connection, 0]])
      expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "after")).message.name).to eq("after")
      expect(manager.connections.to_a).to eq([[connection, 0]])
      expect(counts(events.last)).to eq([1, 0, 0])
    ensure
      2.times { resume&.enqueue(true) }
      2.times { release&.enqueue(true) }
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end
end

RSpec.describe "ThreadedTransport connection count publication" do
  def counts(event) = [event.active, event.draining, event.pending_calls]

  def drain(queue)
    events = []
    events << queue.pop until queue.empty?
    events
  end

  it "ends at zero after a graceful close" do
    peer = threaded_host(server: server)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    events = Thread::Queue.new
    transport = threaded(origin: origin, connection_observer: ->(event) { events << event })
    expect(client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "one")).message.name).to eq("one")
    expect(transport.close(grace: 5)).to be(true)
    published = drain(events)
    expect(published.map { |event| counts(event) }).to include([1, 0, 0])
    expect(counts(published.last)).to eq([0, 0, 0])
  ensure
    transport&.close(grace: 0)
    peer&.stop
  end

  it "ends a failed owner's counts at zero and starts its replacement from zero" do
    entered = Thread::Queue.new
    release = Thread::Queue.new
    peer = threaded_host(server: server(echo: lambda { |message, _|
      entered << message.name
      release.pop if message.name == "pending"
      message
    }))
    origin = "http://127.0.0.1:#{peer.peer.port}"
    events = Thread::Queue.new
    transport = threaded(origin: origin, connection_observer: ->(event) { events << event })
    typed = client(transport, origin)
    pending = Thread.new { capture_error { typed.echo(AsyncHelpers::MESSAGE.new(name: "pending"), timeout: nil) } }
    pop_within(entered, "the pending call in the handler")
    failed = engine_of(transport)
    failed.run_on_owner(-> { raise "injected owner failure" })
    expect(value_within(pending, "the pending caller").code).to eq("unavailable")
    expect(failed.thread.join(5)).to equal(failed.thread)
    # The failed engine published everything before its thread ended.
    published = drain(events)
    expect(published.map { |event| counts(event) }).to include([1, 0, 0])
    expect(counts(published.last)).to eq([0, 0, 0])
    expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "restarted")).message.name).to eq("restarted")
    replacement = drain(events)
    expect(counts(replacement.first)).to eq([0, 0, 1])
    expect(counts(replacement.last)).to eq([1, 0, 0])
  ensure
    release&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end
end
