# frozen_string_literal: true

RSpec.describe "Async client connection metrics" do
  def state(transport, name) = value(:grpc_client_connection_state, transport: transport, state: name)

  # Active, draining and pending calls.
  def gauges(transport)
    [value(:grpc_client_connections, transport: transport, state: "active"),
     value(:grpc_client_connections, transport: transport, state: "draining"),
     value(:grpc_client_pending_calls, transport: transport)]
  end

  def gated_server(entered, release)
    server(echo: lambda { |message, _|
      entered.enqueue(message.name)
      release.dequeue if message.name == "held"
      message
    })
  end

  def holding_peer(task, listener, held)
    ConnectionPeer::RecordingServer.new(task, listener) do |incoming|
      held.dequeue if incoming.path == "/long"
      incoming.send_response(Protocol::HTTP::Response[200, {}, [incoming.path]])
    rescue Protocol::HTTP2::Error, IOError
      nil
    end
  end

  it "records a refused attempt, its transient failure state and the state transitions" do
    reactor do |task|
      reserved = refusing_socket
      origin = "http://127.0.0.1:#{reserved.local_address.ip_port}"
      observer = metrics.connection_observer(transport: "refused")
      expect(state("refused", "idle")).to eq(1)
      transport = ConnectRPC::Async::Transport.new(origin: origin, connection_observer: observer,
                                                   connection_policy: ConnectRPC::Async::ConnectionPolicy.new)
      expect { transport.call(request("#{origin}/x", body: "abc")) }.to unavailable_error
      wait_until(task, "refused attempt") { total(:grpc_client_connection_attempts_total, transport: "refused") == 1 }
      expect(value(:grpc_client_connection_attempts_total, transport: "refused", outcome: "refused")).to eq(1)
      expect(value(:grpc_client_connection_attempt_seconds, transport: "refused").fetch("+Inf")).to eq(1)
      expect(value(:grpc_client_connection_state_changes_total, transport: "refused", state: "connecting")).to eq(1)
      expect(state("refused", "transient_failure")).to eq(1)
      expect(%w[idle connecting ready closing closed].map { |name| state("refused", name) }).to all(eq(0))
    ensure
      transport&.close(grace: 0)
      reserved&.close
    end
  end

  it "records a ready attempt against a local host and closing on transport close" do
    reactor do |task|
      host = ConnectRPC::Async::Host.new(server: server).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin,
                                                   connection_observer: metrics.connection_observer(transport: "local"))
      expect(client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "ready")).message.name).to eq("ready")
      expect(value(:grpc_client_connection_attempts_total, transport: "local", outcome: "ready")).to eq(1)
      expect(state("local", "ready")).to eq(1)
      transport.close(grace: 0)
      wait_until(task, "closed state") { state("local", "closed") == 1 }
      expect(%w[idle connecting ready transient_failure closing].map { |name| state("local", name) }).to all(eq(0))
    ensure
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  it "tracks active connections and waiting calls on a real host and zeros them on close" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      host = ConnectRPC::Async::Host.new(server: gated_server(entered, release), max_requests: 1).start
      origin = "http://127.0.0.1:#{host.port}"
      observer = metrics.connection_observer(transport: "gauges")
      expect(gauges("gauges")).to eq([0, 0, 0])
      transport = ConnectRPC::Async::Transport.new(origin: origin, connection_observer: observer)
      typed = client(transport, origin)
      held = task.async { typed.echo(AsyncHelpers::MESSAGE.new(name: "held")) }
      entered.dequeue
      expect(gauges("gauges")).to eq([1, 0, 0])
      waiting = task.async { typed.echo(AsyncHelpers::MESSAGE.new(name: "waiting")) }
      wait_until(task, "a call waits for the one stream") { gauges("gauges") == [1, 0, 1] }
      release.enqueue(true)
      expect([held.wait, waiting.wait].map { |response| response.message.name }).to eq(%w[held waiting])
      expect(gauges("gauges")).to eq([1, 0, 0])
      expect(transport.close(grace: 1)).to be(true)
      expect(gauges("gauges")).to eq([0, 0, 0])
    ensure
      2.times { release&.enqueue(true) }
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  it "counts a GOAWAY connection as draining until its held call ends" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      held = Async::Queue.new
      peer = holding_peer(task, bound.sockets.first, held)
      origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin,
                                                   connection_observer: metrics.connection_observer(transport: "drain"))
      long = task.async { transport.call(request("#{origin}/long")).body }
      incoming = peer.requests.dequeue
      expect(gauges("drain")).to eq([1, 0, 0])
      frame = Protocol::HTTP2::GoawayFrame.new
      frame.pack(incoming.stream.id, Protocol::HTTP2::Error::NO_ERROR, "")
      peer.connections.first.write_frame(frame)
      wait_until(task, "connection draining") { gauges("drain") == [0, 1, 0] }
      held.enqueue(true)
      expect(long.wait).to eq("/long")
      wait_until(task, "drained connection removed") { gauges("drain") == [0, 0, 0] }
    ensure
      held&.enqueue(true)
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  it "zeros active connections when the peer aborts the connection" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      peer = holding_peer(task, bound.sockets.first, Async::Queue.new)
      origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin,
                                                   connection_observer: metrics.connection_observer(transport: "abort"))
      expect(transport.call(request("#{origin}/warm")).body).to eq("/warm")
      expect(gauges("abort")).to eq([1, 0, 0])
      AsyncHelpers::INTERNAL.raw_close(peer.raws.first)
      wait_until(task, "aborted connection removed") { gauges("abort") == [0, 0, 0] }
      expect(state("abort", "idle")).to eq(1)
    ensure
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  it "counts admission rejections" do
    reactor do
      reserved = refusing_socket
      origin = "http://127.0.0.1:#{reserved.local_address.ip_port}"
      policy = ConnectRPC::Async::ConnectionPolicy.new(max_pending_bytes: 4)
      observer = metrics.connection_observer(transport: "budget")
      transport = ConnectRPC::Async::Transport.new(origin: origin, connection_policy: policy,
                                                   connection_observer: observer)
      expect { transport.call(request("#{origin}/x", body: "too large")) }.to unavailable_error
      expect(value(:grpc_client_admission_rejected_total, transport: "budget")).to eq(1)
      expect(total(:grpc_client_connection_attempts_total, transport: "budget")).to eq(0)
    ensure
      transport&.close(grace: 0)
      reserved&.close
    end
  end
end
