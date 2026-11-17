# frozen_string_literal: true

RSpec.describe "Falcon HTTP/1.1 lifecycle campaign" do
  def envelope(name)
    bytes = Soak::Topology::MESSAGE.encode(Soak::Topology::MESSAGE.new(name: name))
    [0, bytes.bytesize].pack("CN") + bytes
  end

  def connect_head(method, length, id, action, extra = "")
    "POST #{method.path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/connect+proto\r\n" \
      "Content-Length: #{length}\r\n#{Soak::Ledger::ID}: #{id}\r\n#{Soak::Topology::OP}: #{action}\r\n#{extra}\r\n"
  end

  def unread(socket)
    buffer = [0].pack("i")
    socket.ioctl(Soak::RawH2::FIONREAD, buffer)
    buffer.unpack1("i")
  end

  it "1 churn (HTTP/1.1): reuses an idle HTTP1Transport connection and retires TCP, TLS and Net::HTTP clients" do
    campaign("1-churn-http1/falcon", workload: "HTTP1Transport x4 shapes over TCP (persistent and per round) and " \
                                               "TLS, Net::HTTP unary from 4 threads", concurrency: 4,
                                     payload_sizes: [16, 16_384]) do |c|
      peer = c.peer_thread("falcon servers")
      tcp = c.server(peer, kind: :falcon, connections: -> { 1 })
      tls = c.server(peer, kind: :falcon, mode: :tls, connections: -> { 0 })
      persistent = c.http1(tcp.origin, label: "persistent http1", persistent: true, reuse: true)
      net = Soak::Topology::Clients.new(ConnectRPC::Transports::NetHTTP.new, tcp.origin)
      shapes = Soak::Builders::H1_SHAPES
      c.run do
        shapes.each { |shape| c.perform(persistent, shape) }
        c.perform(persistent, :unary, size: 16_384)
        { tcp => :tcp, tls => :tls }.each do |server, mode|
          round = c.http1(server.origin, mode: mode, label: "round http1 #{mode}")
          c.concurrently(*(shapes * 2).map { |shape| -> { c.perform(round, shape) } })
          expect(round.transport.close(grace: 2)).to be(true)
        end
        c.on_threads(*Array.new(4) { -> { c.perform(net, :unary) } })
      end
    end
  end

  it "5 slow peers (HTTP/1.1): withheld upload retires within the input bounds; a never-reading client is " \
     "bounded by close grace" do
    campaign("5-slow-peers-http1/falcon", workload: "early Connect reply to a withheld 100 KB upload; 20 MB " \
                                                    "server stream to a client that never reads",
                                          concurrency: 2, payload_sizes: [16, Soak::Topology::FLOOD_MESSAGE]) do |c|
      peer = c.peer_thread("falcon servers")
      server = c.server(peer, kind: :falcon, connections: -> { 1 })
      clients = c.http1(server.origin, label: "persistent http1", persistent: true)
      c.run do
        c.perform(clients, :unary)
        withheld = c.ledger.next_id("withheld-upload")
        wire = ConnectH1Wire.new(server.origin)
        c.track(:io, wire.raw, "withholding client socket")
        wire.write(connect_head(Soak::Topology::CS, 100_000, withheld, "first") + envelope("first"))
        status, = wire.head
        expect(status).to eq(200)
        wire.body
        eof = c.task.with_timeout(5) { wire.socket.read }
        expect(eof).to eq("")
        expect(Soak.poll("withheld upload handler end") { c.ledger.ended(withheld) }).to eq(:ok)
        wire.close
        Soak::Builders::H1_SHAPES.each { |shape| c.perform(clients, shape) }

        doomed = c.server(peer, kind: :falcon, persistent: false, label: "falcon non-reading target")
        stalled = c.ledger.next_id("never-reads")
        raw = IO::Endpoint.tcp("127.0.0.1", doomed.port).connect
        c.track(:io, raw, "never-reading client socket")
        body = envelope("x")
        # 20 MB exceeds what loopback socket buffers absorb, so the handler blocks.
        raw.write(connect_head(Soak::Topology::SS, body.bytesize, stalled, "flood",
                               "#{Soak::Topology::COUNT}: 100\r\n") + body)
        c.ledger.await_entered([stalled])
        Soak.poll("server output filling the never-reading client's socket") { unread(raw) >= 65_536 }
        sibling = c.http1(doomed.origin, label: "sibling http1")
        Soak::Builders::H1_SHAPES.each { |shape| c.perform(sibling, shape) }
        expect(sibling.transport.close(grace: 2)).to be(true)
        expect(doomed.close(grace: 0.3)).to be(false)
        expect(Soak.poll("never-reading stream's handler end") { c.ledger.ended(stalled) }).not_to eq(:ok)
        Soak.close_io(raw)
      end
    end
  end
end
