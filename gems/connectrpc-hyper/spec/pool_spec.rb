# frozen_string_literal: true

RSpec.describe "Connection pooling" do
  def accepts(tag, from) = peer.count(/\A#{tag} accept /, from: from)

  it "shares one dial and one HTTP/2 connection among concurrent first callers" do
    from = peer.mark
    # TLS keeps the first dial in flight long enough for the others to queue.
    transport = hyper(**tls_options)
    peer_client = client(peer["grpc_tls"], transport: transport)
    start = Queue.new
    threads = Array.new(16) do |index|
      Thread.new do
        start.pop
        peer_client.unary(MSG.new(value: "t#{index}"), timeout: 10).message.value
      end
    end
    16.times { start << true }
    expect(threads.map(&:value)).to eq(Array.new(16) { |index| "t#{index}" })
    expect(accepts("grpc", from)).to eq(1)
    expect(stats(transport).first).to eq(1)
  end

  it "opens one connection per call with connections: :per_call" do
    from = peer.mark
    peer_client = client(peer["grpc_h2c"], transport: hyper(connections: :per_call))
    5.times { |index| peer_client.unary(MSG.new(value: index.to_s), timeout: 5) }
    expect(accepts("grpc", from)).to eq(5)
    peer.wait_count(/\Agrpc conn-closed /, 5, from: from)
  end

  it "closes an idle pooled connection after idle_timeout and reconnects on the next call" do
    from = peer.mark
    peer_client = client(peer["grpc_h2c"], transport: hyper(idle_timeout: 0.2))
    peer_client.unary(MSG.new(value: "a"), timeout: 5)
    peer.wait_event(/\Agrpc conn-closed /, from: from)
    expect(peer_client.unary(MSG.new(value: "b"), timeout: 5).message.value).to eq("b")
    expect(accepts("grpc", from)).to eq(2)
  end

  it "never fails a call when idle eviction races its checkout" do
    from = peer.mark
    peer_client = client(peer["connect_h2c"], transport: hyper(idle_timeout: 0.001), protocol: :connect)
    calls = 0
    # Jittered gaps around the 1 ms idle timeout make the reaper race
    # checkouts; the loop ends once several evictions happened.
    until accepts("connect", from) >= 10
      expect(peer_client.unary(MSG.new(value: calls.to_s), timeout: 5).message.value).to eq(calls.to_s)
      calls += 1
      raise "no eviction raced #{calls} calls" if calls > 5000

      sleep(rand * 0.002)
    end
  end

  describe "HTTP/1.1" do
    it "keeps at most two clean idle connections per origin and reuses them" do
      arrived = Queue.new
      go = Queue.new
      server = TinyHTTP.new do |_request, socket|
        arrived << true
        go.pop
        TinyHTTP.ok(socket)
      end
      transport = hyper(http2: false)
      peer_client = client(server.origin, transport: transport, protocol: :connect)
      calls = Array.new(3) { Thread.new { peer_client.unary(MSG.new, timeout: 10) } }
      3.times { arrived.pop }
      3.times { go << true }
      calls.each(&:join)
      server.wait_for(:closed, 1)
      wait_until("two idle connections") { stats(transport).last == 2 }
      Thread.new do
        2.times do
          arrived.pop
          go << true
        end
      end
      2.times { peer_client.unary(MSG.new, timeout: 10) }
      expect(server.count(:accept)).to eq(3)
      expect(server.count(:request)).to eq(5)
    ensure
      server&.close
    end

    it "never returns the socket of an interrupted upload" do
      from = peer.mark
      transport = hyper(http2: false)
      peer_client = client(peer["connect_h2c"], transport: transport, protocol: :connect)
      expect(peer_client.unary(MSG.new(value: "warm"), timeout: 5).message.value).to eq("warm")
      expect do
        peer_client.client_stream(metadata: { "x-mode" => "gate:upload" }, timeout: 5) do |writer|
          writer.send_message(MSG.new(value: "a"))
          raise IOError, "interrupted upload"
        end
      end.to raise_error(IOError, "interrupted upload")
      peer.command("release upload")
      peer.wait_event(/\Aconnect conn-closed /, from: from)
      expect(peer_client.unary(MSG.new(value: "next"), timeout: 5).message.value).to eq("next")
      expect(peer.since(from).grep(/connect cs-eof/)).to be_empty
      expect(accepts("connect", from)).to eq(2)
    end

    it "never returns the socket of an incomplete response" do
      server = TinyHTTP.new do |request, socket|
        if request.path.end_with?("Unary") && server.count(:request) == 1
          socket.write("HTTP/1.1 200 OK\r\ncontent-type: application/proto\r\ncontent-length: 10\r\n\r\nabc")
          # Holds the response incomplete until the client closes the socket.
          socket.read
          next
        end
        TinyHTTP.ok(socket)
      end
      transport = hyper(http2: false)
      peer_client = client(server.origin, transport: transport, protocol: :connect)
      error = error_of { peer_client.unary(MSG.new, timeout: 0.3) }
      expect(error.code).to eq("deadline_exceeded")
      server.wait_for(:closed, 1)
      peer_client.unary(MSG.new, timeout: 5)
      expect(server.count(:accept)).to eq(2)
    ensure
      server&.close
    end
  end
end
