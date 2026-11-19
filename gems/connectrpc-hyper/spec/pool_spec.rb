# frozen_string_literal: true

require_relative "support/socket_proxy"

RSpec.describe "Connection pooling" do
  include SocketProxy

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

  # Elapsed idle time is the behaviour under test, so these wait for clock
  # points rather than events: a reuse at 0.7 s, a check at 1.25 s (past a
  # stale timer from the first park), and expiry no earlier than one full
  # idle_timeout after the reuse began.
  def idle_restart(transport, open:)
    peer_client = yield
    started = monotonic
    peer_client.call
    wait_until("the reuse point") { monotonic >= started + 0.7 }
    reused = monotonic
    peer_client.call
    wait_until("the stale expiry point") { monotonic >= started + 1.25 }
    expect([stats(transport).first, open.call]).to eq([1, true])
    wait_until("idle expiry", timeout: 10) { !open.call }
    expect(monotonic - reused).to be >= 1.0
  end

  it "restarts an HTTP/2 connection's idle period on reuse and closes it after a full idle_timeout" do
    from = peer.mark
    transport = hyper(idle_timeout: 1.0)
    peer_client = client(peer["connect_h2c"], transport: transport, protocol: :connect)
    open = -> { peer.count(/\Aconnect conn-closed /, from: from).zero? }
    idle_restart(transport, open: open) { -> { peer_client.unary(MSG.new(value: "x"), timeout: 5) } }
    expect(accepts("connect", from)).to eq(1)
  end

  # One attempt for the leader and its waiters: the full-backlog listener
  # drops SYNs, so each attempt runs until connect_timeout.
  describe "concurrent callers while a dial fails", :linux_only do
    def dial_storm(transport, port, callers: 4)
      started = monotonic
      threads = Array.new(callers) do
        Thread.new do
          client("http://127.0.0.1:#{port}", transport: transport, protocol: :connect).unary(MSG.new, timeout: 10)
        rescue ConnectRPC::Error => e
          [e.message, e.cause&.kind, monotonic - started]
        end
      end
      threads.each { |thread| wait_parked(thread, "Driver#unary") }
      [threads, started]
    end

    { true => "shares one HTTP/2 dial attempt and its failure",
      false => "dials HTTP/1.1 connections in parallel" }.each do |http2, behaviour|
      it behaviour do
        port, cleanup = full_backlog
        baseline = syn_sent(port)
        transport = hyper(http2: http2, connect_timeout: 0.5)
        threads, = dial_storm(transport, port)
        expected = http2 ? 1 : 4
        wait_until("#{expected} connect attempts") { syn_sent(port) == baseline + expected }
        outcomes = threads.map(&:value)
        expect(outcomes.map { |message, kind, _| [message, kind] })
          .to eq(Array.new(4) { ["HTTP transport failed: timed out", :connect_timeout] })
        # Serialised dials would fail at 0.5, 1.0, 1.5 and 2.0 s.
        expect(outcomes.map(&:last)).to all(be_between(0.45, 0.95))
        expect(syn_sent(port)).to eq(baseline)
      ensure
        cleanup&.call
      end
    end

    it "lets a waiter dial again when the leading caller leaves before its dial ends" do
      port, cleanup = full_backlog
      transport = hyper(connect_timeout: 1.0)
      started = monotonic
      calls = [0.3, 10].map do |timeout|
        thread = Thread.new do
          client("http://127.0.0.1:#{port}", transport: transport, protocol: :connect).unary(MSG.new, timeout: timeout)
        rescue ConnectRPC::Error => e
          [e.code, e.cause&.kind, monotonic - started]
        end
        wait_parked(thread, "Driver#unary")
        thread
      end
      leader, waiter = calls.map(&:value)
      expect(leader.first(2)).to eq(["deadline_exceeded", nil])
      # A new attempt started when the leader left at 0.3 s.
      expect(waiter.first(2)).to eq(["unavailable", :connect_timeout])
      expect(waiter.last).to be_between(1.25, 3)
    ensure
      cleanup&.call
    end
  end

  it "lets waiters behind a dial that negotiated HTTP/1.1 dial their own connections" do
    from = peer.mark
    gate = Queue.new
    port = URI(peer["connect_tls_h1"]).port
    with_gated_tunnels(port, gate) do |url, heads|
      transport = hyper(proxy: url, **tls_options)
      peer_client = client("https://127.0.0.1:#{port}", transport: transport, protocol: :connect)
      calls = Array.new(4) do |index|
        thread = Thread.new { peer_client.unary(MSG.new(value: "h1-#{index}"), timeout: 20).message.value }
        heads.pop(timeout: 10) if index.zero?
        wait_parked(thread, "Driver#unary")
        thread
      end
      # The waiters joined the held attempt instead of dialing.
      expect(heads.size).to eq(0)
      gate << true
      expect(calls.map(&:value)).to eq(%w[h1-0 h1-1 h1-2 h1-3])
      # Each waiter dialed its own tunnel, unless it found the leader's
      # connection already parked again.
      expect(heads.size).to be_between(1, 3)
      expect(peer.since(from).grep(/connecth1 accept/).size).to eq(heads.size + 1)
      transport.close(grace: 1)
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

    it "keeps one idle timer per idle connection however often it is parked" do
      server = TinyHTTP.new
      transport = hyper(http2: false)
      peer_client = client(server.origin, transport: transport, protocol: :connect)
      200.times { peer_client.unary(MSG.new, timeout: 5) }
      # The connection's own task and its one idle timer.
      expect(stats(transport)).to eq([1, 0, 2, 1])
      expect(server.count(:accept)).to eq(1)
    ensure
      server&.close
    end

    it "restarts an idle connection's idle period on reuse and closes it after a full idle_timeout" do
      server = TinyHTTP.new
      transport = hyper(http2: false, idle_timeout: 1.0)
      peer_client = client(server.origin, transport: transport, protocol: :connect)
      open = -> { stats(transport).last == 1 }
      idle_restart(transport, open: open) { -> { peer_client.unary(MSG.new, timeout: 5) } }
      server.wait_for(:closed, 1)
      expect(server.count(:accept)).to eq(1)
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
