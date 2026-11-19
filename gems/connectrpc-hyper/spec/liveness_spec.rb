# frozen_string_literal: true

RSpec.describe "Liveness and restart" do
  def envelope(message) = [0, message.bytesize].pack("CN") + message

  def accept_address(tag, from)
    peer.wait_event(/\A#{tag} accept /, from: from).split.last
  end

  it "answers server PINGs on an idle pooled connection and reuses it" do
    from = peer.mark
    peer_client = client(peer["grpc_keepalive"], transport: hyper)
    peer_client.unary(MSG.new(value: "a"), timeout: 5)
    idle = peer.mark
    # Each inbound read on the idle connection is the client's PING ACK.
    peer.wait_count(/\Aka read /, 5, from: idle)
    expect(peer.count(/\Aka conn-closed/, from: from)).to eq(0)
    expect(peer_client.unary(MSG.new(value: "b"), timeout: 5).message.value).to eq("b")
    expect(peer.count(/\Aka accept/, from: from)).to eq(1)
  end

  it "reconnects after an idle GOAWAY" do
    from = peer.mark
    peer_client = client(peer["grpc_idle"], transport: hyper)
    peer_client.unary(MSG.new(value: "a"), timeout: 5)
    peer.wait_closed("idle", from: from)
    expect(peer_client.unary(MSG.new(value: "b"), timeout: 5).message.value).to eq("b")
    expect(peer.count(/\Aidle accept/, from: from)).to eq(2)
  end

  it "finishes an active stream on a draining connection while new calls use a new one" do
    from = peer.mark
    transport = hyper
    peer_client = client(peer["grpc_age"], transport: transport)
    received = Queue.new
    stream = Thread.new do
      peer_client.server_stream(MSG.new(value: "gate:age"), timeout: 30) do |reader|
        reader.each { |message| received << message.value }
      end
      :completed
    end
    expect(received.pop(timeout: 10)).to eq("m0")
    # The peer sends GOAWAY 300 ms after accept; calls keep succeeding and
    # move to a new connection once the client processed it.
    calls = 0
    until peer.count(/\Aage accept/, from: from) >= 2
      expect(peer_client.unary(MSG.new(value: calls.to_s), timeout: 5).message.value).to eq(calls.to_s)
      calls += 1
      raise "no second connection after #{calls} calls" if calls > 2000
    end
    peer.command("release age")
    expect(stream.value).to eq(:completed)
    expect(received.pop(timeout: 5)).to eq("m1")
  end

  it "reconnects after the server is killed and restarted on the same port" do
    from = peer.mark
    peer_client = client(peer["grpc_restart"], transport: hyper)
    expect(peer_client.unary(MSG.new(value: "before"), timeout: 5).message.value).to eq("before")
    peer.command("kill")
    expect(peer_client.unary(MSG.new(value: "after"), timeout: 5).message.value).to eq("after")
    expect(peer.count(/\Arestart accept/, from: from)).to eq(2)
  end

  it "reconnects after a graceful restart on the same port" do
    from = peer.mark
    peer_client = client(peer["grpc_restart"], transport: hyper)
    peer_client.unary(MSG.new(value: "before"), timeout: 5)
    peer.command("graceful")
    expect(peer_client.unary(MSG.new(value: "after"), timeout: 5).message.value).to eq("after")
    expect(peer.count(/\Arestart accept/, from: from)).to eq(2)
  end

  describe "keepalive" do
    it "retires a connection whose PING ACK is withheld and fails its in-flight call unavailable" do
      from = peer.mark
      transport = hyper(keepalive: ConnectRPC::Keepalive.new(time: 0.2, timeout: 0.2))
      started = monotonic
      error = error_of do
        core(peer["raw_noack"], transport: transport)
          .server_stream(HyperHelpers::RAW_SERVER_STREAM, MSG.new, metadata: { "x-scenario" => "headers-only" },
                                                                   timeout: 20) { |reader| values(reader) }
      end
      expect(error.code).to eq("unavailable")
      expect(monotonic - started).to be < 5
      peer.wait_event(/\Arawnoack ping-withheld /, from: from)
      peer.wait_closed("rawnoack", from: from)
    end

    it "probes an idle connection only with permit_without_calls" do
      from = peer.mark
      keepalive = ->(permit) { ConnectRPC::Keepalive.new(time: 0.05, timeout: 5, permit_without_calls: permit) }
      # Connect responses without DATA: no BDP PING follows either call.
      raw_call(peer["raw"], "ok", transport: hyper(keepalive: keepalive.call(true)), protocol: :connect)
      permitted = accept_address("raw", from)
      second = peer.mark
      raw_call(peer["raw"], "ok", transport: hyper(keepalive: keepalive.call(false)), protocol: :connect)
      quiet = accept_address("raw", second)
      # The permitted connection's PINGs are the clock: three intervals passed.
      peer.wait_count(/\Araw ping #{Regexp.escape(permitted)}\z/, 3, from: peer.mark)
      expect(peer.count(/\Araw ping #{Regexp.escape(quiet)}\z/, from: from)).to eq(0)
    end

    it "does not count buffered, unread DATA as liveness" do
      from = peer.mark
      transport = hyper(keepalive: ConnectRPC::Keepalive.new(time: 0.2, timeout: 0.2))
      request = ConnectRPC::StreamRequest.new(
        url: URI("#{peer['raw_noack']}/raw.v1.Raw/Stream"), http_method: "POST",
        headers: { "content-type" => ["application/grpc"], "te" => ["trailers"], "x-scenario" => ["silent-data"] }
      )
      error = error_of do
        transport.open_grpc_stream(request) do |exchange|
          exchange.write(envelope(""))
          exchange.close_send
          exchange.response_head(timeout: 5)
          # The peer's DATA frame is buffered and never read while it goes silent.
          peer.wait_closed("rawnoack", from: from)
          exchange.read(64, timeout: 5)
          exchange.read(64, timeout: 5)
        end
      end
      expect(error.code).to eq("unavailable")
      peer.wait_event(/\Arawnoack ping-withheld /, from: from)
    end

    it "keeps a busy connection alive while BDP pings share the outstanding PING" do
      from = peer.mark
      transport = hyper(keepalive: ConnectRPC::Keepalive.new(time: 0.05, timeout: 5))
      count = 0
      client(peer["grpc_h2c"], transport: transport)
        .server_stream(MSG.new(value: "count:2000:4096"), timeout: 30) { |reader| reader.each { count += 1 } }
      expect(count).to eq(2000)
      expect(peer.count(/\Agrpc accept/, from: from)).to eq(1)
      address = Regexp.escape(accept_address("grpc", from))
      expect(peer.count(/\Agrpc conn-closed #{address}\z/, from: from)).to eq(0)
    end
  end
end
