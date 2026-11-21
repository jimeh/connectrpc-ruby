# frozen_string_literal: true

RSpec.describe "Retries" do
  def requests(tag, from) = peer.since(from).grep(/\A#{tag} request /)
  def accepts(tag, from) = peer.count(/\A#{tag} accept /, from: from)

  # The peer allows one stream per connection. With stream 1 held, the first
  # queued call waits inside h2 for the stream slot, its stream id already
  # assigned, and hyper keeps the second in its request queue. A GOAWAY naming
  # stream 1 then ends the connection's admission: hyper hands the second back
  # unsent, and the first, whose stream id is above the last stream id and
  # whose HEADERS never reached the socket, is replayed too. The held stream's
  # response head shows the client has applied the peer's SETTINGS, which
  # precede it on the wire; until then h2 does not know the one-stream limit
  # and would send the queued streams at once. request:indexed shows hyper
  # passed the held request and the first queued one to h2, and
  # request:queued that the second reached hyper's queue, before the GOAWAY.
  # Each queued call succeeds, reaching the peer exactly once on one new
  # connection with its whole body: the peer answers only after the request
  # ends and reports the bytes it received.
  %i[grpc connect].each do |protocol|
    it "replays queued requests once on a new connection after a GOAWAY (#{protocol})" do
      from = peer.mark
      transport = hyper
      native_driver = driver(transport)
      points = %w[request:queued request:indexed retry:unsent retry:unprocessed]
      points.each { |point| native_driver.hold(point, 0) }
      origin = peer["raw_mcs1"]
      tag = "retry-unsent-#{protocol}"
      head = Queue.new
      held = Thread.new do
        request = ConnectRPC::StreamRequest.new(
          url: URI("#{origin}/raw.v1.Raw/Call"), http_method: "POST",
          headers: { "content-type" => ["application/grpc"], "te" => ["trailers"], "x-scenario" => ["hold:#{tag}"] }
        )
        transport.open_grpc_stream(request) do |exchange|
          exchange.write([0, 0].pack("CN"))
          exchange.close_send
          head << exchange.response_head(timeout: 20).status
          nil while exchange.read(64, timeout: 20)
          exchange.response_trailers
        end
      end
      expect(head.pop(timeout: 10)).to eq(200)
      queued = {}
      %w[q0 q1].each_with_index do |name, index|
        queued[name] = Thread.new do
          message = MSG.new(value: "payload-#{name}")
          core(origin, transport: transport, protocol: protocol)
            .call(HyperHelpers::RAW_UNARY, message, metadata: { "x-scenario" => "await-ok:#{name}" }, timeout: 20)
            .message
        rescue ConnectRPC::Error => e
          e
        end
        wait_until("#{name} handed to hyper") { native_driver.reached("request:queued") == index + 2 }
        # q0 reaches h2's stream-slot wait; q1 then stays in hyper's queue.
        wait_until("q0 passed to h2") { native_driver.reached("request:indexed") == 2 } if index.zero?
      end
      peer.command("raw-goaway rawmcs1")
      outcomes = queued.transform_values(&:value)
      context = -> { "outcomes: #{outcomes.inspect}; events: #{peer.since(from).last(12).inspect}" }
      expect(outcomes.values).to all(eq(MSG.new)), context
      expect(%w[retry:unsent retry:unprocessed].map { |point| native_driver.reached(point) }).to eq([1, 1]), context
      # Each sent exactly once, as a stream of one new connection.
      peer.wait_count(/\Arawmcs1 handled await-ok:/, 2, from: from)
      sent = requests("rawmcs1", from).grep(/request await-ok:/).map { |event| event.split.values_at(2, 4) }
      expect(sent.map(&:first).sort).to eq(%w[await-ok:q0 await-ok:q1])
      expect(sent.map(&:last)).to eq(%w[n=1 n=2])
      framing = protocol == :grpc ? 5 : 0
      bodies = peer.since(from).grep(/\Arawmcs1 handled await-ok:/).to_h do |event|
        [event.split[2].delete_prefix("await-ok:"), event[/bytes=(\d+)/, 1].to_i]
      end
      sizes = %w[q0 q1].to_h { |name| [name, MSG.encode(MSG.new(value: "payload-#{name}")).bytesize + framing] }
      expect(bodies).to eq(sizes)
      expect(accepts("rawmcs1", from)).to eq(2)
      peer.command("release #{tag}")
      expect(held.value).to include("grpc-status" => ["0"])
    ensure
      points.each { |point| native_driver&.release(point) }
    end
  end

  def streams(tag, from) = requests(tag, from).map { |event| event.split.values_at(2, 3, 4) }

  # The peer answers stream 1, leaves stream 3 unanswered, then sends a
  # GOAWAY naming stream 1: it never processed stream 3, so the call goes
  # once more to a new connection, where the peer handles it.
  it "replays a Connect unary stream above a GOAWAY's last stream id once, on a new connection" do
    from = peer.mark
    transport = hyper
    native_driver = driver(transport)
    native_driver.hold("retry:unprocessed", 0)
    origin = peer["raw_goaway"]
    key = "cutoff-#{rand(1 << 30)}"
    expect(raw_call(origin, "ok", transport: transport, protocol: :connect).message).to eq(MSG.new)
    call = Thread.new do
      raw_call(origin, "ignore-once:#{key}", transport: transport, protocol: :connect, timeout: 20).message
    end
    peer.wait_event(/\Arawgo ignored stream=3\z/, from: from)
    peer.command("raw-goaway rawgo 1")
    expect(call.value).to eq(MSG.new)
    expect(native_driver.reached("retry:unprocessed")).to eq(1)
    expect(streams("rawgo", from)).to eq([%w[ok stream=1 n=1], ["ignore-once:#{key}", "stream=3", "n=2"],
                                          ["ignore-once:#{key}", "stream=1", "n=1"]])
    expect(peer.count(/\Arawgo handled ignore-once:#{key} /, from: from)).to eq(1)
    expect(accepts("rawgo", from)).to eq(2)
  ensure
    native_driver&.release("retry:unprocessed")
  end

  # Streams 1 and 3 are in flight when the GOAWAY names stream 1 and the peer
  # then closes its side. Stream 3 is replayed; stream 1, which the peer may
  # have processed, fails and is never sent again.
  it "never replays a stream at or below a GOAWAY's last stream id" do
    from = peer.mark
    transport = hyper
    origin = peer["raw_goaway"]
    key = "below-#{rand(1 << 30)}"
    stalled = Thread.new do
      error_of { raw_call(origin, "stall", transport: transport, protocol: :connect, timeout: 20) }
    end
    peer.wait_event(/\Arawgo handler-start stream=1\z/, from: from)
    above = Thread.new do
      raw_call(origin, "ignore-once:#{key}", transport: transport, protocol: :connect, timeout: 20).message
    end
    peer.wait_event(/\Arawgo ignored stream=3\z/, from: from)
    peer.command("raw-goaway rawgo 1 close")
    expect(above.value).to eq(MSG.new)
    error = stalled.value
    expect([error.code, error.message]).to eq(["unavailable", "HTTP transport failed: connection closed"])
    expect(streams("rawgo", from)).to eq([%w[stall stream=1 n=1], ["ignore-once:#{key}", "stream=3", "n=2"],
                                          ["ignore-once:#{key}", "stream=1", "n=1"]])
  end

  # The gRPC stream's HEADERS reached the peer before the GOAWAY disowned
  # it. open_grpc_stream cannot tell unary from streaming, so only a request
  # that never reached the socket is replayed, even with its body unread.
  it "does not replay a gRPC stream whose HEADERS reached the socket, even above the last stream id" do
    from = peer.mark
    transport = hyper
    origin = peer["raw_goaway"]
    key = "sent-#{rand(1 << 30)}"
    request = ConnectRPC::StreamRequest.new(
      url: URI("#{origin}/raw.v1.Raw/Call"), http_method: "POST",
      headers: { "content-type" => ["application/grpc"], "te" => ["trailers"], "x-scenario" => ["ignore-once:#{key}"] }
    )
    call = Thread.new do
      error_of { transport.open_grpc_stream(request) { |exchange| exchange.response_head(timeout: 20) } }
    end
    peer.wait_event(/\Arawgo ignored stream=1\z/, from: from)
    peer.command("raw-goaway rawgo 0")
    expect(call.value&.code).to eq("unavailable")
    expect(streams("rawgo", from)).to eq([["ignore-once:#{key}", "stream=1", "n=1"]])
  end

  it "retries a Connect unary call refused with REFUSED_STREAM once, on a fresh connection" do
    from = peer.mark
    expect(raw_call(peer["raw"], "refuse-once:connect", transport: hyper, protocol: :connect).message).to eq(MSG.new)
    expect(requests("raw", from).map { |event| event[/n=\d+/] }).to eq(%w[n=1 n=1])
    expect(accepts("raw", from)).to eq(2)
  end

  # The pooled connection still admits when it refuses one stream, so the
  # retry dials a connection that serves only that call. The held sibling
  # finishes on the pooled connection, the retry's connection closes after
  # its call, and a later call reuses the pooled one as its third stream.
  it "retries a refused stream on a connection of its own and keeps the pooled connection for other calls" do
    from = peer.mark
    transport = hyper
    origin = peer["raw"]
    tag = "retry-keep-#{rand(1 << 30)}"
    held = Thread.new { raw_call(origin, "hold:#{tag}", transport: transport, timeout: 20).message }
    peer.wait_event(/\Araw request hold:#{tag} /, from: from)
    expect(raw_call(origin, "refuse-once:#{tag}", transport: transport, protocol: :connect).message).to eq(MSG.new)
    peer.command("release #{tag}")
    expect(held.value).to eq(MSG.new)
    expect(raw_call(origin, "ok", transport: transport, protocol: :connect).message).to eq(MSG.new)
    expect(requests("raw", from).map { |event| event.split.values_at(2, 4) })
      .to eq([["hold:#{tag}", "n=1"], ["refuse-once:#{tag}", "n=2"], ["refuse-once:#{tag}", "n=1"], %w[ok n=3]])
    pooled, retried = peer.wait_count(/\Araw accept /, 2, from: from).map { |event| Regexp.escape(event.split.last) }
    peer.wait_event(/\Araw conn-closed #{retried}\z/, from: from)
    expect([accepts("raw", from), peer.count(/\Araw conn-closed #{pooled}\z/, from: from)]).to eq([2, 0])
  end

  it "never retries twice" do
    from = peer.mark
    error = error_of { raw_call(peer["raw"], "refuse-always", transport: hyper, protocol: :connect) }
    expect([error.code, error.message]).to eq(["unavailable", "HTTP transport failed: stream reset"])
    expect(requests("raw", from).size).to eq(2)
  end

  it "does not replay a refused gRPC unary call, which open_grpc_stream cannot tell from a stream" do
    from = peer.mark
    error = error_of { raw_call(peer["raw"], "refuse-once:grpc", transport: hyper) }
    expect([error.code, error.message]).to eq(["unavailable", "HTTP/2 stream reset"])
    expect(requests("raw", from).size).to eq(1)
  end

  it "does not replay an ambiguous disconnect after the handler started" do
    from = peer.mark
    %i[connect grpc].each do |protocol|
      error = error_of { raw_call(peer["raw"], "disconnect", transport: hyper, protocol: protocol) }
      expect(error.code).to eq("unavailable")
    end
    expect(peer.since(from).grep(/raw handler-start/).size).to eq(2)
    expect(requests("raw", from).size).to eq(2)
  end
end
