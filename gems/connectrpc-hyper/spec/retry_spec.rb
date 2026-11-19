# frozen_string_literal: true

RSpec.describe "Retries" do
  def requests(tag, from) = peer.since(from).grep(/\A#{tag} request /)
  def accepts(tag, from) = peer.count(/\A#{tag} accept /, from: from)

  # The peer allows one stream per connection. With stream 1 held, a queued
  # call either waits inside hyper for the stream slot or stays in hyper's
  # request queue; a GOAWAY then ends the connection's admission. hyper hands
  # a request still in its queue back unsent, and the transport sends it once
  # more on a new connection; it reports the stream-slot waiter as failed.
  # Whether hyper's connection task moved the first queued request into the
  # slot wait before the GOAWAY arrived is its own scheduling, which nothing
  # outside hyper observes, so either call or both may be handed back. The
  # The held stream's response head shows the client has applied the peer's
  # SETTINGS, which precede it on the wire; until then h2 does not know the
  # one-stream limit and would send the queued streams at once. The
  # request:queued point shows every request reached hyper before the GOAWAY,
  # and retry:unsent counts the ones it actually handed back: each of those,
  # and no other call, succeeds, reaching the peer exactly once on one new
  # connection.
  %i[grpc connect].each do |protocol|
    it "sends a request hyper hands back unsent once more on a new connection (#{protocol})" do
      from = peer.mark
      transport = hyper
      native_driver = driver(transport)
      %w[request:queued retry:unsent].each { |point| native_driver.hold(point, 0) }
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
      queued = %w[q0 q1].to_h do |name|
        [name, Thread.new do
          raw_call(origin, "ok:#{name}", transport: transport, protocol: protocol, timeout: 20).message
        rescue ConnectRPC::Error => e
          e
        end]
      end
      # The held request and both queued ones; retries come only after GOAWAY.
      wait_until("every request handed to hyper") { native_driver.reached("request:queued") == 3 }
      peer.command("raw-goaway rawmcs1")
      outcomes = queued.transform_values(&:value)
      handed_back = native_driver.reached("retry:unsent")
      succeeded = outcomes.select { |_, outcome| outcome.is_a?(MSG) }.keys
      failed = outcomes.except(*succeeded).values
      context = -> { "outcomes: #{outcomes.inspect}; events: #{peer.since(from).last(12).inspect}" }
      expect(handed_back).to be_between(1, 2), context
      expect(succeeded.size).to eq(handed_back), context
      expect(failed.map { |error| [error.code, error.message] })
        .to eq(Array.new(failed.size, ["unavailable", "HTTP transport failed: connection closed"]))
      # Each success sent exactly once, as a stream of one new connection.
      peer.wait_count(/\Arawmcs1 request ok:/, succeeded.size, from: from)
      sent = requests("rawmcs1", from).grep(/request ok:/).map { |event| event.split.values_at(2, 4) }
      expect(sent.map(&:first).sort).to eq(succeeded.map { |name| "ok:#{name}" }.sort)
      expect(sent.map(&:last)).to eq(Array.new(succeeded.size) { |index| "n=#{index + 1}" })
      expect(accepts("rawmcs1", from)).to eq(2)
      peer.command("release #{tag}")
      expect(held.value).to include("grpc-status" => ["0"])
    ensure
      %w[request:queued retry:unsent].each { |point| native_driver&.release(point) }
    end
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
