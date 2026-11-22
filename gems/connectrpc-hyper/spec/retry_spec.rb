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
  # passed the held request and the first queued one to h2 (it fires in the
  # dispatch poll that hands q0 to h2's send_request, with no await between,
  # so a GOAWAY overtakes it only if that thread stalls there; q0 then never
  # gets a stream id, and the outcome is the same), and
  # request:queued that the second reached hyper's queue, before the GOAWAY.
  # Each queued call succeeds, reaching the peer exactly once on one new
  # connection with its whole body: the peer answers only after the request
  # ends and reports the bytes it received. Once the held stream ends, h2
  # closes the old connection; the queued request whose stream h2 failed never
  # reaches it, because h2 dropped that stream's queued HEADERS.
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
      old = Regexp.escape(peer.since(from).grep(/\Arawmcs1 accept /).first.split.last)
      peer.wait_event(/\Arawmcs1 conn-closed #{old}\z/, from: from)
      expect(requests("rawmcs1", from).grep(/request await-ok:/).size).to eq(2)
    ensure
      points&.each { |point| native_driver&.release(point) }
    end
  end

  # The GOAWAY retry derives each request's stream id from the order hyper's
  # dispatch passes requests to h2 (wire.rs): one index per request, taken in
  # the body's size_hint right before h2's send_request, and none for a request
  # whose caller left while it waited in hyper's queue. With one stream slot
  # held, q0 waits in h2 and q1 in hyper's queue; q1's caller cancels. Once the
  # slot frees, h2 opens q0 and hyper skips q1, so q2 gets the next stream.
  # request:stream:N counts each derived id; the peer reports the real ones. A
  # hyper or h2 release that changes this dispatch breaks this spec.
  it "derives each request's stream id from hyper's dispatch order" do
    from = peer.mark
    transport = hyper
    native_driver = driver(transport)
    points = %w[request:queued request:indexed] + [1, 3, 5, 7].map { |id| "request:stream:#{id}" }
    points.each { |point| native_driver.hold(point, 0) }
    origin = peer["raw_mcs1"]
    tag = "stream-ids-#{rand(1 << 30)}"
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
      end
    end
    expect(head.pop(timeout: 10)).to eq(200)
    call = lambda do |name, **options|
      raw_call(origin, "await-ok:#{name}", transport: transport, protocol: :connect, timeout: 20, **options)
    end
    q0 = Thread.new { call.call("q0").message }
    wait_until("q0 passed to h2") { native_driver.reached("request:indexed") == 2 }
    source = ConnectRPC::CancellationSource.new
    q1 = Thread.new { error_of { call.call("q1", cancellation: source.token) } }
    wait_until("q1 queued in hyper") { native_driver.reached("request:queued") == 3 }
    source.cancel
    expect(q1.value&.code).to eq("canceled")
    peer.command("release #{tag}")
    expect(q0.value).to eq(MSG.new)
    expect(call.call("q2").message).to eq(MSG.new)
    held.value
    expect(native_driver.reached("request:indexed")).to eq(3)
    expect([1, 3, 5, 7].map { |id| native_driver.reached("request:stream:#{id}") }).to eq([1, 1, 1, 0])
    expect(streams("rawmcs1", from).map { |scenario, stream, _| [scenario, stream] })
      .to eq([["hold:#{tag}", "stream=1"], %w[await-ok:q0 stream=3], %w[await-ok:q2 stream=5]])
    expect(accepts("rawmcs1", from)).to eq(1)
  ensure
    points&.each { |point| native_driver&.release(point) }
  end

  # As above, a GOAWAY leaves a queued gRPC call unsent and the retry recovers
  # its unread body, held at retry:unprocessed. The writer catches the
  # cancellation inside the exchange, which stays open. The retry task ends
  # while still held, so it never dispatches, and the peer never sees the call.
  it "does not dispatch a GOAWAY retry for a call canceled while the retry waits" do
    from = peer.mark
    transport = hyper
    native_driver = driver(transport)
    native_driver.hold("request:indexed", 0)
    native_driver.hold("retry:unprocessed", 20)
    origin = peer["raw_mcs1"]
    tag = "retry-cancel-#{rand(1 << 30)}"
    grpc = { "content-type" => ["application/grpc"], "te" => ["trailers"] }
    head = Queue.new
    held = Thread.new do
      request = ConnectRPC::StreamRequest.new(url: URI("#{origin}/raw.v1.Raw/Call"), http_method: "POST",
                                              headers: grpc.merge("x-scenario" => ["hold:#{tag}"]))
      transport.open_grpc_stream(request) do |exchange|
        exchange.write([0, 0].pack("CN"))
        exchange.close_send
        head << exchange.response_head(timeout: 20).status
        nil while exchange.read(64, timeout: 20)
      end
    end
    expect(head.pop(timeout: 10)).to eq(200)
    source = ConnectRPC::CancellationSource.new
    caught = Queue.new
    leave = Queue.new
    call = Thread.new do
      request = ConnectRPC::StreamRequest.new(url: URI("#{origin}/raw.v1.Raw/Call"), http_method: "POST",
                                              headers: grpc.merge("x-scenario" => ["await-ok:#{tag}"]),
                                              cancellation: source.token)
      error_of do
        transport.open_grpc_stream(request) do |exchange|
          exchange.write([0, 0].pack("CN"))
        rescue ConnectRPC::Error => e
          caught << e.code
          leave.pop(timeout: 20)
        end
      end
    end
    wait_until("the queued call passed to h2") { native_driver.reached("request:indexed") == 2 }
    peer.command("raw-goaway rawmcs1")
    wait_until("the retry held") { native_driver.reached("retry:unprocessed") == 1 }
    tasks = stats(transport)[2]
    source.cancel
    expect(caught.pop(timeout: 10)).to eq("canceled")
    wait_until("the retry task ended") { stats(transport)[2] < tasks }
    native_driver.release("retry:unprocessed")
    leave << true
    expect(call.value&.code).to eq("canceled")
    peer.command("release #{tag}")
    held.value
    expect(accepts("rawmcs1", from)).to eq(1)
    expect(requests("rawmcs1", from).grep(/await-ok:/)).to be_empty
  ensure
    %w[request:indexed retry:unprocessed].each { |point| native_driver&.release(point) }
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

  # Socket writes are held back after stream 1, as a full send buffer would
  # hold them. h2 encodes the gRPC call's HEADERS and offers them to the
  # socket, which takes nothing (wire:headers-queued counts that offer). A
  # GOAWAY naming stream 1 then disowns the call, whose body is still unread.
  # Its HEADERS may yet reach the socket, so it is not replayed. This depends
  # on h2 offering every encoded frame on its next write (wire.rs); a release
  # that buffers HEADERS unseen breaks this spec.
  it "does not replay a gRPC stream whose HEADERS are still waiting for the socket" do
    from = peer.mark
    transport = hyper
    native_driver = driver(transport)
    origin = peer["raw_goaway"]
    expect(raw_call(origin, "ok", transport: transport, protocol: :connect).message).to eq(MSG.new)
    %w[wire:headers-queued retry:unprocessed].each { |point| native_driver.hold(point, 0) }
    native_driver.hold("socket:write", 20)
    request = ConnectRPC::StreamRequest.new(
      url: URI("#{origin}/raw.v1.Raw/Call"), http_method: "POST",
      headers: { "content-type" => ["application/grpc"], "te" => ["trailers"], "x-scenario" => ["ok"] }
    )
    call = Thread.new do
      error_of { transport.open_grpc_stream(request) { |exchange| exchange.response_head(timeout: 20) } }
    end
    wait_until("the HEADERS offered and held back") { native_driver.reached("wire:headers-queued") >= 1 }
    peer.command("raw-goaway rawgo 1")
    expect(call.value&.code).to eq("unavailable")
    expect(native_driver.reached("retry:unprocessed")).to eq(0)
    expect(accepts("rawgo", from)).to eq(1)
  ensure
    %w[socket:write wire:headers-queued retry:unprocessed].each { |point| native_driver&.release(point) }
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
