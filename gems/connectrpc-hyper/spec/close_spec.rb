# frozen_string_literal: true

require_relative "support/socket_proxy"

RSpec.describe "Transport#close" do
  include SocketProxy

  def closed_error(transport, origin = peer["grpc_h2c"])
    error_of { client(origin, transport: transport).unary(MSG.new, timeout: 5) }
  end

  # A call that forced teardown ends fails with the closed phrase if its own
  # setup saw closing first, else the shutdown phrase; which is not fixed.
  def forced_phrase = eq("HTTP transport admission is closed").or(eq("HTTP transport failed: connection closed"))

  # A CONNECT proxy that holds the tunnel, and with it the caller's dial,
  # until `gate` yields, then relays to the h2c peer. `arrived` receives the
  # CONNECT request.
  def gated_relay(arrived, gate)
    lambda do |downstream|
      arrived << read_head(downstream)
      gate.pop
      upstream = TCPSocket.new("127.0.0.1", URI(peer["grpc_h2c"]).port)
      downstream.write("HTTP/1.1 200 Connection established\r\n\r\n")
      pipes = [[downstream, upstream], [upstream, downstream]].map do |from, to|
        Thread.new do
          IO.copy_stream(from, to)
        rescue IOError, SystemCallError
          nil
        end
      end
      pipes.first.join(10)
    ensure
      upstream&.close
    end
  end

  # A per-call dial is the call's own: close lets it finish within the
  # grace. A pooled HTTP/2 call waits on a pool dial instead, which close
  # ends (below).
  it "lets a per-call call admitted before close finish its dial and call, then returns true" do
    arrived = Queue.new
    gate = Queue.new
    with_socket_proxy(gated_relay(arrived, gate)) do |url|
      transport = hyper(connections: :per_call, proxy: url)
      peer_client = client("http://127.0.0.1:#{URI(peer['grpc_h2c']).port}", transport: transport)
      dialing = Thread.new do
        peer_client.unary(MSG.new(value: "admitted"), timeout: 20).message.value
      rescue ConnectRPC::Error => e
        [e.code, e.message]
      end
      expect(arrived.pop(timeout: 10)).to start_with("CONNECT ")
      # gRPC unary dials in its first exchange write.
      wait_parked(dialing, "Exchange#write")
      closer = Thread.new { transport.close(grace: 10) }
      # Parked in native close: admission is closed while the dial is held.
      wait_parked(closer, "Driver#close")
      gate << true
      expect(dialing.value).to eq("admitted")
      # Its connection was never pooled, so it closed with the call and
      # close drained well inside the grace.
      started = monotonic
      expect(closer.value).to be(true)
      expect(monotonic - started).to be < 5
      expect(stats(transport)[1..2]).to eq([0, 0])
    end
  end

  # request:strand keeps the request from hyper on a healthy connection, so
  # the call waits until forced teardown aborts the connection's dispatcher,
  # whose end then strands it. The stranded call must not dial a replacement.
  it "fails a stranded call that forced teardown ends with a forced phrase and no replacement dial" do
    transport = hyper
    native_driver = driver(transport)
    native_driver.hold("request:strand", 0)
    peer_client = client(peer["connect_h2c"], transport: transport, protocol: :connect)
    call = Thread.new { error_of { peer_client.unary(MSG.new, timeout: 10) } }
    wait_until("the request stranded") { native_driver.reached("request:strand") == 1 }
    expect(transport.close(grace: 0)).to be(false)
    expect(call.value&.message).to forced_phrase
    expect([stats(transport).first, native_driver.reached("request:strand")]).to eq([1, 1])
  ensure
    native_driver&.release("request:strand")
  end

  # The dialing call parks its thread inside the poll that finished the
  # handshake, before the connection's dispatcher registers. A forced close
  # then returns; the released call must not start a task it no longer owns.
  # Both dials are the call's own (per-call HTTP/2, pooled HTTP/1.1), so the
  # hold parks the caller's thread, never a runtime worker that close waits
  # for. Neither request reaches the peer: the sealed tracker refused the
  # dispatcher, so hyper hands each request back unqueued. The gRPC call
  # (HTTP/2, write first) also loses its response task to the sealed tracker
  # and fails at its next wait; the Connect unary (HTTP/1.1) retries and meets
  # the closed transport. Each fails with one of the two forced phrases.
  { "HTTP/2" => [{ connections: :per_call }, "grpc_h2c", :grpc],
    "HTTP/1.1" => [{ http2: false }, "connect_h2c", :connect] }
    .each do |name, (options, origin, protocol)|
      it "starts no #{name} dispatcher task after a forced close returned" do
        transport = hyper(**options)
        native_driver = driver(transport)
        native_driver.hold("connect:handshaken", 10)
        native_driver.hold("dispatcher", 10)
        dialing = Thread.new do
          error_of { client(peer[origin], transport: transport, protocol: protocol).unary(MSG.new, timeout: 20) }
        end
        wait_until("the handshake") { native_driver.reached("connect:handshaken") == 1 }
        expect(transport.close(grace: 0)).to be(false)
        native_driver.release("connect:handshaken")
        error = dialing.value
        expect([native_driver.reached("dispatcher"), stats(transport)[2]]).to eq([0, 0])
        expect([error.code, error.message]).to match(["unavailable", forced_phrase])
      ensure
        native_driver&.release("dispatcher")
      end
    end

  # Forced teardown refuses spawns before it fires `shutdown` (#193), so a
  # dial that finishes its handshake while teardown is parked between the two
  # registers no dispatcher, and the server never receives the request.
  { "HTTP/2" => [{ connections: :per_call }, :grpc],
    "HTTP/1.1" => [{ http2: false }, :connect] }
    .each do |name, (options, protocol)|
      it "starts no #{name} dispatcher, nor sends its request, once forced teardown fired shutdown" do
        tiny = TinyHTTP.new if protocol == :connect
        origin = tiny ? tiny.origin : peer["raw"]
        from = peer.mark
        transport = hyper(**options)
        native_driver = driver(transport)
        native_driver.hold("connect:handshaken", 10)
        native_driver.hold("force:shutdown", 10)
        native_driver.hold("dispatcher", 0)
        dialing = Thread.new do
          error_of { raw_call(origin, "ok", transport: transport, protocol: protocol, timeout: 20) }
        end
        wait_until("the handshake") { native_driver.reached("connect:handshaken") == 1 }
        closer = Thread.new { transport.close(grace: 0) }
        wait_until("forced teardown parked after shutdown") { native_driver.reached("force:shutdown") == 1 }
        native_driver.release("connect:handshaken")
        error = dialing.value
        expect([error.code, error.message]).to match(["unavailable", forced_phrase])
        expect(native_driver.reached("dispatcher")).to eq(0)
        requests = tiny ? tiny.count(:request) : peer.since(from).grep(/\Araw request /).size
        expect(requests).to eq(0)
        native_driver.release("force:shutdown")
        expect(closer.value).to be(false)
      ensure
        %w[connect:handshaken force:shutdown dispatcher].each { |point| native_driver&.release(point) }
        closer&.join(10)
        tiny&.close
      end
    end

  # Head first: the released call dispatches its request to the task that
  # awaits the response, which the sealed tracker refuses. The response head
  # then finds that task gone without a delivery: forced teardown, not an
  # abort of the call, so never a bare "HTTP transport failed".
  { "HTTP/2" => [{ connections: :per_call }, "grpc_h2c", true],
    "HTTP/1.1" => [{ http2: false }, "connect_h2c", false] }
    .each do |name, (options, origin, grpc)|
      it "fails an #{name} exchange waiting for its response head with a forced phrase after a forced close" do
        transport = hyper(**options)
        native_driver = driver(transport)
        native_driver.hold("connect:handshaken", 10)
        content_type = grpc ? "application/grpc" : "application/connect+proto"
        request = ConnectRPC::StreamRequest.new(url: URI("#{peer[origin]}/connectrpc.grpcinterop.v1.Peer/Bidi"),
                                                http_method: "POST", headers: { "content-type" => [content_type] })
        opening = Thread.new do
          error_of do
            open = grpc ? :open_grpc_stream : :open_stream
            transport.public_send(open, request) { |exchange| exchange.response_head(timeout: 20) }
          end
        end
        wait_until("the handshake") { native_driver.reached("connect:handshaken") == 1 }
        expect(transport.close(grace: 0)).to be(false)
        native_driver.release("connect:handshaken")
        error = opening.value
        expect([error.code, error.message]).to match(["unavailable", forced_phrase])
        expect(stats(transport)[1..2]).to eq([0, 0])
      ensure
        native_driver&.release("connect:handshaken")
      end
    end

  it "drains in-flight calls within grace and returns true" do
    from = peer.mark
    transport = hyper
    peer_client = client(peer["grpc_h2c"], transport: transport)
    received = Queue.new
    stream = Thread.new do
      peer_client.server_stream(MSG.new(value: "gate:drain"), timeout: 30) do |reader|
        reader.each { |message| received << message.value }
      end
      :completed
    end
    expect(received.pop(timeout: 5)).to eq("m0")
    closer = Thread.new { transport.close(grace: 10) }
    wait_until("admission closed") { transport.inspect.include?("state=closing") }
    expect([closed_error(transport).code, closed_error(transport).message])
      .to eq(["unavailable", "HTTP transport admission is closed"])
    peer.command("release drain")
    expect(closer.value).to be(true)
    expect(stream.value).to eq(:completed)
    expect(stats(transport)[1..2]).to eq([0, 0])
    peer.wait_closed("grpc", from: from)
  end

  # A pooled connection would keep its connection task, so close would wait
  # out the grace and return false.
  it "never pools a retry's own HTTP/2 connection that finishes dialing after close began" do
    from = peer.mark
    transport = hyper
    native_driver = driver(transport)
    origin = peer["raw"]
    tag = "close-retry-#{rand(1 << 30)}"
    # Connection A is pooled and stays usable, so the refused call's retry
    # dials a connection of its own: the only handshake from here on.
    raw_call(origin, "ok", transport: transport, protocol: :connect)
    native_driver.hold("connect:handshaken", 10)
    call = Thread.new do
      raw_call(origin, "refuse-once:#{tag}", transport: transport, protocol: :connect, timeout: 20).message
    rescue ConnectRPC::Error => e
      [e.code, e.message]
    end
    wait_until("the retry's handshake") { native_driver.reached("connect:handshaken") == 1 }
    closer = Thread.new { transport.close(grace: 3) }
    wait_until("admission closed") { transport.inspect.include?("state=closing") }
    wait_parked(closer, "Driver#close")
    native_driver.release("connect:handshaken")
    expect(call.value).to eq(MSG.new)
    expect(closer.value).to be(true)
    expect([stats(transport)[1..2], peer.count(/\Araw accept /, from: from)]).to eq([[0, 0], 2])
  ensure
    native_driver&.release("connect:handshaken")
  end

  # Without an idle timer (which closing also ends), a parked connection
  # would keep its task until forced teardown.
  it "never parks an HTTP/1.1 connection whose call ends cleanly after close began" do
    arrived = Queue.new
    go = Queue.new
    server = TinyHTTP.new do |_request, socket|
      arrived << true
      go.pop
      TinyHTTP.ok(socket)
    end
    transport = hyper(http2: false, idle_timeout: nil)
    call = Thread.new { client(server.origin, transport: transport, protocol: :connect).unary(MSG.new, timeout: 20) }
    expect(arrived.pop(timeout: 5)).to be(true)
    closer = Thread.new { transport.close(grace: 3) }
    wait_until("admission closed") { transport.inspect.include?("state=closing") }
    wait_parked(closer, "Driver#close")
    go << true
    expect(call.value.message).to eq(MSG.new)
    expect(closer.value).to be(true)
    expect(stats(transport)[1..2]).to eq([0, 0])
  ensure
    go&.push(true)
    server&.close
  end

  it "aborts calls still running after grace, waits for its tasks and returns false" do
    from = peer.mark
    key = peer.hold_key
    transport = hyper
    held = Thread.new do
      client(peer["grpc_h2c"], transport: transport)
        .server_stream(MSG.new(value: "hold:#{key}"), timeout: 30) { |reader| values(reader) }
    rescue ConnectRPC::Error => e
      [e.code, e.message]
    end
    peer.wait_event(peer.hold_sent(key), from: from)
    expect(transport.close(grace: 0.2)).to be(false)
    expect(held.value).to eq(["unavailable", "HTTP transport failed: connection closed"])
    expect(stats(transport)[1..2]).to eq([0, 0])
    peer.wait_event(peer.hold_done(key), from: from)
    expect(closed_error(transport).message).to eq("HTTP transport admission is closed")
  end

  it "shares the first outcome among repeated and concurrent closers" do
    from = peer.mark
    key = peer.hold_key
    transport = hyper
    held = Thread.new do
      client(peer["grpc_h2c"], transport: transport)
        .server_stream(MSG.new(value: "hold:#{key}"), timeout: 30) { |reader| values(reader) }
    rescue ConnectRPC::Error
      nil
    end
    peer.wait_event(peer.hold_sent(key), from: from)
    closers = Array.new(3) { Thread.new { transport.close(grace: 0.3) } }
    expect(closers.map(&:value)).to eq([false, false, false])
    expect(transport.close(grace: 5)).to be(false)
    held.join
    # The peer logs the held stream's end only once it sees the reset, which
    # can come after the call returned; wait so it lands in this example.
    peer.wait_event(peer.hold_done(key), from: from)
    expect(hyper.close).to be(true)
  end

  # The task-drop order: hyper's HTTP/2 connection task, which owns the
  # socket, is spawned by the pool dial's handshake into the runtime worker
  # running that dial, which then parks at connect:handshaken. tokio never
  # lets another worker steal that slot, so close aborts the connection task
  # before its first poll. Its socket drop then parks too: the task must
  # still be counted, and close must not return, until the socket is gone.
  # force:sealed (zero bound) only marks that teardown sealed the tracker.
  it "waits for a task aborted before its first poll to drop its socket" do
    transport = hyper
    native_driver = driver(transport)
    native_driver.hold("connect:handshaken", 10)
    native_driver.hold("socket:drop", 10)
    native_driver.hold("force:sealed", 0)
    dialing = Thread.new { error_of { client(peer["grpc_h2c"], transport: transport).unary(MSG.new, timeout: 20) } }
    wait_until("the handshake") { native_driver.reached("connect:handshaken") == 1 }
    closer = Thread.new { transport.close(grace: 0) }
    wait_until("the sealed tracker") { native_driver.reached("force:sealed") == 1 }
    native_driver.release("connect:handshaken")
    wait_until("the socket drop") { native_driver.reached("socket:drop") == 1 }
    expect([stats(transport)[2], closer.alive?]).to eq([1, true])
    native_driver.release("socket:drop")
    expect(closer.value).to be(false)
    expect(stats(transport)[2]).to eq(0)
    # With grace 0 the waiter may see the forced teardown before it sees
    # close, so it fails with either forced phrase.
    expect([dialing.value.code, dialing.value.message]).to match(["unavailable", forced_phrase])
    # A forced close does not wait for calls to unwind: the call leaves the
    # in-flight count only as it returns.
    expect(stats(transport)[1..2]).to eq([0, 0])
  ensure
    %w[connect:handshaken socket:drop force:sealed].each { |name| native_driver&.release(name) }
  end

  # Forced teardown ends the call's own dial with a forced phrase.
  it "closes during a per-call dial with grace 0 and fails the dialing call with a forced phrase", :linux_only do
    port, cleanup = full_backlog
    transport = hyper(connections: :per_call)
    dialing = Thread.new do
      client("http://127.0.0.1:#{port}", transport: transport).unary(MSG.new, timeout: 30)
    rescue ConnectRPC::Error => e
      [e.code, e.message]
    end
    wait_until("SYN_SENT") { syn_sent?(port) }
    wait_parked(dialing, "Native::")
    expect(transport.close(grace: 0)).to be(false)
    expect(dialing.value).to match(["unavailable", forced_phrase])
    expect(stats(transport)[1..2]).to eq([0, 0])
  ensure
    cleanup&.call
  end

  # Every caller waits on the pool dial, including the one whose call started
  # it. Close fails them with the closed phrase and ends the dial, so it
  # drains at once.
  it "fails lazy exchanges and the waiters of a pool dial with the closed phrase, ending the dial", :linux_only do
    port, cleanup = full_backlog
    baseline = syn_sent(port)
    transport = hyper(connect_timeout: 30)
    callers = count_dial_joins(driver(transport)) do
      %i[grpc connect].each_with_index.map do |protocol, index|
        thread = Thread.new do
          client("http://127.0.0.1:#{port}", transport: transport, protocol: protocol).unary(MSG.new, timeout: 30)
        rescue ConnectRPC::Error => e
          e.message
        end
        wait_joined(driver(transport), index + 1)
        thread
      end
    end
    # The dial is its own task; it opens the socket after its starter joined.
    wait_until("the pool dial's SYN") { syn_sent(port) == baseline + 1 }
    lazy = nil
    request = ConnectRPC::StreamRequest.new(url: URI("#{peer['grpc_h2c']}/connectrpc.grpcinterop.v1.Peer/Bidi"),
                                            http_method: "POST", headers: { "content-type" => ["application/grpc"] })
    lazy_result = Thread.new do
      transport.open_grpc_stream(request) do |exchange|
        lazy = :opened
        Thread.stop
        exchange.write("\x00\x00\x00\x00\x00".b)
      end
    rescue ConnectRPC::Error => e
      e.message
    end
    wait_until("lazy exchange opened") { lazy == :opened && lazy_result.status == "sleep" }
    closer = Thread.new { transport.close(grace: 10) }
    expect(callers.map(&:value)).to eq(Array.new(2, "HTTP transport admission is closed"))
    lazy_result.wakeup
    expect(lazy_result.value).to eq("HTTP transport admission is closed")
    started = monotonic
    expect(closer.value).to be(true)
    expect(monotonic - started).to be < 5
    expect(stats(transport)[1..2]).to eq([0, 0])
    wait_until("the dial's socket closed") { syn_sent(port) == baseline }
  ensure
    cleanup&.call
  end

  # A fresh process, so this close is the runtime's first use. The injected
  # start failure panics as tokio does when the OS refuses a worker thread.
  it "lets a later close start teardown after the first close failed to start the runtime" do
    output, status = ruby_child(<<~RUBY, timeout: 30)
      transport = ConnectRPC::Hyper::Transport.new
      driver = transport.send(:driver)
      ConnectRPC::Hyper.const_get(:Native).fail_runtime_starts(1)
      first = begin
        transport.close(grace: 1)
      rescue Exception => e # rubocop:disable Lint/RescueException
        [e.class.name, e.message]
      end
      admission = begin
        transport.send(:driver)
      rescue ConnectRPC::Error => e
        e.message
      end
      closer = Thread.new { transport.close(grace: 1) }
      second = closer.join(10) ? closer.value : :still_waiting
      p [first, admission, driver.close_outcome, second]
      # A stuck native close defers interrupts, so a normal exit would wait on it.
      exit!(0)
    RUBY
    expect(status).to be_success
    first, admission, outcome, second = eval(output.lines.last) # rubocop:disable Security/Eval
    expect(first).to eq(["fatal", "OS can't spawn worker thread: failure injected by a spec"])
    # The transport stays closing between the two closes.
    expect(admission).to eq("HTTP transport admission is closed")
    expect([outcome, second]).to eq([true, true])
  end
end
