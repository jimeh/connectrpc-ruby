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

  # The pool dial parks at connect:handshaken while two callers wait for it,
  # then again between pooling its connection and publishing its outcome; a
  # caller arriving then uses the pooled connection at once, and one after
  # publication finds it pooled. Every caller shares the one accept. A caller
  # parked in a native wait may not have claimed yet, so the spec waits for
  # both to join the dial.
  { "Driver#unary" => :connect, "Exchange#write" => :grpc }.each do |path, protocol|
    it "shares one accept among #{path} callers arriving before, during and after the dial pools its connection" do
      from = peer.mark
      transport = hyper
      native_driver = driver(transport)
      native_driver.hold("connect:handshaken", 10)
      native_driver.hold("dial:pooled", 10)
      peer_client = client(peer["#{protocol}_h2c"], transport: transport, protocol: protocol)
      call = ->(value) { Thread.new { peer_client.unary(MSG.new(value: value), timeout: 10).message.value } }
      before = count_dial_joins(native_driver) do
        threads = %w[b0 b1].map { |value| call.call(value) }
        wait_joined(native_driver, 2)
        threads
      end
      wait_until("the dial's handshake") { native_driver.reached("connect:handshaken") == 1 }
      native_driver.release("connect:handshaken")
      wait_until("the dial pooled its connection") { native_driver.reached("dial:pooled") == 1 }
      during = call.call("during")
      expect(during.join(5)&.value).to eq("during")
      expect(before).to all(be_alive)
      native_driver.release("dial:pooled")
      expect(before.map(&:value)).to eq(%w[b0 b1])
      expect(call.call("after").value).to eq("after")
      expect(accepts(protocol, from)).to eq(1)
    ensure
      native_driver&.release("connect:handshaken")
      native_driver&.release("dial:pooled")
    end
  end

  # Three callers wait on a pool dial through tunnel 0, which reaches an
  # http/1.1-only peer. The peer holds every call until released, so the
  # caller that takes the parked connection keeps it busy, and the other two
  # find no usable connection and dial their own, through tunnels 1 and 2 to
  # an h2 peer, each held by its gate. Tunnel 1's connection is pooled first;
  # tunnel 2's then serves only its own call and closes, and the next call
  # reuses tunnel 1.
  it "keeps the first pooled HTTP/2 connection when callers after an HTTP/1.1 dial each negotiate h2" do
    from = peer.mark
    tag = "after-h1-#{rand(1 << 30)}"
    h1_port = URI(peer["connect_tls_h1"]).port
    h2_port = URI(peer["connect_tls"]).port
    gates = Array.new(3) { Queue.new }
    with_routed_tunnels(->(index) { index.zero? ? h1_port : h2_port }, gates) do |url, heads, ended|
      transport = hyper(proxy: url, **tls_options)
      native_driver = driver(transport)
      peer_client = client("https://127.0.0.1:#{h2_port}", transport: transport, protocol: :connect)
      waiters = count_dial_joins(native_driver) do
        Array.new(3) do |index|
          thread = Thread.new { peer_client.unary(MSG.new(value: "gate:#{tag}:w#{index}"), timeout: 20).message.value }
          expect(heads.pop(timeout: 10)).to start_with("CONNECT ") if index.zero?
          wait_joined(native_driver, index + 1)
          thread
        end
      end
      expect(heads.size).to eq(0)
      gates[0] << true
      2.times { expect(heads.pop(timeout: 10)).to start_with("CONNECT ") }
      gates[1] << true
      # The parked HTTP/1.1 connection's call and tunnel 1's call.
      peer.wait_count(/ unary-gate-start #{tag}\z/, 2, from: from)
      gates[2] << true
      peer.wait_count(/ unary-gate-start #{tag}\z/, 3, from: from)
      peer.command("release #{tag}")
      expect(waiters.map(&:value)).to eq(Array.new(3) { |index| "gate:#{tag}:w#{index}" })
      expect(ended.pop(timeout: 10)).to eq(2)
      expect(peer_client.unary(MSG.new(value: "next"), timeout: 5).message.value).to eq("next")
      expect([heads.size, ended.size]).to eq([0, 0])
      transport.close(grace: 0)
    end
  end

  it "opens one connection per call with connections: :per_call" do
    from = peer.mark
    peer_client = client(peer["grpc_h2c"], transport: hyper(connections: :per_call))
    5.times { |index| peer_client.unary(MSG.new(value: index.to_s), timeout: 5) }
    expect(accepts("grpc", from)).to eq(5)
    peer.wait_closed("grpc", from: from, count: 5)
  end

  it "closes an idle pooled connection after idle_timeout and reconnects on the next call" do
    from = peer.mark
    peer_client = client(peer["grpc_h2c"], transport: hyper(idle_timeout: 0.2))
    peer_client.unary(MSG.new(value: "a"), timeout: 5)
    peer.wait_closed("grpc", from: from)
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

  # The reaper parks (for its hold's bound, 0.5 s) between seeing no calls
  # and reading the checkout generation, while a stream checks the
  # connection out. Reading both under the checkout lock orders that
  # checkout after the snapshot, so the stream's use restarts the idle
  # period. A snapshot that counted the checkout as idle would close the
  # connection one idle_timeout after the checkout; the stream ends halfway
  # through that, and the close is measured from its end. Elapsed idle time
  # is the behaviour under test, so these are clock points.
  it "does not count a checkout that races the idle reaper's snapshot as idle time" do
    from = peer.mark
    transport = hyper(idle_timeout: 1.0)
    native_driver = driver(transport)
    native_driver.hold("reap:idle", 0.5)
    peer_client = client(peer["grpc_h2c"], transport: transport)
    tag = "reap-#{rand(1 << 30)}"
    peer_client.unary(MSG.new(value: "a"), timeout: 5)
    wait_until("the reaper's idle snapshot") { native_driver.reached("reap:idle") == 1 }
    received = []
    stream = Thread.new do
      peer_client.server_stream(MSG.new(value: "gate:#{tag}"), timeout: 30) { |reader| received.concat(values(reader)) }
    end
    peer.wait_event(/grpc ss-gate-sent #{tag}/, from: from)
    checked_out = monotonic
    native_driver.release("reap:idle")
    wait_until("halfway through an idle period") { monotonic >= checked_out + 0.5 }
    released = monotonic
    peer.command("release #{tag}")
    stream.join
    expect(received).to eq(%w[m0 m1])
    peer.wait_closed("grpc", from: from)
    expect(monotonic - released).to be >= 1.0
    expect(accepts("grpc", from)).to eq(1)
  end

  it "restarts an HTTP/2 connection's idle period on reuse and closes it after a full idle_timeout" do
    from = peer.mark
    transport = hyper(idle_timeout: 1.0)
    peer_client = client(peer["connect_h2c"], transport: transport, protocol: :connect)
    address = nil
    open = lambda do
      address ||= Regexp.escape(peer.wait_event(/\Aconnect accept /, from: from).split.last)
      peer.count(/\Aconnect conn-closed #{address}\z/, from: from).zero?
    end
    idle_restart(transport, open: open) { -> { peer_client.unary(MSG.new(value: "x"), timeout: 5) } }
    expect(accepts("connect", from)).to eq(1)
  end

  # One pool dial for every waiter: the full-backlog listener drops SYNs, so
  # each attempt runs until connect_timeout.
  describe "concurrent callers while a dial fails", :linux_only do
    # Each caller joins the pool dial before the next starts; HTTP/1.1
    # callers share no dial, so parking in the native wait is enough there.
    def dial_storm(transport, port, http2: true, callers: 4, timeouts: Array.new(callers, 10))
      native_driver = driver(transport)
      started = monotonic
      storm = lambda do
        timeouts.each_with_index.map do |timeout, index|
          thread = Thread.new do
            client("http://127.0.0.1:#{port}", transport: transport, protocol: :connect)
              .unary(MSG.new, timeout: timeout)
          rescue ConnectRPC::Error => e
            [e.message, e.cause&.kind, monotonic - started]
          end
          http2 ? wait_joined(native_driver, index + 1) : wait_parked(thread, "Driver#unary")
          thread
        end
      end
      [http2 ? count_dial_joins(native_driver, &storm) : storm.call, started]
    end

    { true => "shares one HTTP/2 dial and its failure, and the next call dials again",
      false => "dials HTTP/1.1 connections in parallel" }.each do |http2, behaviour|
      it behaviour do
        port, cleanup = full_backlog
        baseline = syn_sent(port)
        transport = hyper(http2: http2, connect_timeout: 0.5)
        threads, = dial_storm(transport, port, http2: http2)
        expected = http2 ? 1 : 4
        wait_until("#{expected} connect attempts") { syn_sent(port) == baseline + expected }
        outcomes = threads.map(&:value)
        expect(outcomes.map { |message, kind, _| [message, kind] })
          .to eq(Array.new(4) { ["HTTP transport failed: timed out", :connect_timeout] })
        # Serialised dials would fail at 0.5, 1.0, 1.5 and 2.0 s.
        expect(outcomes.map(&:last)).to all(be_between(0.45, 0.95))
        expect(syn_sent(port)).to eq(baseline)
        # Nothing is cached: the next call starts a new dial.
        next_call = Thread.new do
          client("http://127.0.0.1:#{port}", transport: transport, protocol: :connect).unary(MSG.new, timeout: 10)
        rescue ConnectRPC::Error => e
          [e.message, e.cause&.kind]
        end
        wait_until("a new connect attempt") { syn_sent(port) == baseline + 1 }
        expect(next_call.value).to eq(["HTTP transport failed: timed out", :connect_timeout])
      ensure
        cleanup&.call
      end
    end

    # The caller that started the dial leaves at 0.3 s; the dial carries on
    # with its own connect_timeout budget and fails the waiter at 1 s, with
    # no second attempt.
    it "keeps a pool dial running after the call that started it ends" do
      port, cleanup = full_backlog
      baseline = syn_sent(port)
      transport = hyper(connect_timeout: 1.0)
      threads, = dial_storm(transport, port, timeouts: [0.3, 10])
      wait_until("the dial's attempt") { syn_sent(port) == baseline + 1 }
      starter, waiter = threads
      expect(starter.value.first).to eq("Deadline exceeded")
      # Still the one attempt, well before connect_timeout.
      expect([syn_sent(port), waiter.alive?]).to eq([baseline + 1, true])
      waiter = waiter.value
      expect(waiter.first(2)).to eq(["HTTP transport failed: timed out", :connect_timeout])
      expect(waiter.last).to be_between(0.95, 1.25)
    ensure
      cleanup&.call
    end
  end

  # The shim resolves peer.pair to 127.0.0.2 (a full backlog on the peer's
  # port drops its SYNs), then 127.0.0.1 (the peer). The pool dial splits
  # connect_timeout alone (6 s gives the first address 3 s), so the caller
  # that started it fails its own 0.5 s deadline while the dial goes on to
  # the peer, and the waiter and a later call use the pooled connection.
  it "gives a pool dial connect_timeout's budget, whatever the deadline of the call that started it",
     :dns_shim do
    environment = stall_environment
    from = peer.mark
    port = URI(peer["grpc_h2c"]).port
    blackhole = Socket.new(:INET, :STREAM)
    blackhole.bind(Addrinfo.tcp("127.0.0.2", port))
    blackhole.listen(0)
    fillers = Array.new(8) do
      socket = Socket.new(:INET, :STREAM)
      socket.connect_nonblock(Addrinfo.tcp("127.0.0.2", port), exception: false)
      socket
    end
    output, status = ruby_child(<<~RUBY, env: environment)
      transport = ConnectRPC::Hyper::Transport.new(connect_timeout: 6, proxy: nil)
      driver = transport.instance_variable_get(:@driver)
      points = %w[dial:started dial:joined]
      points.each { |point| driver.hold(point, 0) }
      peer_client = client("http://peer.pair:#{port}", transport)
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      calls = [0.5, 20].each_with_index.map do |timeout, index|
        thread = Thread.new do
          reply = begin
            peer_client.unary(MSG.new(value: "w\#{index}"), timeout: timeout).message.value
          rescue ConnectRPC::Error => e
            e.code
          end
          [reply, (Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).round(2)]
        end
        # Each caller joins the one pool dial before the next starts.
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10
        until driver.reached("dial:joined") == index + 1
          raise "caller \#{index} never joined: \#{points.map { driver.reached(it) }}" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

          Thread.pass
        end
        thread
      end
      # Counted once both calls ended: a dial started after the starter's
      # deadline would show here.
      values = calls.map(&:value)
      counts = points.map { |point| driver.reached(point) }
      points.each { |point| driver.release(point) }
      p [*values, peer_client.unary(MSG.new(value: "later"), timeout: 5).message.value, counts]
      transport.close(grace: 0)
    RUBY
    expect(status).to be_success
    starter, waiter, later, counts = eval(output.lines.last) # rubocop:disable Security/Eval
    expect(counts).to eq([1, 2])
    expect(starter).to match(["deadline_exceeded", be < 1])
    expect(waiter).to match(["w1", be_between(2.9, 4)])
    expect(later).to eq("later")
    expect(accepts("grpc", from)).to eq(1)
  ensure
    fillers&.each(&:close)
    blackhole&.close
  end

  # After the pool dial negotiated http/1.1, each waiter uses a usable pooled
  # or idle connection, else dials its own, and a streaming waiter, which
  # needs HTTP/2, fails without dialing. The peer holds every call until all
  # four arrived, so no connection is free again in between: one waiter takes
  # the parked connection and three dial.
  it "serves the waiters of a dial that negotiated HTTP/1.1 on HTTP/1.1 connections" do
    from = peer.mark
    tag = "h1-waiters-#{rand(1 << 30)}"
    gate = Queue.new
    port = URI(peer["connect_tls_h1"]).port
    with_gated_tunnels(port, gate) do |url, heads|
      transport = hyper(proxy: url, **tls_options)
      native_driver = driver(transport)
      peer_client = client("https://127.0.0.1:#{port}", transport: transport, protocol: :connect)
      calls, streaming = count_dial_joins(native_driver) do
        calls = Array.new(4) do |index|
          thread = Thread.new { peer_client.unary(MSG.new(value: "gate:#{tag}:#{index}"), timeout: 20).message.value }
          heads.pop(timeout: 10) if index.zero?
          thread
        end
        streaming = Thread.new do
          error_of { peer_client.server_stream(MSG.new(value: "x"), timeout: 20) { |reader| values(reader) } }
        end
        wait_joined(native_driver, 5)
        [calls, streaming]
      end
      # The waiters joined the held dial instead of dialing.
      expect(heads.size).to eq(0)
      gate << true
      expect(streaming.value.message).to eq("HTTP transport failed: ALPN negotiation failed")
      peer.wait_count(/ unary-gate-start #{tag}\z/, 4, from: from)
      peer.command("release #{tag}")
      expect(calls.map(&:value)).to eq(Array.new(4) { |index| "gate:#{tag}:#{index}" })
      # The dialed connection served one waiter; three more dialed their own
      # tunnels, and the streaming waiter none.
      expect(heads.size).to eq(3)
      expect(peer.since(from).grep(/connecth1 accept/).size).to eq(4)
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
      # The connection's own task and its one idle timer; aborted timers
      # leave the task set asynchronously.
      wait_until("one idle timer") { stats(transport) == [1, 0, 2, 1] }
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
      peer.wait_closed("connect", from: from)
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
