# frozen_string_literal: true

RSpec.describe "Cancellation and deadlines" do
  def canceled_call(source)
    thread = Thread.new do
      yield source.token
      :returned
    rescue ConnectRPC::Error => e
      [e.code, e.message]
    end
    [thread, -> { source.cancel }]
  end

  [false, true].each do |data_in_flight|
    it "resets one stream #{data_in_flight ? 'while the peer is sending DATA' : 'quietly'} beside a live sibling" do
      from = peer.mark
      key = peer.hold_key unless data_in_flight
      transport = hyper
      peer_client = client(peer["grpc_h2c"], transport: transport)
      peer_client.bidi(timeout: 20) do |sibling|
        sibling.send_message(MSG.new(value: "before"))
        expect(sibling.receive.value).to eq("before")
        source = ConnectRPC::CancellationSource.new
        started = Queue.new
        thread, cancel = canceled_call(source) do |token|
          value = data_in_flight ? "flood" : "hold:#{key}"
          peer_client.server_stream(MSG.new(value: value), timeout: 20, cancellation: token) do |reader|
            reader.each { started << true if started.empty? }
          end
        end
        # The stream delivered its first message: the peer is sending (flood)
        # or holding, and the reader keeps reading.
        expect(started.pop(timeout: 10)).to be(true)
        wait_parked(thread, "Native::") unless data_in_flight
        cancel.call
        expect(thread.value).to eq(["canceled", "Call canceled"])
        ended = data_in_flight ? /grpc ss-flood-done/ : peer.hold_done(key, reason: "context canceled")
        peer.wait_event(ended, from: from)
        sibling.send_message(MSG.new(value: "after"))
        expect(sibling.receive.value).to eq("after")
        sibling.close_send
        expect(sibling.receive).to be_nil
      end
      # Both streams shared one connection.
      expect(peer.count(/\Agrpc accept/, from: from)).to eq(1)
    end
  end

  it "cancels from another thread through a CancellationSource" do
    from = peer.mark
    key = peer.hold_key
    source = ConnectRPC::CancellationSource.new
    thread, cancel = canceled_call(source) do |token|
      client(peer["connect_h2c"], transport: hyper, protocol: :connect)
        .server_stream(MSG.new(value: "hold:#{key}"), timeout: 20, cancellation: token) { |reader| values(reader) }
    end
    peer.wait_event(peer.hold_sent(key, tag: "connect"), from: from)
    wait_parked(thread, "Exchange#read")
    started = monotonic
    cancel.call
    expect(thread.value).to eq(["canceled", "Call canceled"])
    expect(monotonic - started).to be < 0.5
    peer.wait_event(peer.hold_done(key, tag: "connect", reason: "context canceled"), from: from)
  end

  # grpc-go's server resets a call with RST_STREAM(CANCEL) when the deadline
  # it was sent expires by its own clock, and that timer can fire before the
  # client's deadline (on Windows Go reads the coarse interrupt time, so it
  # can fire up to a clock tick early). The raw peer's stalled call never ends
  # that way, so only the client's own deadline ends it.
  it "applies a per-call deadline to one call on a shared connection" do
    from = peer.mark
    transport = hyper
    tag = "deadline-sibling-#{rand(1 << 30)}"
    sent = Queue.new
    resume = Queue.new
    metadata = { "x-scenario" => "await-ok:#{tag}" }
    sibling = Thread.new do
      core(peer["raw"], transport: transport)
        .client_stream(HyperHelpers::RAW_CLIENT_STREAM, metadata: metadata, timeout: 20) do |writer|
        writer.send_message(MSG.new(value: "a"))
        sent << true
        resume.pop(timeout: 10) || raise("never resumed")
        writer.send_message(MSG.new(value: "b"))
      end.message
    end
    expect(sent.pop(timeout: 10)).to be(true)
    peer.wait_event(/\Araw request await-ok:#{tag} /, from: from)
    started = monotonic
    error = error_of { raw_call(peer["raw"], "stall", transport: transport, timeout: 0.3) }
    expect([error.code, error.message, monotonic - started])
      .to match(["deadline_exceeded", "Deadline exceeded", be_between(0.29, 1.5)])
    stalled = peer.wait_event(/\Araw request stall /, from: from)[/ stream=(\d+) /, 1]
    peer.wait_event(/\Araw rst stream=#{stalled} code=8\z/, from: from)
    resume << true
    expect(sibling.value).to eq(MSG.new)
    # The peer answers once the sibling's request ends, counting both messages,
    # the second sent after the other call expired: 5-byte gRPC prefixes and
    # 3-byte messages.
    peer.wait_event(/\Araw handled await-ok:#{tag} stream=\d+ bytes=16\z/, from: from)
    expect(peer.count(/\Araw accept /, from: from)).to eq(1)
  end

  # grpc-go's client reports a peer's RST_STREAM(CANCEL) as DeadlineExceeded
  # once the call's own deadline passed, and as Canceled before it (the
  # "maps gRPC reset code 8" spec). The call parks in its response-head wait
  # under a 20-second native budget; the deadline's clock passes while the
  # peer holds the reset, and the call must end well before that budget, so
  # only the reset can end its wait.
  it "reports a peer's CANCEL reset after the call's deadline as deadline_exceeded" do
    tag = "reset-late-#{rand(1 << 30)}"
    now = 0.0
    deadline = ConnectRPC::Deadline.new(20, clock: -> { now })
    call = Thread.new do
      error_of { raw_call(peer["raw"], "reset-on:#{tag}:8", transport: hyper, timeout: nil, deadline: deadline) }
    end
    wait_parked(call, "Exchange#response_head")
    now = 20.0
    peer.command("release #{tag}")
    expect(call.join(5)).to be(call)
    expect([call.value.code, call.value.message, call.value.cause])
      .to eq(["deadline_exceeded", "Deadline exceeded", nil])
  end

  it "cancels a token during TCP connect, TLS and while waiting behind another caller's dial", :linux_only do
    port, cleanup = full_backlog
    silent = TCPServer.new("127.0.0.1", 0)
    accepted = Queue.new
    acceptor = Thread.new { loop { accepted << silent.accept } }
    transport = hyper
    dialer, waiter, cancel_waiter = count_dial_joins(driver(transport)) do
      dialer, = canceled_call(ConnectRPC::CancellationSource.new) do |token|
        client("http://127.0.0.1:#{port}", transport: transport).unary(MSG.new, timeout: 30, cancellation: token)
      end
      wait_until("SYN_SENT") { syn_sent?(port) }
      wait_parked(dialer, "Native::")
      # The second caller to the same origin waits behind the first one's dial.
      source = ConnectRPC::CancellationSource.new
      waiter, cancel_waiter = canceled_call(source) do |token|
        client("http://127.0.0.1:#{port}", transport: transport).unary(MSG.new, timeout: 30, cancellation: token)
      end
      wait_joined(driver(transport), 2)
      [dialer, waiter, cancel_waiter]
    end
    wait_parked(waiter, "Native::")
    cancel_waiter.call
    expect(waiter.value).to eq(["canceled", "Call canceled"])
    expect(dialer).to be_alive
    dialer.kill.join
    tls_source = ConnectRPC::CancellationSource.new
    tls, cancel_tls = canceled_call(tls_source) do |token|
      client("https://127.0.0.1:#{silent.addr[1]}", transport: hyper(**tls_options))
        .unary(MSG.new, timeout: 30, cancellation: token)
    end
    # The ClientHello arrived, so the call now waits in the TLS handshake.
    expect((accepted.pop(timeout: 5) || raise("no TLS accept")).wait_readable(5)).not_to be_nil
    wait_parked(tls, "Native::")
    cancel_tls.call
    expect(tls.value).to eq(["canceled", "Call canceled"])
  ensure
    cleanup&.call
    acceptor&.kill
    silent&.close
  end

  it "cancels a token during name resolution", :dns_shim do
    environment = stall_environment
    output, status = ruby_child(<<~RUBY, env: environment)
      events = File.open(ENV.fetch("STALL_EVENTS"), "r+")
      source = ConnectRPC::CancellationSource.new
      thread = Thread.new do
        client("http://peer.stall:1", ConnectRPC::Hyper::Transport.new).unary(MSG.new, timeout: 30, cancellation: source.token)
      rescue ConnectRPC::Error => e
        e.code
      end
      events.gets
      Thread.pass until thread.status == "sleep"
      source.cancel
      p thread.value
    RUBY
    expect(status).to be_success
    expect(output.lines.last.chomp).to eq('"canceled"')
  end

  it "bounds a silent HTTP/2 peer by the call deadline" do
    silent = TCPServer.new("127.0.0.1", 0)
    acceptor = Thread.new { loop { silent.accept } }
    %i[grpc connect].each do |protocol|
      started = monotonic
      error = error_of do
        client("http://127.0.0.1:#{silent.addr[1]}", transport: hyper, protocol: protocol)
          .unary(MSG.new, timeout: 0.3)
      end
      expect([error.code, monotonic - started]).to match(["deadline_exceeded", be_between(0.29, 1.5)])
    end
  ensure
    acceptor&.kill
    silent&.close
  end

  it "sends the deadline remaining at dispatch, after a delayed setup", :dns_shim do
    environment = stall_environment
    from = peer.mark
    port = URI(peer["raw"]).port
    # Resolution of *.delay names waits for the parent; the second of delay
    # before releasing it is the setup time the wire timeout must reflect.
    output, status = ruby_child(<<~RUBY, env: environment)
      events = File.open(ENV.fetch("STALL_EVENTS"), "r+")
      method = ConnectRPC::Method.new(service: "raw.v1.Raw", name: "Call", request_type: MSG, response_type: MSG)
      %i[grpc connect].each do |protocol|
        thread = Thread.new do
          ConnectRPC::Client.new(base_url: "http://peer.delay:#{port}", transport: ConnectRPC::Hyper::Transport.new,
                                 protocol: protocol, binding: ConnectRPC::Bindings::GoogleProtobuf.new)
                            .call(method, MSG.new, metadata: { "x-scenario" => "ok" }, timeout: 5)
        end
        events.gets
        sleep 1
        File.write(ENV.fetch("STALL_RELEASE"), "x")
        thread.value
      end
      puts "done"
    RUBY
    expect(status).to be_success
    expect(output.lines.last.chomp).to eq("done")
    timeouts = peer.wait_count(/\Araw request ok /, 2, from: from).map { |event| event[/timeout=(\S+)/, 1] }
    grpc, connect = timeouts
    expect(grpc).to match(/\A\d+[mu]\z/)
    expect(grpc.end_with?("m") ? grpc.to_i : grpc.to_i / 1000).to be_between(3000, 4100)
    expect(connect.to_i).to be_between(3000, 4100)
  end
end
