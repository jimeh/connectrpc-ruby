# frozen_string_literal: true

require_relative "support/socket_proxy"

RSpec.describe "Transport#close" do
  include SocketProxy

  def closed_error(transport, origin = peer["grpc_h2c"])
    error_of { client(origin, transport: transport).unary(MSG.new, timeout: 5) }
  end

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

  %i[pooled per_call].each do |connections|
    it "lets a #{connections} call admitted before close finish its dial and call, then returns true" do
      arrived = Queue.new
      gate = Queue.new
      with_socket_proxy(gated_relay(arrived, gate)) do |url|
        transport = hyper(connections: connections, proxy: url)
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
  end

  # The dialing call parks its thread inside the poll that finished the
  # handshake, before the connection's dispatcher registers. A forced close
  # then returns; the released call must not start a task it no longer owns.
  # The HTTP/1.1 request comes back unsent and its retry meets the closed
  # transport.
  { "HTTP/2" => [true, "grpc_h2c", :grpc, "HTTP transport failed: connection closed"],
    "HTTP/1.1" => [false, "connect_h2c", :connect, "HTTP transport admission is closed"] }
    .each do |name, (http2, origin, protocol, message)|
      it "starts no #{name} dispatcher task after a forced close returned" do
        transport = hyper(http2: http2)
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
        expect([error.code, error.message]).to eq(["unavailable", message])
      ensure
        native_driver&.release("dispatcher")
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
    peer.wait_event(/\Agrpc conn-closed /, from: from)
  end

  it "aborts calls still running after grace, waits for its tasks and returns false" do
    from = peer.mark
    transport = hyper
    held = Thread.new do
      client(peer["grpc_h2c"], transport: transport)
        .server_stream(MSG.new(value: "hold"), timeout: 30) { |reader| values(reader) }
    rescue ConnectRPC::Error => e
      [e.code, e.message]
    end
    peer.wait_event(/grpc ss-hold-sent/, from: from)
    expect(transport.close(grace: 0.2)).to be(false)
    expect(held.value).to eq(["unavailable", "HTTP transport failed: connection closed"])
    expect(stats(transport)[1..2]).to eq([0, 0])
    peer.wait_event(/grpc ss-hold-done/, from: from)
    expect(closed_error(transport).message).to eq("HTTP transport admission is closed")
  end

  it "shares the first outcome among repeated and concurrent closers" do
    from = peer.mark
    transport = hyper
    held = Thread.new do
      client(peer["grpc_h2c"], transport: transport)
        .server_stream(MSG.new(value: "hold"), timeout: 30) { |reader| values(reader) }
    rescue ConnectRPC::Error
      nil
    end
    peer.wait_event(/grpc ss-hold-sent/, from: from)
    closers = Array.new(3) { Thread.new { transport.close(grace: 0.3) } }
    expect(closers.map(&:value)).to eq([false, false, false])
    expect(transport.close(grace: 5)).to be(false)
    held.join
    expect(hyper.close).to be(true)
  end

  it "closes during connection setup with grace 0 and fails the dialing call", :linux_only do
    port, cleanup = full_backlog
    transport = hyper
    dialing = Thread.new do
      client("http://127.0.0.1:#{port}", transport: transport).unary(MSG.new, timeout: 30)
    rescue ConnectRPC::Error => e
      [e.code, e.message]
    end
    wait_until("SYN_SENT") { syn_sent?(port) }
    wait_parked(dialing, "Native::")
    expect(transport.close(grace: 0)).to be(false)
    expect(dialing.value).to eq(["unavailable", "HTTP transport failed: connection closed"])
    expect(stats(transport)[1..2]).to eq([0, 0])
  ensure
    cleanup&.call
  end

  it "fails lazy exchanges and waiters behind a dial with the closed phrase", :linux_only do
    port, cleanup = full_backlog
    transport = hyper
    dialer = Thread.new do
      client("http://127.0.0.1:#{port}", transport: transport).unary(MSG.new, timeout: 30)
    rescue ConnectRPC::Error => e
      e.message
    end
    wait_until("SYN_SENT") { syn_sent?(port) }
    waiter = Thread.new do
      client("http://127.0.0.1:#{port}", transport: transport, protocol: :connect).unary(MSG.new, timeout: 30)
    rescue ConnectRPC::Error => e
      e.message
    end
    wait_parked(waiter, "Driver#unary")
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
    closer = Thread.new { transport.close(grace: 5) }
    expect(waiter.value).to eq("HTTP transport admission is closed")
    lazy_result.wakeup
    expect(lazy_result.value).to eq("HTTP transport admission is closed")
    expect(closer.join(10)).not_to be_nil
    expect(dialer.value).to eq("HTTP transport failed: connection closed")
  ensure
    cleanup&.call
  end
end
