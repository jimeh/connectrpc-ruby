# frozen_string_literal: true

require "connectrpc/transports/net_http"
require_relative "support/socket_peer"

RSpec.describe "NetHTTP owned dialing" do
  include SocketPeer

  # Linux drops SYNs once a listen(0) queue holds one unaccepted connection,
  # so the next connect stays pending without leaving loopback.
  def with_full_backlog
    listener = Socket.new(:INET, :STREAM)
    listener.bind(Addrinfo.tcp("127.0.0.1", 0))
    listener.listen(0)
    filler = Socket.new(:INET, :STREAM)
    filler.connect(listener.local_address)
    yield listener.local_address.ip_port
  ensure
    filler&.close
    listener&.close
  end

  def track_sockets
    sockets = Queue.new
    allow(Socket).to receive(:new).and_wrap_original do |original, *args|
      original.call(*args).tap { |socket| sockets << socket }
    end
    sockets
  end

  %i[deadline cancellation].each do |ending|
    it "interrupts a pending TCP connect on #{ending} and closes its socket" do
      with_full_backlog do |port|
        sockets = track_sockets
        waiting = Queue.new
        allow_any_instance_of(Socket).to receive(:wait_writable).and_wrap_original do |original, *args|
          waiting << true
          original.call(*args)
        end
        source = ConnectRPC::CancellationSource.new
        request = transport_request("http://127.0.0.1:#{port}", timeout: 0.3)
        request.deadline = nil if ending == :cancellation
        request.cancellation = source.token if ending == :cancellation
        caller = Thread.new do
          ConnectRPC::Transports::NetHTTP.new.call(request)
        rescue ConnectRPC::Error => e
          e
        end
        expect(waiting.pop(timeout: 2)).to be(true)
        source.cancel if ending == :cancellation
        expect(caller.join(2)).to equal(caller)
        expect(caller.value.code).to eq(ending == :deadline ? "deadline_exceeded" : "canceled")
        socket = sockets.pop(timeout: 0)
        expect([socket.closed?, sockets.size]).to eq([true, 0])
      ensure
        caller&.join(2)
      end
    end
  end

  it "tries resolved addresses one at a time and keeps the first that connects" do
    reserved = Socket.new(:INET, :STREAM)
    reserved.bind(Addrinfo.tcp("127.0.0.1", 0))
    refused = reserved.local_address
    sockets = track_sockets
    peer = lambda do |socket|
      read_request(socket)
      socket.write("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
      socket.read
    end
    with_peer(peer) do |url|
      port = URI(url).port
      allow(Addrinfo).to receive(:getaddrinfo).with("multi.test", port, nil, :STREAM, nil, 0, timeout: Numeric)
                                              .and_return([refused, Addrinfo.tcp("127.0.0.1", port)])
      expect(ConnectRPC::Transports::NetHTTP.new(proxy: nil).call(transport_request("http://multi.test:#{port}")).body)
        .to eq("ok")
    end
    expect(Array.new(sockets.size) { sockets.pop }.map(&:closed?)).to eq([true, true])
  ensure
    reserved&.close
  end

  it "reports the first failure when every address fails" do
    reserved = Socket.new(:INET, :STREAM)
    reserved.bind(Addrinfo.tcp("127.0.0.1", 0))
    allow(Addrinfo).to receive(:getaddrinfo).and_return([reserved.local_address, reserved.local_address])
    request = transport_request("http://multi.test:#{reserved.local_address.ip_port}")
    transport = ConnectRPC::Transports::NetHTTP.new(proxy: nil)
    expect { transport.call(request) }.to raise_error(ConnectRPC::Error) do |error|
      expect([error.message, error.cause.class]).to eq(["HTTP transport failed: connection refused", Errno::ECONNREFUSED])
    end
  ensure
    reserved&.close
  end

  it "still closes an owned socket whose shutdown fails before it connected" do
    source = ConnectRPC::CancellationSource.new
    sockets = ConnectRPC::Transports::NetHTTP::SocketLifecycle.new(source.token)
    socket = Socket.new(:INET, :STREAM)
    sockets.attach(socket)
    expect { socket.shutdown(Socket::SHUT_RDWR) }.to raise_error(Errno::ENOTCONN)
    source.cancel
    expect(socket).to be_closed
    late = Socket.new(:INET, :STREAM)
    sockets.attach(late)
    expect(late).to be_closed
  ensure
    sockets&.retire
    socket&.close unless socket&.closed?
    late&.close unless late&.closed?
  end
end
