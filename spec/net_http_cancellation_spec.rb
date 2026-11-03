# frozen_string_literal: true

require "connectrpc/transports/net_http"
require_relative "support/socket_peer"

RSpec.describe "NetHTTP explicit cancellation" do
  include SocketPeer

  let(:method) do
    ConnectRPC::Method.new(service: "example.Cancel", name: "Call", request_type: String, response_type: String)
  end
  let(:binding) do
    Class.new do
      def encode(message, **) = message
      def decode(bytes, **) = bytes
    end.new
  end

  def client(url, transport = ConnectRPC::Transports::NetHTTP.new, **)
    ConnectRPC::Client.new(base_url: url, binding: binding, transport: transport, **)
  end

  %i[headers body write tls].product([nil, 30]).each do |phase, timeout|
    it "interrupts actual #{phase} I/O with timeout #{timeout.inspect} and cleans its socket without retry" do
      source = ConnectRPC::CancellationSource.new
      progress = Queue.new
      release = Queue.new
      peer = lambda do |socket|
        if %i[tls write].include?(phase)
          expect(socket.readpartial(1024)).not_to be_empty
        else
          read_request(socket)
          socket.write("HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nx") if phase == :body
        end
        progress << true
        release.pop if phase == :write
        # TLS ClientHello bytes and a blocked large write may still be queued.
        socket.read
      end
      with_peer(peer) do |url, listener|
        url = url.sub("http:", "https:") if phase == :tls
        message = phase == :write ? "x" * (32 * 1024 * 1024) : "request"
        caller = Thread.new do
          client(url, request_limit: 40 * 1024 * 1024).call(method, message, timeout: timeout,
                                                                             cancellation: source.token)
        rescue ConnectRPC::Error => e
          e
        end
        expect(progress.pop(timeout: 2)).to be(true)
        expect(source.cancel).to be(true)
        expect(caller.join(2)).to equal(caller)
        expect([caller.value.code, caller.value.cause]).to eq(["canceled", nil])
        release << true
        expect(listener.wait_readable(0)).to be_nil
      ensure
        release << true
        caller&.join(2)
      end
    end
  end

  it "cooperates at native TCP return and sends no late request" do
    source = ConnectRPC::CancellationSource.new
    transport = ConnectRPC::Transports::NetHTTP.new
    closed = Queue.new
    peer = lambda do |socket|
      expect(socket.read).to eq("")
      closed << true
    end
    allow(transport).to receive(:connection).and_wrap_original do |original, *args|
      http = original.call(*args)
      allow(http).to receive(:start).and_wrap_original do |start|
        start.call
        source.cancel
        http
      end
      http
    end
    with_peer(peer) do |url, listener|
      expect { client(url, transport).call(method, "request", timeout: nil, cancellation: source.token) }
        .to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("canceled") }
      expect(closed.pop(timeout: 2)).to be(true)
      expect(listener.wait_readable(0)).to be_nil
    end
  end

  it "retires socket access before a late cancellation and leaves unrelated calls healthy" do
    source = ConnectRPC::CancellationSource.new
    peer = lambda do |socket|
      read_request(socket)
      socket.write("HTTP/1.1 200 OK\r\nContent-Type: application/proto\r\nContent-Length: 2\r\n\r\nok")
      expect(socket.read).to eq("")
    end
    transport = ConnectRPC::Transports::NetHTTP.new
    2.times do
      with_peer(peer) do |url|
        expect(client(url, transport).call(method, "request", timeout: nil,
                                                              cancellation: source.token).message).to eq("ok")
      end
    end
    expect(source.instance_variable_get(:@signal).instance_variable_get(:@registrations)).to be_empty
    source.cancel
    with_peer(peer) { |url| expect(client(url, transport).call(method, "request").message).to eq("ok") }
  end
end

RSpec.describe "Owned socket retirement" do
  it "guards an already-running cancellation callback after unregister can no longer suppress it" do
    source = ConnectRPC::CancellationSource.new
    left, right = Socket.pair(:UNIX, :STREAM, 0)
    sockets = ConnectRPC::Transports::NetHTTP::SocketLifecycle.new(source.token)
    sockets.attach(left)
    entered = Queue.new
    release = Queue.new
    allow(sockets).to receive(:close).and_wrap_original do |close|
      entered << true
      release.pop
      close.call
    end
    worker = Thread.new { source.cancel }
    expect(entered.pop(timeout: 1)).to be(true)
    sockets.retire
    release << true
    expect(worker.join(1)).to equal(worker)
    expect(left).not_to be_closed
    left.write("still owned by caller")
    expect(right.readpartial(100)).to eq("still owned by caller")
  ensure
    release&.push(true)
    worker&.join(1)
    left&.close unless left&.closed?
    right&.close unless right&.closed?
  end
end

RSpec.describe "Direct NetHTTP cancellation and deadline composition" do
  include SocketPeer

  it "keeps a real deadline authoritative during I/O with a plain live token" do
    progress = Queue.new
    peer = lambda do |socket|
      read_request(socket)
      progress << true
      expect(socket.read).to eq("")
    end
    with_peer(peer) do |url|
      source = ConnectRPC::CancellationSource.new
      outgoing = transport_request(url, timeout: 0.2)
      outgoing.cancellation = source.token
      # Elapsed time is the actual external socket deadline, not a readiness delay.
      expect { ConnectRPC::Transports::NetHTTP.new.call(outgoing) }.to raise_error(ConnectRPC::Error) do |error|
        expect([error.code, error.cause]).to eq(["deadline_exceeded", nil])
      end
      expect(progress.pop(timeout: 1)).to be(true)
      expect(outgoing.cancellation).to equal(source.token)
      expect(source.instance_variable_get(:@signal).instance_variable_get(:@registrations)).to be_empty
    end
  end

  it "rejects an already-expired direct request before connecting even with a live token" do
    now = 0.0
    deadline = ConnectRPC::Deadline.new(1, clock: -> { now })
    source = ConnectRPC::CancellationSource.new
    outgoing = transport_request("http://example.invalid")
    outgoing.deadline = deadline
    outgoing.cancellation = source.token
    now = 2.0
    transport = ConnectRPC::Transports::NetHTTP.new
    allow(transport).to receive(:connection).and_raise(IOError, "connection attempted")
    expect { transport.call(outgoing) }.to raise_error(ConnectRPC::Error) do |error|
      expect(error.code).to eq("deadline_exceeded")
    end
    expect(transport).not_to have_received(:connection)
  end
end
