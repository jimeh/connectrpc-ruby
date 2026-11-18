# frozen_string_literal: true

require "connectrpc/transports/net_http"
require_relative "support/socket_peer"

RSpec.describe "NetHTTP owned dialing" do
  include SocketPeer

  # Linux drops SYNs once a listen(0) queue holds one unaccepted connection,
  # so the next connect stays pending without leaving loopback.
  def with_full_backlog(host = "127.0.0.1")
    address = Addrinfo.tcp(host, 0)
    listener = Socket.new(address.afamily, :STREAM)
    listener.bind(address)
    listener.listen(0)
    filler = Socket.new(address.afamily, :STREAM)
    filler.connect(listener.local_address)
    yield listener.local_address.ip_port
  ensure
    filler&.close
    listener&.close
  end

  def with_blackholes(&)
    skip "IPv6 loopback unavailable" unless Socket.ip_address_list.any?(&:ipv6_loopback?)
    with_full_backlog("::1") do |v6|
      with_full_backlog { |v4| yield Addrinfo.tcp("::1", v6), Addrinfo.tcp("127.0.0.1", v4) }
    end
  end

  def resolve(name, addresses)
    allow(Addrinfo).to receive(:getaddrinfo).with(name, Integer, nil, :STREAM, nil, 0, timeout: Numeric)
                                            .and_return(addresses)
  end

  def answer
    lambda do |socket|
      read_request(socket)
      socket.write("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
      socket.read
    end
  end

  def elapsed
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    yield
    Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
  end

  def drain(queue) = Array.new(queue.size) { queue.pop }

  def outcome
    yield.body
  rescue ConnectRPC::Error => e
    e.code
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

  # Elapsed-time bounds below are upper limits on the behaviour under test:
  # a serial dial would spend the whole deadline on the blackholed address.
  it "starts the other address family after the fallback delay when the first stalls" do
    with_blackholes do |stalled, _|
      sockets = Queue.new
      allow(Socket).to receive(:new).and_wrap_original do |original, family, *args|
        original.call(family, *args).tap { |socket| sockets << [family, socket] }
      end
      with_peer(answer) do |url|
        port = URI(url).port
        resolve("dual.test", [stalled, Addrinfo.tcp("127.0.0.1", port)])
        transport = ConnectRPC::Transports::NetHTTP.new(proxy: nil)
        result = nil
        time = elapsed { result = outcome { transport.call(transport_request("http://dual.test:#{port}", timeout: 5)) } }
        expect([result, time < 2]).to eq(["ok", true])
      end
      expect(drain(sockets).map { |family, socket| [family, socket.closed?] })
        .to eq([[Socket::AF_INET6, true], [Socket::AF_INET, true]])
    end
  end

  it "starts the other family at once when the first family is refused" do
    skip "IPv6 loopback unavailable" unless Socket.ip_address_list.any?(&:ipv6_loopback?)
    reserved = Socket.new(:INET6, :STREAM)
    reserved.bind(Addrinfo.tcp("::1", 0))
    stub_const("ConnectRPC::Transports::NetHTTP::Dialer::FALLBACK_DELAY", 30)
    with_peer(answer) do |url|
      port = URI(url).port
      resolve("dual.test", [reserved.local_address, Addrinfo.tcp("127.0.0.1", port)])
      transport = ConnectRPC::Transports::NetHTTP.new(proxy: nil)
      expect(outcome { transport.call(transport_request("http://dual.test:#{port}", timeout: 5)) }).to eq("ok")
    end
  ensure
    reserved&.close
  end

  [
    ["moves on after an equal share of the deadline", 0.1, 1.5, "ok"],
    ["gives one attempt at least the minimum share", 2.0, 0.4, "deadline_exceeded"]
  ].each do |label, minimum, timeout, expected|
    it "#{label} for same-family addresses" do
      stub_const("ConnectRPC::Transports::NetHTTP::Dialer::MIN_ATTEMPT", minimum)
      with_full_backlog do |stalled_port|
        sockets = track_sockets
        stalled = Addrinfo.tcp("127.0.0.1", stalled_port)
        with_peer(answer) do |url, listener|
          port = URI(url).port
          resolve("multi.test", [stalled, stalled, Addrinfo.tcp("127.0.0.1", port)])
          transport = ConnectRPC::Transports::NetHTTP.new(proxy: nil)
          expect(outcome { transport.call(transport_request("http://multi.test:#{port}", timeout: timeout)) })
            .to eq(expected)
          listener.close if expected != "ok"
        end
        expect(drain(sockets).map(&:closed?)).to eq(expected == "ok" ? [true, true, true] : [true])
      end
    end
  end

  describe "with both families in flight" do
    before { stub_const("ConnectRPC::Transports::NetHTTP::Dialer::FALLBACK_DELAY", 0) }

    def selecting
      selecting = Queue.new
      allow(IO).to receive(:select).and_wrap_original do |original, *args|
        selecting << args[1].size if args[1]
        original.call(*args)
      end
      selecting
    end

    # The selecting signal fires just before IO.select; a close that lands
    # before the call fails fast, so wait until the thread actually blocks.
    def blocked(thread)
      limit = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 2
      Thread.pass until thread.status == "sleep" || Process.clock_gettime(Process::CLOCK_MONOTONIC) > limit
      thread.status
    end

    %i[deadline cancellation interruption].each do |ending|
      it "closes both attempts on #{ending}" do
        with_blackholes do |v6, v4|
          sockets = track_sockets
          waits = selecting
          resolve("dual.test", [v6, v4])
          source = ConnectRPC::CancellationSource.new
          request = transport_request("http://dual.test:80", timeout: ending == :deadline ? 0.5 : 30)
          if ending == :cancellation
            request.deadline = nil
            request.cancellation = source.token
          end
          interrupted = Class.new(StandardError)
          caller = Thread.new do
            ConnectRPC::Transports::NetHTTP.new(proxy: nil).call(request)
          rescue ConnectRPC::Error, interrupted => e
            e
          end
          expect(waits.pop(timeout: 2)).to eq(2)
          expect(blocked(caller)).to eq("sleep")
          source.cancel if ending == :cancellation
          caller.raise(interrupted) if ending == :interruption
          expect(caller.join(2)).to equal(caller)
          outcome = caller.value
          expect(outcome.is_a?(interrupted) ? :interruption : outcome.code)
            .to eq({ deadline: "deadline_exceeded", cancellation: "canceled" }.fetch(ending, ending))
          expect(drain(sockets).map(&:closed?)).to eq([true, true])
        ensure
          caller&.join(2)
        end
      end
    end
  end

  it "reports deadline_exceeded when a thread switch lands between the attempt's deadline and clock reads" do
    with_full_backlog do |port|
      # The sleep stands in for a scheduler delay inside the attempt budget
      # calculation; it widens that window rather than waiting for progress.
      allow_any_instance_of(ConnectRPC::Transports::NetHTTP::Dialer).to receive(:budget)
        .and_wrap_original do |original, *args|
          sleep 0.05
          original.call(*args)
        end
      request = transport_request("http://127.0.0.1:#{port}", timeout: 0.3)
      expect(outcome { ConnectRPC::Transports::NetHTTP.new(proxy: nil).call(request) }).to eq("deadline_exceeded")
    end
  end

  it "closes a single pending attempt when an outer interruption lands during its wait" do
    with_full_backlog do |port|
      sockets = track_sockets
      waiting = Queue.new
      allow_any_instance_of(Socket).to receive(:wait_writable).and_wrap_original do |original, *args|
        waiting << true
        original.call(*args)
      end
      interrupted = Class.new(StandardError)
      caller = Thread.new do
        ConnectRPC::Transports::NetHTTP.new(proxy: nil).call(transport_request("http://127.0.0.1:#{port}", timeout: 30))
      rescue interrupted => e
        e
      end
      expect(waiting.pop(timeout: 2)).to be(true)
      caller.raise(interrupted)
      expect(caller.join(2)).to equal(caller)
      expect(caller.value).to be_a(interrupted)
      expect(drain(sockets).map(&:closed?)).to eq([true])
    ensure
      caller&.join(2)
    end
  end

  it "tries same-family addresses in order and keeps the first that connects" do
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
