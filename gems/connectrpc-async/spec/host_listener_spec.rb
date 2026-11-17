# frozen_string_literal: true

require "spec_helper"

RSpec.describe ConnectRPC::Async::Host do
  def two_listeners(host)
    bounds = 2.times.map { IO::Endpoint.tcp("127.0.0.1", 0).bound }
    bound = Struct.new(:sockets, :bounds) do
      def close = bounds.each(&:close)
    end.new(bounds.flat_map(&:sockets), bounds)
    host.define_singleton_method(:bind) { bound }
    bound.sockets
  end

  # A peer the kernel completed but no listener has taken yet. Async runs
  # IO.select on a thread; a zero timeout never waits beside the accept fiber.
  def queued?(listener)
    !IO.select([listener], nil, nil, 0).nil? # rubocop:disable Lint/IncompatibleIoSelectWithFiberScheduler
  end

  def reset?(raw)
    raw.read_nonblock(1, exception: false).nil?
  rescue Errno::ECONNRESET
    true
  end

  # One Connect unary call over exactly this TCP connection, so a reconnect
  # cannot hide a reset.
  def echo_on(raw, port, name)
    connection = Async::HTTP::Protocol::HTTP2.client(raw)
    body = AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new(name: name))
    request = Protocol::HTTP::Request["POST", "/example.v1.EchoService/Echo",
                                      { "content-type" => "application/proto" }, [body],
                                      scheme: "http", authority: "127.0.0.1:#{port}"]
    response = connection.call(request)
    [response.status, AsyncHelpers::MESSAGE.decode(response.read).name]
  ensure
    connection&.close
  end

  def record_admissions(host)
    outcomes = []
    allow(host_session(host)).to receive(:accept).and_wrap_original do |accept, raw|
      accept.call(raw).tap { |accepted| outcomes << !accepted.nil? }
    end
    outcomes
  end

  def live(host) = host_session(host).instance_variable_get(:@resources)

  it "leaves a peer above max_connections queued, then serves it on the same connection once a slot frees" do
    reactor do |task|
      host = described_class.new(server: server, max_connections: 1, idle_timeout: nil)
      admissions = record_admissions(host)
      host.start
      listener = host.instance_variable_get(:@bound).sockets.first
      silent = IO::Endpoint.tcp("127.0.0.1", host.port).connect
      wait_until(task, "the silent peer holding the only slot") { live(host).size == 1 }
      waiting = IO::Endpoint.tcp("127.0.0.1", host.port).connect
      # A listener either leaves the peer queued in the backlog or takes it.
      wait_until(task, "the second peer queued or reset") { queued?(listener) || reset?(waiting) }
      expect(reset?(waiting)).to be(false)
      expect(live(host).size).to eq(1)
      silent.close
      expect(echo_on(waiting, host.port, "queued")).to eq([200, "queued"])
      expect(admissions).to eq([true, true])
    ensure
      AsyncHelpers::INTERNAL.raw_close(silent)
      AsyncHelpers::INTERNAL.raw_close(waiting)
      host.close(grace: 0)
    end
  end

  it "serves a peer queued only on a second listener once the other listener's peer frees the slot" do
    reactor do |task|
      host = described_class.new(server: server, max_connections: 1, idle_timeout: nil)
      first, second = two_listeners(host)
      admissions = record_admissions(host)
      host.start
      silent = IO::Endpoint.tcp("127.0.0.1", first.local_address.ip_port).connect
      wait_until(task, "the first listener's peer holding the only slot") { live(host).size == 1 }
      port = second.local_address.ip_port
      waiting = IO::Endpoint.tcp("127.0.0.1", port).connect
      wait_until(task, "the second listener's peer queued or reset") { queued?(second) || reset?(waiting) }
      expect(reset?(waiting)).to be(false)
      silent.close
      expect(echo_on(waiting, port, "second listener")).to eq([200, "second listener"])
      expect(admissions).to eq([true, true])
    ensure
      AsyncHelpers::INTERNAL.raw_close(silent)
      AsyncHelpers::INTERNAL.raw_close(waiting)
      host.close(grace: 0)
    end
  end

  it "binds every listener close-on-exec" do
    reactor do
      host = described_class.new(server: server, host: "localhost").start
      sockets = host.instance_variable_get(:@bound).sockets
      expect(sockets).not_to be_empty
      expect(sockets).to all(satisfy(&:close_on_exec?))
    ensure
      host&.close(grace: 0)
    end
  end

  context "with several resolved addresses" do
    # Linux routes all of 127.0.0.0/8 to loopback, so both addresses bind.
    let(:addresses) { %w[127.0.0.1 127.0.0.2] }

    before do
      allow(Addrinfo).to receive(:getaddrinfo).and_wrap_original do |original, name, port, *rest, **options|
        if name == "multi.test"
          addresses.map { |address| Addrinfo.tcp(address, port) }
        else
          original.call(name, port, *rest, **options)
        end
      end
    end

    def bound_sockets
      sockets = []
      allow(IO::Endpoint::Wrapper.default).to receive(:bind).and_wrap_original do |bind, address, **options|
        yield address if block_given?
        bind.call(address, **options).tap { |socket| sockets << socket }
      end
      sockets
    end

    it "binds every address on the one port the kernel chose for the first" do
      reactor do
        host = described_class.new(server: server, host: "multi.test", port: 0).start
        expect(host.addresses.map(&:ip_address)).to eq(addresses)
        expect(host.addresses.map(&:ip_port)).to all(eq(host.port))
        addresses.each do |address|
          transport = ConnectRPC::Async::Transport.new(origin: "http://#{address}:#{host.port}")
          expect(client(transport, "http://#{address}:#{host.port}")
            .echo(AsyncHelpers::MESSAGE.new(name: address)).message.name).to eq(address)
        ensure
          transport&.close(grace: 0)
        end
      ensure
        host&.close(grace: 0)
      end
    end

    it "restarts the whole bind when a later address already uses the chosen port" do
      reactor do
        collisions = 2
        sockets = bound_sockets do |address|
          if address.ip_address == addresses.last && collisions.positive?
            collisions -= 1
            raise Errno::EADDRINUSE, "chosen port taken"
          end
        end
        host = described_class.new(server: server, host: "multi.test", port: 0).start
        expect(host.addresses.map(&:ip_port)).to all(eq(host.port))
        expect(sockets.size).to eq(4)
        expect(sockets.first(2)).to all(be_closed)
        expect(sockets.last(2)).to all(satisfy { |socket| !socket.closed? })
      ensure
        host&.close(grace: 0)
      end
    end

    it "closes what it bound and raises when every bind attempt collides" do
      reactor do
        sockets = bound_sockets do |address|
          raise Errno::EADDRINUSE, "always taken" if address.ip_address == addresses.last
        end
        host = described_class.new(server: server, host: "multi.test", port: 0)
        expect { host.start }.to raise_error(Errno::EADDRINUSE)
        expect(sockets.size).to eq(5)
        expect(sockets).to all(be_closed)
        expect(host.wait).to be_nil
      end
    end

    it "closes the bound listeners and raises at once when a fixed-port bind fails" do
      reactor do
        sockets = bound_sockets do |address|
          raise Errno::EADDRNOTAVAIL, "not local" if address.ip_address == addresses.last
        end
        port = TCPServer.open("127.0.0.1", 0).then do |probe|
          probe.local_address.ip_port.tap { probe.close }
        end
        host = described_class.new(server: server, host: "multi.test", port: port)
        expect { host.start }.to raise_error(Errno::EADDRNOTAVAIL)
        expect(sockets.size).to eq(1)
        expect(sockets).to all(be_closed)
      end
    end
  end
end
