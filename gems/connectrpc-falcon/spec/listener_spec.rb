# frozen_string_literal: true

require "async/service/configuration"
require_relative "support/http1_wire"

RSpec.describe "Falcon native listener" do
  def rack = ->(_env) { [200, { "content-type" => "text/plain", "content-length" => "2" }, ["ok"]] }

  # One keep-alive HTTP/1.1 request on exactly this connection.
  def get(wire)
    wire.write("GET /ok HTTP/1.1\r\nHost: localhost\r\n\r\n")
    status, fields = wire.head
    [status, wire.socket.read(Integer(fields.fetch("content-length").first))]
  end

  def listener(host) = host.endpoint.sockets.first
  def live(host) = host.instance_variable_get(:@session).instance_variable_get(:@resources)

  # A completed peer no worker has taken yet. Async runs IO.select on a
  # thread; a zero timeout never waits beside the accept fiber.
  def queued?(socket)
    !IO.select([socket], nil, nil, 0).nil? # rubocop:disable Lint/IncompatibleIoSelectWithFiberScheduler
  end

  def reset?(wire)
    wire.raw.read_nonblock(1, exception: false).nil?
  rescue Errno::ECONNRESET
    true
  end

  describe "close-on-exec" do
    it "binds parsed, owned and managed parent listeners close-on-exec unless the caller says otherwise" do
      reactor do
        parsed = ConnectRPC::Falcon::Endpoint.parse("http://127.0.0.1:0")
        bound = parsed.bound
        expect(bound.sockets).to all(satisfy(&:close_on_exec?))
        inherited = parsed.bound(close_on_exec: false)
        expect(inherited.sockets).to all(satisfy { |socket| !socket.close_on_exec? })
        owned = ConnectRPC::Falcon::Server.new(server: server, endpoint: parsed)
        expect(owned.instance_variable_get(:@owned_bound).sockets).to all(satisfy(&:close_on_exec?))
        core = server
        configuration = Async::Service::Configuration.build do
          service "close-on-exec" do
            include ConnectRPC::Falcon::Environment

            url "http://127.0.0.1:0"
            connectrpc_server { core }
          end
        end
        service = ConnectRPC::Falcon::Service.new(configuration.environments.fetch(0))
        service.start
        managed = service.instance_variable_get(:@listener).endpoint
        expect(managed.sockets).not_to be_empty
        expect(managed.sockets).to all(satisfy(&:close_on_exec?))
      ensure
        bound&.close
        inherited&.close
        owned&.close(grace: 0)
        service&.stop
      end
    end
  end

  it "leaves a peer above max_connections queued, then serves it on the same connection" do
    reactor do |task|
      falcon(rack_app: rack, max_connections: 1) do |host, origin|
        idle = ConnectH1Wire.new(origin)
        expect(get(idle)).to eq([200, "ok"])
        waiting = ConnectH1Wire.new(origin)
        # A full worker either leaves the peer queued in the backlog or takes it.
        wait_until(task, "the second peer queued or reset") { queued?(listener(host)) || reset?(waiting) }
        expect(reset?(waiting)).to be(false)
        expect(live(host).size).to eq(1)
        idle.close
        expect(get(waiting)).to eq([200, "ok"])
      ensure
        idle&.close
        waiting&.close
      end
    end
  end

  it "retries 20 consecutive EMFILE accept failures with capped backoff while serving established peers" do
    reactor do |task|
      falcon(rack_app: rack) do |host, origin|
        entered = Async::Queue.new
        release = Async::Queue.new
        host.instance_variable_get(:@accept_wrapper).define_singleton_method(:accept_backoff) do |delay|
          entered.enqueue(delay)
          release.dequeue
        end
        # Disarmed until the established peer has been accepted.
        injected = 20
        fault = lambda do
          if injected < 20
            injected += 1
            raise Errno::EMFILE, "accept #{injected}"
          end
        end
        socket = listener(host)
        # Inject wherever the listener takes a queued peer.
        %i[accept accept_nonblock].each do |name|
          original = socket.method(name)
          socket.define_singleton_method(name) do |*arguments, **options|
            fault.call
            original.call(*arguments, **options)
          end
        end
        established = ConnectH1Wire.new(origin)
        expect(get(established)).to eq([200, "ok"])
        injected = 0
        delays = Array.new(20) { |index| [0.005 * (2**index), 1.0].min }
        expect do
          later = task.async do
            wire = ConnectH1Wire.new(origin)
            get(wire)
          ensure
            wire&.close
          end
          delays.each do |delay|
            expect(entered.dequeue).to eq(delay)
            expect(get(established)).to eq([200, "ok"])
            release.enqueue(true)
          end
          expect(later.wait).to eq([200, "ok"])
        end.to output(delays.map { |delay| "ConnectRPC accept error: Errno::EMFILE; retrying in #{delay}s\n" }.join)
          .to_stderr
        expect(injected).to eq(20)
      ensure
        established&.close
      end
    end
  end
end
