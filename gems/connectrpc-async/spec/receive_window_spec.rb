# frozen_string_literal: true

require_relative "support/ingress_peer"

RSpec.describe "HTTP/2 receive windows" do
  include IngressPeer

  def window_host(app, **)
    # Barrier PINGs outpace grpc-go's enforcement without server writes between them.
    host = ConnectRPC::Async::Host.new(server: server, keepalive: ConnectRPC::Async::ServerKeepalive.new(min_time: nil),
                                       **)
    host.start
    host_session(host).instance_variable_get(:@application).instance_variable_set(:@fallback, app)
    yield "http://127.0.0.1:#{host.port}"
  ensure
    host&.close(grace: 0)
  end

  def open_request(connection, origin, path)
    response = connection.create_response
    response.instance_variable_set(:@request, Protocol::HTTP::Request["POST", path, {}, nil])
    response.stream.send_headers([[":method", "POST"], [":scheme", "http"],
                                  [":authority", URI(origin).authority], [":path", path]])
    response
  end

  it "advertises the client's stream window and grows its connection window at the preface" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      listener = bound.sockets.first
      peers = Async::Queue.new
      done = Async::Promise.new
      peer = task.async do
        socket, = listener.accept
        connection = Async::HTTP::Protocol::HTTP2::Server.new(IO::Stream(socket))
        wire = observe_ingress_wire(connection)
        connection.read_connection_preface(AsyncHelpers::INTERNAL.settings(128))
        connection.read_in_background(parent: task)
        incoming = connection.requests.dequeue
        peers.enqueue([connection, wire, incoming.stream])
        incoming.stream.send_headers([[":status", "200"]])
        # Raw frames spend the whole stream window without waiting for credit.
        16.times { ingress_data(connection, incoming.stream.id, "a" * 16_384) }
        ingress_data(connection, incoming.stream.id, "", finish: true)
        done.wait
      ensure
        AsyncHelpers::INTERNAL.raw_close(socket)
        connection&.close
      end
      origin = "http://127.0.0.1:#{listener.local_address.ip_port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin, stream_receive_window: 262_144)
      call = task.async { transport.call(request("#{origin}/large")) }
      connection, wire, stream = peers.dequeue
      expect(wire[:credit][0]).to eq(16_777_216 - 65_535)
      expect(connection.remote_settings.initial_window_size).to eq(262_144)
      expect(stream.remote_window.capacity).to eq(262_144)
      expect(call.wait.body.bytesize).to eq(262_144)
      expect(wire[:resets]).to be_empty
      done.resolve(true)
    ensure
      call&.stop
      transport&.close(grace: 0)
      done&.resolve(true) unless done&.resolved?
      peer&.wait
      bound&.close
    end
  end

  it "grows the host's connection window and keeps that target after the client acknowledges SETTINGS" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Promise.new
      app = lambda do |request|
        entered.enqueue(request.path)
        release.wait
        Protocol::HTTP::Response[200, {}, ["held"]]
      end
      window_host(app, connection_receive_window: 262_144) do |origin|
        with_ingress_client(task, origin, :h2c) do |connection, wire|
          grown = 262_144 - 65_535
          expect(wire[:credit][0]).to eq(grown)
          held = Array.new(3) do
            response = open_request(connection, origin, "/hold")
            entered.dequeue
            4.times { ingress_data(connection, response.stream.id, "a" * 16_384) }
            response
          end
          ingress_barrier(task, connection, wire)
          # 192 KiB arrived on three unread streams, which withhold their own
          # credit. The connection still replenishes, since its target stays
          # 256 KiB; protocol-http2 alone would retarget it to the 64 KiB
          # stream window on SETTINGS ACK and return nothing here.
          expect(held.map { |response| wire[:credit][response.stream.id] }).to eq([0, 0, 0])
          expect(wire[:credit][0] - grown).to be >= 65_536
          release.resolve(true)
          held.each do |response|
            ingress_data(connection, response.stream.id, "", finish: true)
            connection.read_response(response)
            expect(response.read).to eq("held")
          end
        end
      end
    end
  end

  it "accepts a full larger stream window from a peer that has not yet acknowledged SETTINGS" do
    reactor do |task|
      app = lambda do |request|
        Protocol::HTTP::Response[200, {}, [request.body.join.bytesize.to_s]]
      end
      window_host(app, stream_receive_window: 262_144) do |origin|
        raw = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
        connection = Async::HTTP::Protocol::HTTP2::Client.new(IO::Stream(raw))
        held_acks = []
        connection.define_singleton_method(:write_frame) do |frame|
          next held_acks << frame if frame.is_a?(Protocol::HTTP2::SettingsFrame) && frame.acknowledgement?

          super(frame)
        end
        wire = observe_ingress_wire(connection)
        connection.send_connection_preface(AsyncHelpers::INTERNAL.settings(128))
        connection.read_in_background(parent: task)
        ingress_barrier(task, connection, wire)
        expect(held_acks.size).to eq(1)
        expect(connection.remote_settings.initial_window_size).to eq(262_144)
        response = open_request(connection, origin, "/upload")
        16.times { ingress_data(connection, response.stream.id, "a" * 16_384) }
        ingress_data(connection, response.stream.id, "", finish: true)
        ingress_barrier(task, connection, wire)
        expect(wire[:resets]).to be_empty
        held_acks.each { |frame| connection.method(:write_frame).super_method.call(frame) }
        connection.read_response(response)
        expect(response.read).to eq("262144")
      ensure
        AsyncHelpers::INTERNAL.raw_close(raw) if raw
        connection&.close
      end
    end
  end

  it "passes the window options from ThreadedTransport to its owner's connections" do
    observed = Thread::Queue.new
    hosts = []
    # The handler runs on the host's reactor thread, which owns the connection.
    peer = threaded_host(server: server(echo: lambda { |message, _context|
      connection = host_session(hosts.first).instance_variable_get(:@resources).first.connection
      observed << [connection.remote_settings.initial_window_size, connection.remote_window.available]
      message
    }))
    hosts << peer.peer
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin, stream_receive_window: 131_072, connection_receive_window: 262_144,
                         max_receive_window: 196_608)
    expect(client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "w")).message.name).to eq("w")
    # No response DATA has been sent yet, so the host's send window is the
    # client's whole connection window.
    expect(pop_within(observed, "the handler's view of the client windows")).to eq([131_072, 262_144])
    _, limits = on_owner(transport) do
      engine_of(transport).inner.instance_variable_get(:@resources).map do |resource|
        resource.connection.instance_variable_get(:@bandwidth).instance_variable_get(:@limit)
      end
    end
    expect(limits).to eq([196_608])
    static = threaded(origin: origin, dynamic_receive_windows: false)
    expect(client(static, origin).echo(AsyncHelpers::MESSAGE.new(name: "s")).message.name).to eq("s")
    _, estimators = on_owner(static) do
      engine_of(static).inner.instance_variable_get(:@resources).map do |resource|
        resource.connection.instance_variable_get(:@bandwidth)
      end
    end
    expect(estimators).to eq([nil])
  ensure
    transport&.close(grace: 0)
    static&.close(grace: 0)
    peer&.stop
  end

  it "defaults client stream windows to 4 MiB and keeps the host's at 64 KiB" do
    calls = {
      "Transport" => lambda do |origin|
        reactor do
          transport = ConnectRPC::Async::Transport.new(origin: origin)
          client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "w")).message.name
        ensure
          transport&.close(grace: 0)
        end
      end,
      "ThreadedTransport" => lambda do |origin|
        transport = threaded(origin: origin)
        client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "w")).message.name
      ensure
        transport&.close(grace: 0)
      end
    }
    calls.each do |name, call|
      observed = Thread::Queue.new
      hosts = []
      peer = threaded_host(server: server(echo: lambda { |message, _context|
        connection = host_session(hosts.first).instance_variable_get(:@resources).first.connection
        # The client's advertised window and the host's own, as sent.
        observed << [connection.remote_settings.initial_window_size,
                     connection.local_settings.pending.initial_window_size]
        message
      }))
      hosts << peer.peer
      origin = "http://127.0.0.1:#{peer.peer.port}"
      expect(call.call(origin)).to eq("w")
      expect(pop_within(observed, "the host's view of the #{name} windows")).to eq([4_194_304, 65_536]), name
    ensure
      peer&.stop
    end
  end

  it "retains at most the default 4 MiB client stream window for an unread response" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      listener = bound.sockets.first
      peers = Async::Queue.new
      peer = task.async do
        socket, = listener.accept
        connection = Async::HTTP::Protocol::HTTP2::Server.new(IO::Stream(socket))
        wire = observe_ingress_wire(connection)
        connection.read_connection_preface(AsyncHelpers::INTERNAL.settings(128))
        connection.read_in_background(parent: task)
        incoming = connection.requests.dequeue
        incoming.stream.send_headers([[":status", "200"]])
        peers.enqueue([connection, wire, incoming.stream])
        Async::Promise.new.wait
      ensure
        AsyncHelpers::INTERNAL.raw_close(socket)
        connection&.close
      end
      origin = "http://127.0.0.1:#{listener.local_address.ip_port}"
      # Static: the raw peer's 4 MiB burst would otherwise grow the window it measures.
      transport = ConnectRPC::Async::Transport.new(origin: origin, dynamic_receive_windows: false)
      outgoing = ConnectRPC::StreamRequest.new(url: URI("#{origin}/stream"), http_method: "POST", headers: {})
      transport.open_stream(outgoing) do |exchange|
        exchange.close_send
        exchange.response_head
        connection, wire, stream = peers.dequeue
        body = exchange.instance_variable_get(:@operation).fetch(:response).stream.instance_variable_get(:@input)
        # Raw frames spend the whole default window while nothing reads.
        256.times { ingress_data(connection, stream.id, "a" * 16_384) }
        ingress_barrier(task, connection, wire)
        expect(ingress_backlog(body)).to eq([4_194_304, 256])
        expect(wire[:credit][stream.id]).to eq(0)
        expect(wire[:resets]).to be_empty
        # One byte past the window resets only this stream.
        ingress_data(connection, stream.id, "a")
        ingress_barrier(task, connection, wire)
        expect(wire[:resets]).to eq(stream.id => Protocol::HTTP2::Error::FLOW_CONTROL_ERROR)
        expect(wire[:goaway]).to be_nil
      end
    ensure
      begin
        transport&.close(grace: 0)
        peer&.stop
        peer&.wait
      ensure
        # A failed peer re-raises from wait; the listener still closes.
        bound&.close
      end
    end
  end

  it "validates both windows on every client and server constructor" do
    reactor do
      constructors = {
        "Transport" => ->(**options) { ConnectRPC::Async::Transport.new(origin: "http://127.0.0.1:1", **options) },
        "ThreadedTransport" => lambda do |**options|
          ConnectRPC::Async::ThreadedTransport.new(origin: "http://127.0.0.1:1", **options)
        end,
        "ServerSession" => ->(**options) { ConnectRPC::Async::ServerSession.new(application: ->(_) {}, **options) },
        "Host" => ->(**options) { ConnectRPC::Async::Host.new(server: server, **options) }
      }
      constructors.each do |name, build|
        %i[stream_receive_window connection_receive_window max_receive_window].each do |option|
          [65_534, 2**31, 65_536.0, "65536", nil].each do |value|
            expect { build.call(option => value) }
              .to raise_error(ArgumentError, "#{option} must be an integer from 65535 to 2147483647"),
                  "#{name} #{option}: #{value.inspect}"
          end
        end
        # The ceiling is validated even when windows stay static.
        expect { build.call(dynamic_receive_windows: false, max_receive_window: 0) }
          .to raise_error(ArgumentError, "max_receive_window must be an integer from 65535 to 2147483647"), name
        [nil, 1, "true", "false"].each do |value|
          expect { build.call(dynamic_receive_windows: value) }
            .to raise_error(ArgumentError, "dynamic_receive_windows must be true or false"),
                "#{name} dynamic_receive_windows: #{value.inspect}"
        end
      end
      built = [ConnectRPC::Async::Transport.new(origin: "http://127.0.0.1:1", stream_receive_window: 65_535,
                                                connection_receive_window: (2**31) - 1, max_receive_window: 65_535),
               ConnectRPC::Async::ServerSession.new(application: ->(_) {}, stream_receive_window: (2**31) - 1,
                                                    connection_receive_window: 65_535,
                                                    dynamic_receive_windows: false,
                                                    max_receive_window: (2**31) - 1)]
      built.first.close(grace: 0)
      built.last.abort
    end
  end
end
