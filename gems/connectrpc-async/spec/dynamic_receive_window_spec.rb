# frozen_string_literal: true

require_relative "support/ingress_peer"

# BDP-driven receive window growth over real connections. Raw protocol-http2
# peers write DATA without flow-control bookkeeping, so each example states
# the window it spends.
RSpec.describe "HTTP/2 dynamic receive windows" do
  include IngressPeer

  let(:bdp_ping) { AsyncHelpers::INTERNAL::BDPEstimator::PING }
  let(:window) { Protocol::HTTP2::Settings::INITIAL_WINDOW_SIZE }
  let(:flow_control_error) { Protocol::HTTP2::Error::FLOW_CONTROL_ERROR }

  def window_host(app, **)
    # Barrier PINGs outpace grpc-go's enforcement without server writes between them.
    host = ConnectRPC::Async::Host.new(server: server, keepalive: ConnectRPC::Async::ServerKeepalive.new(min_time: nil),
                                       **)
    host.start
    host_session(host).instance_variable_get(:@application).instance_variable_set(:@fallback, app)
    yield host, "http://127.0.0.1:#{host.port}"
  ensure
    host&.close(grace: 0)
  end

  def host_connection(host) = host_session(host).instance_variable_get(:@resources).first.connection

  def open_request(connection, origin, path)
    response = connection.create_response
    response.instance_variable_set(:@request, Protocol::HTTP::Request["POST", path, {}, nil])
    response.stream.send_headers([[":method", "POST"], [":scheme", "http"],
                                  [":authority", URI(origin).authority], [":path", path]])
    response
  end

  def await_growth(task, wire, windows)
    ingress_wait(task, "grown windows #{windows}", state: -> { wire }) { ingress_growth(wire) == windows }
  end

  # Holds the next frame the block selects once it is flushed, while its
  # writer still holds the frame guard. The reader is not held: its next
  # buffer refill finds no pending output to flush.
  def hold_after_flush(connection, &select)
    gate = Data.define(:started, :release).new(Async::Queue.new, Async::Queue.new)
    framer = connection.framer
    state = { armed: true, holding: false }
    framer.define_singleton_method(:write_frame) do |frame|
      state[:holding] = true if state[:armed] && select.call(frame)
      super(frame)
    end
    framer.define_singleton_method(:flush) do
      super()
      next unless state[:holding]

      state[:holding] = state[:armed] = false
      gate.started.enqueue(true)
      gate.release.dequeue
    end
    gate
  end

  def bdp_ping?(frame)
    frame.is_a?(Protocol::HTTP2::PingFrame) && !frame.acknowledgement? && frame.unpack == bdp_ping
  end

  it "grows a host stream window from one sample, which the peer spends at once, up to max_receive_window" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Promise.new
      app = lambda do |request|
        entered.enqueue(request.path)
        release.wait
        Protocol::HTTP::Response[200, {}, [request.path == "/first" ? request.body.join.bytesize.to_s : "ok"]]
      end
      window_host(app, max_receive_window: 131_072) do |_host, origin|
        with_ingress_client(task, origin, :h2c) do |connection, wire|
          first = open_request(connection, origin, "/first")
          entered.dequeue
          ingress_burst(connection, [first.stream.id, 65_536])
          await_growth(task, wire, [131_072])
          # Pending connection credit precedes the PING, as in grpc-go. The
          # grown stream window is below the 16 MiB connection window, so no
          # connection WINDOW_UPDATE comes with its SETTINGS.
          control = [[:window_update, 16_777_216 - 65_535], [:window_update, 16_384], [:ping, bdp_ping],
                     [:settings, [[window, 131_072]]]]
          expect(wire[:control].drop(1)).to eq(control)
          second = open_request(connection, origin, "/second")
          entered.dequeue
          # The open stream spends its increase and the new stream the whole
          # grown window, without stream credit.
          ingress_burst(connection, [first.stream.id, 65_536], [second.stream.id, 131_072])
          ingress_barrier(task, connection, wire)
          expect(wire[:resets]).to be_empty
          expect(wire[:credit].slice(first.stream.id, second.stream.id)).to be_empty
          # At the ceiling, DATA starts no sample: no more PINGs or SETTINGS.
          expect(wire[:control].drop(1)).to eq(control)
          # One byte past the ceiling resets only that stream.
          ingress_data(connection, second.stream.id, "a")
          ingress_barrier(task, connection, wire)
          expect(wire[:resets]).to eq(second.stream.id => flow_control_error)
          expect(wire[:goaway]).to be_nil
          release.resolve(true)
          ingress_data(connection, first.stream.id, "", finish: true)
          connection.read_response(first)
          expect(first.read).to eq("131072")
        end
      end
    end
  end

  it "grows the host's connection window first when the estimate exceeds it" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Promise.new
      app = lambda do |request|
        entered.enqueue(request.path)
        release.wait
        Protocol::HTTP::Response[200, {}, ["ok"]]
      end
      window_host(app, stream_receive_window: 65_535, connection_receive_window: 65_535,
                       max_receive_window: 131_070) do |host, origin|
        with_ingress_client(task, origin, :h2c) do |connection, wire|
          first = open_request(connection, origin, "/first")
          entered.dequeue
          ingress_burst(connection, [first.stream.id, 65_535])
          await_growth(task, wire, [131_070])
          # The connection increment comes before the SETTINGS that needs it.
          expect(wire[:control].last(2)).to eq([[:window_update, 65_535], [:settings, [[window, 131_070]]]])
          expect(host_connection(host).local_window.desired).to eq(131_070)
          # 131,070 bytes on a new stream exceed the original 65,535-byte
          # connection window, but not the grown one.
          second = open_request(connection, origin, "/second")
          entered.dequeue
          ingress_burst(connection, [second.stream.id, 131_070])
          ingress_barrier(task, connection, wire)
          expect([wire[:resets], wire[:goaway]]).to eq([{}, nil])
          release.resolve(true)
        end
      end
    end
  end

  it "grows a Transport stream window from a server's DATA, up to max_receive_window" do
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
      transport = ConnectRPC::Async::Transport.new(origin: origin, stream_receive_window: 65_536,
                                                   max_receive_window: 131_072)
      outgoing = ConnectRPC::StreamRequest.new(url: URI("#{origin}/stream"), http_method: "POST", headers: {})
      transport.open_stream(outgoing) do |exchange|
        exchange.close_send
        exchange.response_head
        connection, wire, stream = peers.dequeue
        body = exchange.instance_variable_get(:@operation).fetch(:response).stream.instance_variable_get(:@input)
        ingress_burst(connection, [stream.id, 65_536])
        await_growth(task, wire, [131_072])
        # The reader writes them itself while the frame guard is free.
        expect(wire[:control].drop(1)).to eq([[:window_update, 16_777_216 - 65_535], [:window_update, 16_384],
                                              [:ping, bdp_ping], [:settings, [[window, 131_072]]]])
        ingress_burst(connection, [stream.id, 65_536])
        ingress_barrier(task, connection, wire)
        expect(ingress_backlog(body)).to eq([131_072, 8])
        expect([wire[:credit][stream.id], wire[:resets], wire[:probes]]).to eq([0, {}, [bdp_ping]])
        ingress_data(connection, stream.id, "a")
        ingress_barrier(task, connection, wire)
        expect(wire[:resets]).to eq(stream.id => flow_control_error)
        expect(wire[:goaway]).to be_nil
      end
    ensure
      begin
        transport&.close(grace: 0)
        peer&.stop
        peer&.wait
      ensure
        bound&.close
      end
    end
  end

  it "keeps a grown window when the peer acknowledges an older SETTINGS while a newer one is pending" do
    reactor do |task|
      entered = Async::Queue.new
      app = lambda do |request|
        entered.enqueue(request.path)
        Async::Promise.new.wait
      end
      window_host(app, max_receive_window: 262_144) do |host, origin|
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
        # A frozen estimator clock makes every sample's bandwidth equal, so
        # growth depends only on sample sizes, not on loopback timing.
        host_connection(host).instance_variable_get(:@bandwidth).instance_variable_set(:@clock,
                                                                                       Struct.new(:now).new(0.0))
        first = open_request(connection, origin, "/first")
        entered.dequeue
        ingress_burst(connection, [first.stream.id, 65_536])
        await_growth(task, wire, [131_072])
        second = open_request(connection, origin, "/second")
        entered.dequeue
        ingress_burst(connection, [first.stream.id, 65_536], [second.stream.id, 131_072])
        await_growth(task, wire, [131_072, 262_144])
        expect(held_acks.size).to eq(3)
        # The preface and first growth ACKs arrive; protocol-http2 alone
        # would set every stream back to 131,072.
        held_acks.shift(2).each { |frame| connection.method(:write_frame).super_method.call(frame) }
        ingress_barrier(task, connection, wire)
        third = open_request(connection, origin, "/third")
        entered.dequeue
        ingress_burst(connection, [first.stream.id, 131_072], [second.stream.id, 131_072],
                      [third.stream.id, 262_144])
        ingress_barrier(task, connection, wire)
        expect([wire[:resets], wire[:goaway]]).to eq([{}, nil])
        held_acks.each { |frame| connection.method(:write_frame).super_method.call(frame) }
        ingress_barrier(task, connection, wire)
        expect(host_connection(host).streams.values.map { |stream| stream.local_window.available }).to eq([0, 0, 0])
        expect([wire[:resets], wire[:goaway]]).to eq([{}, nil])
      ensure
        AsyncHelpers::INTERNAL.raw_close(raw) if raw
        connection&.close
      end
    end
  end

  it "publishes growth only when its SETTINGS is written, not while another frame write holds the guard" do
    reactor do |task|
      entered = Async::Queue.new
      respond = Async::Promise.new
      app = lambda do |request|
        entered.enqueue(request.path)
        request.path == "/respond" ? respond.wait : Async::Promise.new.wait
        Protocol::HTTP::Response[200, {}, ["ok"]]
      end
      window_host(app, max_receive_window: 131_072) do |host, origin|
        with_ingress_client(task, origin, :h2c) do |connection, wire|
          held_acks = Async::Queue.new
          ping = bdp_ping
          connection.define_singleton_method(:write_frame) do |frame|
            next held_acks.enqueue(frame) if frame.is_a?(Protocol::HTTP2::PingFrame) && frame.acknowledgement? &&
                                             frame.unpack == ping

            super(frame)
          end
          responding = open_request(connection, origin, "/respond")
          entered.dequeue
          upload = open_request(connection, origin, "/upload")
          entered.dequeue
          ingress_burst(connection, [upload.stream.id, 65_536])
          ack = held_acks.dequeue
          server = host_connection(host)
          gate = hold_after_flush(server) { |frame| frame.is_a?(Protocol::HTTP2::HeadersFrame) }
          respond.resolve(true)
          gate.started.dequeue
          connection.method(:write_frame).super_method.call(ack)
          bandwidth = server.instance_variable_get(:@bandwidth)
          ingress_wait(task, "growth computed", state: -> { bandwidth.estimate }) { bandwidth.estimate == 131_072 }
          stream = server.streams.fetch(upload.stream.id)
          published = -> { [server.local_settings.pending.initial_window_size, stream.local_window.capacity] }
          # The response's HEADERS write holds the guard, so the SETTINGS
          # waits: nothing is appended or raised yet.
          expect([published.call, server.stream_receive_window,
                  ingress_growth(wire)]).to eq([[65_536, 65_536], 65_536, []])
          gate.release.enqueue(true)
          await_growth(task, wire, [131_072])
          expect([published.call, server.stream_receive_window]).to eq([[131_072, 131_072], 131_072])
          connection.read_response(responding)
          expect(responding.read).to eq("ok")
        end
      end
    end
  end

  it "counts a BDP PING's ACK read while the PING's writer still holds the frame guard" do
    reactor do |task|
      entered = Async::Queue.new
      app = lambda do |request|
        entered.enqueue(request.path)
        Async::Promise.new.wait
      end
      window_host(app, max_receive_window: 131_072) do |host, origin|
        # The held PING write would otherwise exceed the control writer's 100 ms write bound.
        host_session(host).instance_variable_set(:@clock, ConnectionPeer::FakeClock.new)
        with_ingress_client(task, origin, :h2c) do |connection, wire|
          upload = open_request(connection, origin, "/upload")
          entered.dequeue
          server = host_connection(host)
          gate = hold_after_flush(server) { |frame| bdp_ping?(frame) }
          ingress_burst(connection, [upload.stream.id, 65_536])
          gate.started.dequeue
          # The peer reads the PING and ACKs it; the host reads that ACK
          # before the PING's write_frame returns.
          bandwidth = server.instance_variable_get(:@bandwidth)
          ingress_wait(task, "growth from the ACK read during the PING write", state: -> { bandwidth.estimate }) do
            bandwidth.estimate == 131_072
          end
          gate.release.enqueue(true)
          await_growth(task, wire, [131_072])
        end
      end
    end
  end

  it "resumes a host response waiting for credit when the peer grants it only through SETTINGS" do
    reactor do |task|
      app = ->(_request) { Protocol::HTTP::Response[200, {}, ["b" * 131_072]] }
      window_host(app, response_body_timeout: 5) do |_host, origin|
        with_ingress_client(task, origin, :h2c) do |connection, wire|
          response = open_request(connection, origin, "/download")
          id = response.stream.id
          # The unread response fills the client's 65,536-byte stream window.
          ingress_wait(task, "a full stream window", state: -> { wire[:data] }) { wire[:data][id] == 65_536 }
          ingress_barrier(task, connection, wire)
          expect(wire[:data][id]).to eq(65_536)
          connection.send_settings([[window, 131_072]])
          ingress_wait(task, "the rest of the response", state: -> { wire[:data] }) { wire[:data][id] == 131_072 }
          connection.read_response(response)
          expect(response.read.bytesize).to eq(131_072)
          expect(wire[:resets]).to be_empty
        end
      end
    end
  end

  it "keeps a Transport's BDP PINGs within the default Host's PING enforcement" do
    reactor do |task|
      body = Protocol::HTTP::Body::Writable.new
      entered = Async::Promise.new
      host = ConnectRPC::Async::Host.new(server: server).start
      host_session(host).instance_variable_get(:@application).instance_variable_set(:@fallback, lambda { |_request|
        entered.resolve(true)
        Protocol::HTTP::Response[200, {}, body]
      })
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin, stream_receive_window: 65_536)
      call = task.async { transport.call(request("#{origin}/stream")) }
      entered.wait
      server = host_connection(host)
      bdp = lambda do |frame, ack|
        frame.is_a?(Protocol::HTTP2::PingFrame) && frame.acknowledgement? == ack &&
          frame.unpack == AsyncHelpers::INTERNAL::BDPEstimator::PING
      end
      probes = []
      acks = []
      server.define_singleton_method(:read_frame) do |&block|
        super() do |frame|
          probes << frame if bdp.call(frame, false)
          block&.call(frame)
        end
      end
      server.define_singleton_method(:write_frame) do |frame|
        result = super(frame)
        acks << frame if bdp.call(frame, true)
        result
      end
      # Each chunk follows the previous probe's ACK on the wire, so it starts
      # a new sample: five BDP PINGs within the 5-second min_time, past the
      # third strike, each after a server DATA write.
      5.times do |round|
        body.write("a" * 16_384)
        ingress_wait(task, "BDP PING #{round + 1} acknowledged", state: -> { [probes.size, acks.size] }) do
          acks.size == round + 1
        end
      end
      expect(probes.size).to eq(5)
      body.close_write
      expect(call.wait.body.bytesize).to eq(16_384 * 5)
      resource = transport.instance_variable_get(:@resources).first
      expect([resource.calmed?, resource.finished?, server.upkeep.violated?]).to eq([false, false, false])
    ensure
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  it "sends no BDP PINGs or SETTINGS when windows are static" do
    reactor do |task|
      entered = Async::Queue.new
      app = lambda do |request|
        entered.enqueue(request.path)
        Async::Promise.new.wait
      end
      [{ dynamic_receive_windows: false }, { max_receive_window: 65_536 }].each do |options|
        window_host(app, **options) do |host, origin|
          with_ingress_client(task, origin, :h2c) do |connection, wire|
            upload = open_request(connection, origin, "/upload")
            entered.dequeue
            ingress_burst(connection, [upload.stream.id, 65_536])
            ingress_barrier(task, connection, wire)
            expect([wire[:probes], ingress_growth(wire), wire[:resets]]).to eq([[], [], {}]), options.inspect
            expect(host_connection(host).instance_variable_get(:@bandwidth)).to be_nil
          end
        end
      end
    end
  end
end
