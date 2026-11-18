# frozen_string_literal: true

require_relative "support/ingress_peer"

# Connection credit returned at a quarter of a 65,535-byte connection window:
# one 16,384-byte DATA frame reaches it. Raw protocol-http2 peers write DATA
# without flow-control bookkeeping, so each example states what it spends.
RSpec.describe "HTTP/2 connection credit" do
  include IngressPeer

  let(:bdp_ping) { AsyncHelpers::INTERNAL::BDPEstimator::PING }

  # Holds `connection`'s frame guard, as a stuck frame write would, until
  # the returned promise resolves.
  def hold_guard(task, connection)
    holding = Async::Promise.new
    release = Async::Promise.new
    task.async do
      connection.synchronize do
        holding.resolve(true)
        release.wait
      end
    end
    holding.wait
    release
  end

  it "keeps a Host's reader applying DATA while another writer holds the guard, then writes one coalesced " \
     "WINDOW_UPDATE" do
    reactor do |task|
      entered = Async::Queue.new
      app = lambda do |request|
        entered.enqueue(request.path)
        Async::Promise.new.wait
      end
      # Barrier PINGs outpace grpc-go's enforcement without server writes between them.
      keepalive = ConnectRPC::Async::ServerKeepalive.new(min_time: nil)
      host = ConnectRPC::Async::Host.new(server: server, keepalive: keepalive, connection_receive_window: 65_535,
                                         dynamic_receive_windows: false).start
      host_session(host).instance_variable_get(:@application).instance_variable_set(:@fallback, app)
      origin = "http://127.0.0.1:#{host.port}"
      release = nil
      with_ingress_client(task, origin, :h2c) do |connection, wire|
        ids = %w[/first /second].map do |path|
          response = connection.create_response
          response.instance_variable_set(:@request, Protocol::HTTP::Request["POST", path, {}, nil])
          response.stream.send_headers([[":method", "POST"], [":scheme", "http"],
                                        [":authority", URI(origin).authority], [":path", path]])
          entered.dequeue
          response.stream.id
        end
        server = host_session(host).instance_variable_get(:@resources).first.connection
        backlog = -> { ids.map { |id| ingress_backlog(server.streams.fetch(id).instance_variable_get(:@input)).first } }
        release = hold_guard(task, server)
        # The first frame reaches the quarter; the sibling's adds as much.
        ingress_data(connection, ids.first, "a" * 16_384)
        ingress_data(connection, ids.last, "b" * 16_384)
        ingress_wait(task, "the reader to apply both streams' DATA while the guard is held", state: backlog) do
          backlog.call == [16_384, 16_384]
        end
        # Both increments are reserved; neither is written yet.
        expect([server.local_window.available, wire[:control].drop(1)]).to eq([65_535, []])
        release.resolve(true)
        ingress_wait(task, "the connection credit", state: -> { wire[:control] }) { wire[:credit][0] == 32_768 }
        ingress_barrier(task, connection, wire)
        expect(wire[:control].drop(1)).to eq([[:window_update, 32_768]])
        expect([wire[:resets], wire[:goaway]]).to eq([{}, nil])
      end
    ensure
      release&.resolve(true)
      host&.close(grace: 0)
    end
  end

  [{ windows: "dynamic", options: {}, frames: ->(ping) { [[:window_update, 32_768], [:ping, ping]] } },
   { windows: "static", options: { dynamic_receive_windows: false },
     frames: ->(_ping) { [[:window_update, 32_768]] } }].each do |variant|
    it "keeps a Transport's reader applying DATA while another writer holds the guard, then writes one coalesced " \
       "WINDOW_UPDATE, with #{variant[:windows]} windows" do
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
          2.times do
            incoming = connection.requests.dequeue
            incoming.stream.send_headers([[":status", "200"]])
            peers.enqueue([connection, wire, incoming.stream])
          end
          Async::Promise.new.wait
        ensure
          AsyncHelpers::INTERNAL.raw_close(socket)
          connection&.close
        end
        origin = "http://127.0.0.1:#{listener.local_address.ip_port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin, stream_receive_window: 65_536,
                                                     connection_receive_window: 65_535, **variant[:options])
        outgoing = ConnectRPC::StreamRequest.new(url: URI("#{origin}/stream"), http_method: "POST", headers: {})
        input = lambda do |exchange|
          exchange.instance_variable_get(:@operation).fetch(:response).stream.instance_variable_get(:@input)
        end
        transport.open_stream(outgoing) do |first|
          first.close_send
          first.response_head
          connection, wire, first_stream = peers.dequeue
          transport.open_stream(outgoing) do |second|
            second.close_send
            second.response_head
            _, _, second_stream = peers.dequeue
            client = transport.instance_variable_get(:@resources).first.connection
            backlog = -> { [first, second].map { |exchange| ingress_backlog(input.call(exchange)).first } }
            release = hold_guard(task, client)
            ingress_data(connection, first_stream.id, "a" * 16_384)
            ingress_data(connection, second_stream.id, "b" * 16_384)
            ingress_wait(task, "the reader to apply both streams' DATA while the guard is held", state: backlog) do
              backlog.call == [16_384, 16_384]
            end
            expect([client.local_window.available, wire[:control].drop(1)]).to eq([65_535, []])
            release.resolve(true)
            frames = variant[:frames].call(bdp_ping)
            ingress_wait(task, "the connection credit", state: -> { wire[:control] }) do
              wire[:control].drop(1) == frames
            end
            ingress_barrier(task, connection, wire)
            expect(wire[:control].drop(1)).to eq(frames)
            expect([wire[:resets], wire[:goaway]]).to eq([{}, nil])
          ensure
            release&.resolve(true)
          end
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
  end
end
