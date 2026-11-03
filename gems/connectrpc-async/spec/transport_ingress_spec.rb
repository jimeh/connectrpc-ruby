# frozen_string_literal: true

require_relative "support/ingress_peer"

RSpec.describe "Transport native response ingress" do
  include IngressPeer

  it "withholds unread response credit through the actual pool and progresses a same-connection sibling" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      listener = bound.sockets.first
      peers = Async::Queue.new
      responses = Async::Queue.new
      entered = Async::Queue.new
      release = Async::Queue.new
      consumed = Async::Queue.new
      peer = task.async do
        socket, = listener.accept
        connection = Async::HTTP::Protocol::HTTP2::Server.new(IO::Stream(socket))
        wire = observe_ingress_wire(connection)
        connection.read_connection_preface(AsyncHelpers::INTERNAL.settings(128))
        connection.read_in_background(parent: task)
        peers.enqueue([connection, wire])
        while (incoming = connection.requests.dequeue)
          if incoming.path == "/hold"
            incoming.stream.send_headers([[":status", "200"], ["content-length", "131072"]])
            responses.enqueue(incoming.stream)
          else
            incoming.send_response(Protocol::HTTP::Response[200, {}, ["sibling"]])
          end
        end
      ensure
        AsyncHelpers::INTERNAL.raw_close(socket)
        connection&.close
      end
      origin = "http://127.0.0.1:#{listener.local_address.ip_port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      # Gate only the application boundary. The real Ingress#read, windows,
      # Transport read loop, pool and cleanup all execute unchanged.
      transport.define_singleton_method(:read_body) do |response, outgoing|
        if outgoing.url.path == "/hold"
          body = response.body
          body.define_singleton_method(:read) do
            release.dequeue
            chunk = super()
            consumed.enqueue(chunk) if chunk
            chunk
          end
          entered.enqueue(body)
        end
        super(response, outgoing)
      end
      call = task.async { transport.call(request("#{origin}/hold")) }
      connection, wire = peers.dequeue
      outgoing = responses.dequeue
      body = entered.dequeue
      4.times { ingress_data(connection, outgoing.id, "a" * 16_384) }
      ingress_barrier(task, connection, wire)
      expect(ingress_backlog(body)).to eq([65_536, 4])
      expect(wire[:credit][outgoing.id]).to eq(0)
      expect(transport.call(request("#{origin}/sibling")).body).to eq("sibling")
      4.times do |round|
        release.enqueue(true)
        expect(consumed.dequeue).to eq("a" * 16_384)
        ingress_barrier(task, connection, wire)
        expect(wire[:credit][outgoing.id]).to eq((round + 1) * 16_384)
        ingress_data(connection, outgoing.id, "a" * 16_384)
        ingress_barrier(task, connection, wire)
        expect(ingress_backlog(body)).to eq([65_536, 4])
      end
      ingress_data(connection, outgoing.id, "", finish: true)
      5.times { release.enqueue(true) }
      expect(call.wait.body).to eq("a" * 131_072)
      pool = transport.instance_variable_get(:@pool)
      expect(pool.resources.values.sum).to eq(0)
      expect(transport.instance_variable_get(:@resources).size).to eq(1)
      expect(transport.instance_variable_get(:@resources).first.connection.streams).to be_empty
      expect(ingress_backlog(body)).to eq([0, 0])
      expect(body.instance_variable_get(:@stream)).to be_nil
    ensure
      release&.enqueue(true)
      transport&.close(grace: 0)
      peer&.cancel
      peer&.wait
      bound&.close
    end
  end
end
