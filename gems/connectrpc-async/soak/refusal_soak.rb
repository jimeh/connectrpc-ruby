# frozen_string_literal: true

RSpec.describe "Async Transport lifecycle campaign: refusal semantics" do
  def goaway(connection, last_stream_id, code = Protocol::HTTP2::Error::NO_ERROR)
    frame = Protocol::HTTP2::GoawayFrame.new
    frame.pack(last_stream_id, code, "")
    connection.write_frame(frame)
  end

  # A raw peer refusing each id per its plan entry [kind, refusals]; every
  # HEADERS counts against the id, and only processed calls enter the ledger.
  def refusing_peer(campaign, listener, plan, streams, signals)
    settings = { Protocol::HTTP2::Settings::MAXIMUM_CONCURRENT_STREAMS => 2 }
    ConnectionPeer::RecordingServer.new(campaign.task, listener, settings: settings) do |incoming|
      id = incoming.headers[Soak::Ledger::ID].to_s
      count = campaign.ledger.headers(id)
      streams[id] = incoming.stream
      kind, refusals = plan.fetch(id, [:none, 0])
      next refuse(incoming, kind, streams, signals) if count <= refusals

      campaign.ledger.enter(id)
      campaign.gates.wait(id) if kind == :hold
      body = incoming.read
      incoming.send_response(Protocol::HTTP::Response[200, { "content-type" => "application/proto" }, [body]])
    rescue Protocol::HTTP2::Error, IOError
      nil
    end
  end

  def refuse(incoming, kind, streams, signals)
    stream = incoming.stream
    stream.send_headers([[":status", "103"]]) if kind == :interim
    if kind == :goaway
      signals[:goaway].dequeue
      goaway(stream.connection, streams.fetch(signals[:below]).id)
    else
      stream.send_reset_stream(Protocol::HTTP2::Error::REFUSED_STREAM)
    end
  end

  it "10 refusal: resends only provably unprocessed Connect unary calls, once, under connection churn" do
    campaign("10-refusal/raw-peer", workload: "fresh Transport per round: reset x1/x2, gRPC, stream, 103+reset, " \
                                              "GOAWAY above a held call with an unsent reroute",
                                    concurrency: 3, payload_sizes: [16]) do |c|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      c.cleanup { bound.close }
      plan = {}
      streams = {}
      signals = { goaway: Async::Queue.new }
      peer = refusing_peer(c, bound.sockets.first, plan, streams, signals)
      c.cleanup { peer.stop }
      origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
      c.run do
        plan.clear
        streams.clear
        clients = c.transport(origin, label: "round transport")
        expected = {}
        case_id = lambda do |name, kind, refusals, headers|
          c.ledger.next_id(name).tap do |id|
            plan[id] = [kind, refusals]
            expected[id] = headers
          end
        end
        c.perform(clients, :unary, id: case_id.call("refused-once", :reset, 1, 2))
        c.perform(clients, :unary, id: case_id.call("refused-twice", :reset, 2, 2), expect: ["unavailable"])
        c.perform(clients, :grpc_unary, id: case_id.call("grpc-refused", :reset, 2, 1), expect: ["unavailable"])
        c.perform(clients, :server_stream, id: case_id.call("stream-refused", :reset, 2, 1),
                                           expect: ["unavailable"])
        c.perform(clients, :unary, id: case_id.call("interim-refused", :interim, 2, 1), expect: ["unavailable"])

        held = case_id.call("held", :hold, 0, 1)
        held_call = c.task.async { c.perform(clients, :unary, id: held, timeout: nil) }
        c.ledger.await_entered([held])
        signals[:below] = held
        resent = case_id.call("goaway-resent", :goaway, 1, 2)
        resent_call = c.task.async { c.perform(clients, :unary, id: resent) }
        Soak.poll("the GOAWAY refusal's first HEADERS") { c.ledger.header_counts[resent] == 1 }
        rerouted = case_id.call("unsent-reroute", :none, 0, 1)
        rerouted_call = c.task.async { c.perform(clients, :unary, id: rerouted) }
        Soak.poll("a call waiting for connection capacity") { Soak::Sampler.acquiring(clients.transport) == 1 }
        signals[:goaway].enqueue(true)
        [resent_call, rerouted_call].each(&:wait)
        c.gates.open(held)
        held_call.wait
        expect(c.ledger.header_counts).to eq(expected)
        expect(expected.keys.map { |id| c.ledger.entries(id) }).to eq([1, 0, 0, 0, 0, 1, 1, 1])
        expect(clients.transport.close(grace: 2)).to be(true)
      end
    end
  end
end
