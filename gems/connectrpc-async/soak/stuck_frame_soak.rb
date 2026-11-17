# frozen_string_literal: true

RSpec.describe "Async Transport lifecycle campaign: stuck-frame fallback" do
  it "4a stuck frame: cancelling a call whose DATA frame is held fails its connection and siblings, then " \
     "reconnects" do
    campaign("4a-stuck-frame/host", workload: "held sibling + 500 KB unary gated mid-DATA, cancel, x7 shapes after",
                                    concurrency: 2, payload_sizes: [16, 500_000]) do |c|
      peer = c.peer_thread("host")
      server = c.server(peer, connections: -> { 1 })
      clients = c.transport(server.origin, label: "persistent transport", persistent: true, connections: 1)
      c.run do
        c.perform(clients, :unary)
        sibling_id = c.ledger.next_id("sibling")
        sibling = c.task.async do
          c.perform(clients, :unary, id: sibling_id, action: "hold", timeout: nil, expect: ["unavailable"])
        end
        c.ledger.await_entered([sibling_id])
        resource = Soak::Sampler.live_resources(clients.transport).first
        gate = Soak::Sampler.install_gate(resource, FrameGate.new(resource.socket, frame_type: 0))
        source = c.call_source("blocked call source")
        blocked_id = c.ledger.next_id("blocked")
        blocked = c.task.async do
          c.perform(clients, :unary, id: blocked_id, size: 500_000, timeout: nil, cancellation: source.token,
                                     expect: ["canceled"])
        end
        gate.started.dequeue
        source.cancel
        blocked.wait
        sibling.wait
        expect([resource.raw.closed?, resource.finished?]).to eq([true, true])
        expect(c.ledger.entries(blocked_id)).to eq(0)
        Soak::Builders::H2_SHAPES.each { |shape| c.perform(clients, shape) }
      ensure
        gate&.release&.enqueue(true)
      end
    end
  end

  it "4b peer reset while output is blocked: the lease is released and keepalive retires the connection" do
    campaign("4b-reset-blocked-output/raw-peer", workload: "4 KB stream DATA gated, peer RST, rerouted sibling",
                                                 concurrency: 2, payload_sizes: [4_096],
                                                 keepalive: { time: 0.2, timeout: Soak::TIMER_HEADROOM }) do |c|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      c.cleanup { bound.close }
      reset = Async::Queue.new
      peer = ConnectionPeer::RecordingServer.new(c.task, bound.sockets.first) do |incoming|
        if incoming.path == "/stream"
          reset.dequeue
          incoming.stream.send_reset_stream(Protocol::HTTP2::Error::CANCEL)
        else
          c.ledger.enter(incoming.headers[Soak::Ledger::ID].to_s)
          incoming.send_response(Protocol::HTTP::Response[200, {}, [incoming.path]])
        end
      rescue Protocol::HTTP2::Error, IOError
        nil
      end
      c.cleanup { peer.stop }
      origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
      events = []
      # Healthy connections must answer PINGs in time through a stalled scheduler.
      keepalive = ConnectRPC::Async::Keepalive.new(time: 0.2, timeout: Soak::TIMER_HEADROOM)
      clients = c.transport(origin, label: "persistent keepalive transport", persistent: true, connections: 1,
                                    connection_policy: ConnectRPC::Async::ConnectionPolicy.new(keepalive: keepalive),
                                    connection_observer: ->(event) { events << event })
      transport = clients.transport
      # A unary call through the raw peer, recorded in the ledger.
      unary = lambda do |path|
        id = c.ledger.next_id(path.delete("/"))
        call = request("#{origin}#{path}", headers: { Soak::Ledger::ID => [id] })
        call.cancellation = c.shared_source.token
        body = transport.call(call).body
        c.ledger.outcome(id, :ok)
        body
      end
      c.run do
        expect(unary.call("/warm")).to eq("/warm")
        resource = Soak::Sampler.live_resources(transport).first
        gate = Soak::Sampler.install_gate(resource, FrameGate.new(resource.socket, frame_type: 0))
        proceed = Async::Queue.new
        stream_request = ConnectRPC::StreamRequest.new(url: URI("#{origin}/stream"), http_method: "POST", headers: {},
                                                       cancellation: c.shared_source.token)
        call = c.task.async do
          transport.open_stream(stream_request) do |exchange|
            exchange.write("x" * 4_096)
            proceed.dequeue
          end
        end
        gate.started.dequeue
        stream = Soak::Sampler.client_streams(transport).first
        resets = Soak::Sampler.watch_resets(stream)
        reset.enqueue(true)
        Soak.poll("peer reset observed while DATA is held") { stream.closed? }
        proceed.enqueue(true)
        call.wait
        # The peer reset the stream unprocessed; the exchange itself ended normally.
        code = resets.pop(timeout: 0)
        name = Protocol::HTTP2::Error.constants.find { |constant| Protocol::HTTP2::Error.const_get(constant) == code }
        c.ledger.outcome(c.ledger.next_id("reset-stream"), "RST_STREAM #{name || code.inspect}",
                         expect: ["RST_STREAM CANCEL"])
        expect([resource.connection.streams.size, Soak::Sampler.usage(transport), resource.finished?])
          .to eq([0, 0, false])
        closed = -> { events.select { |event| event.type == :connection_closed }.map(&:outcome) }
        closed_before = closed.call.size
        sibling = c.task.async { unary.call("/after") }
        Soak.poll("keepalive retirement of the blocked connection") { resource.finished? }
        expect(sibling.wait).to eq("/after")
        expect(gate).not_to be_written
        Soak.poll("the blocked connection's connection_closed event") { closed.call.size > closed_before }
        expect(closed.call.drop(closed_before)).to eq([:keepalive_timeout])
        expect(unary.call("/next")).to eq("/next")
      ensure
        gate&.release&.enqueue(true)
      end
    end
  end
end
