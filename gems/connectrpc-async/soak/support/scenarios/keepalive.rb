# frozen_string_literal: true

# Scenario 9 (server half) against an HTTP/2 server kind (:host or :falcon).
RSpec.shared_examples "soak server keepalive" do |kind|
  it "9b server keepalive: aborts a raw client that never ACKs while an ACKing client keeps its connection " \
     "(#{kind})" do
    campaign("9b-server-keepalive/#{kind}", workload: "raw h2c client ignoring PINGs with a held call, sibling " \
                                                      "Transport x7 shapes", concurrency: 2, payload_sizes: [16],
                                            keepalive: { time: 0.2, timeout: Soak::TIMER_HEADROOM }) do |c|
      peer = c.peer_thread("#{kind} server")
      # The ACKing client must keep its connection through a stalled scheduler.
      keepalive = ConnectRPC::Async::ServerKeepalive.new(time: 0.2, timeout: Soak::TIMER_HEADROOM)
      server = c.server(peer, kind: kind, keepalive: keepalive, connections: -> { 1 })
      clients = c.transport(server.origin, label: "persistent ACKing transport", persistent: true, reuse: true,
                                           connections: 1)
      c.run do
        c.perform(clients, :unary)
        raw = keepalive_client(c.task, server.port)
        c.track(:io, raw.raw, "silent client socket")
        raw.wire.ack_pings = false
        id = c.ledger.next_id("silent")
        headers = request_headers(Soak::Topology::ECHO::ECHO.path, [Soak::Ledger::ID, id],
                                  [Soak::Topology::OP, "hold"])
        raw_headers(raw, 1, headers)
        raw_data(raw, 1, Soak::Topology::MESSAGE.encode(Soak::Topology.message(1)), end_stream: true)
        c.ledger.await_entered([id])
        Soak.poll("server keepalive abort of the silent client") { raw.eof? }
        expect(raw.wire.pings).not_to be_empty
        expect(Soak.poll("silent call's handler end") { c.ledger.ended(id) }).not_to eq(:ok)
        Soak::Builders::H2_SHAPES.each { |shape| c.perform(clients, shape) }
        close_keepalive_client(raw)
      end
    end
  end
end
