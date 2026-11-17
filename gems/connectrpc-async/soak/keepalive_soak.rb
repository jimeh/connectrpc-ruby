# frozen_string_literal: true

require_relative "support/scenarios/keepalive"

RSpec.describe "Async lifecycle campaign: keepalive" do
  def closed_outcomes(events) = events.select { |event| event.type == :connection_closed }.map(&:outcome)

  it "9a client keepalive: a black-holed connection retires with keepalive_timeout and the client reconnects" do
    campaign("9a-client-keepalive/host", workload: "held unary through a relay, black hole, x7 shapes after",
                                         concurrency: 1, payload_sizes: [16],
                                         keepalive: { time: 0.2, timeout: Soak::TIMER_HEADROOM }) do |c|
      peer = c.peer_thread("host")
      server = c.server(peer, connections: -> { 1 })
      relay = c.relay(server.port)
      events = []
      # Healthy connections must answer PINGs in time through a stalled scheduler.
      keepalive = ConnectRPC::Async::Keepalive.new(time: 0.2, timeout: Soak::TIMER_HEADROOM)
      clients = c.transport("http://127.0.0.1:#{relay.port}", label: "persistent keepalive transport",
                                                              persistent: true, connections: 1,
                                                              connection_policy: ConnectRPC::Async::ConnectionPolicy
                                                                                 .new(keepalive: keepalive),
                                                              connection_observer: ->(event) { events << event })
      c.run do
        c.perform(clients, :unary)
        before = closed_outcomes(events).size
        id = c.ledger.next_id("black-holed")
        held = c.task.async do
          c.perform(clients, :unary, id: id, action: "hold", timeout: nil, expect: ["unavailable"])
        end
        c.ledger.await_entered([id])
        relay.blackhole!
        held.wait
        Soak.poll("the black-holed connection's connection_closed event") { closed_outcomes(events).size > before }
        expect(closed_outcomes(events).drop(before)).to eq([:keepalive_timeout])
        Soak.poll("black-holed call's handler end") { c.ledger.ended(id) }
        Soak::Builders::H2_SHAPES.each { |shape| c.perform(clients, shape) }
      end
    end
  end

  it "9c too_many_pings: ENHANCE_YOUR_CALM ends an over-eager connection and later connections double the " \
     "interval" do
    campaign("9c-too-many-pings/host", workload: "client PINGs every 0.1 s vs server min_time 1 s",
                                       concurrency: 1, payload_sizes: [16],
                                       keepalive: { client_time: 0.1, server_min_time: 1 }) do |c|
      peer = c.peer_thread("host")
      server = c.server(peer, keepalive: ConnectRPC::Async::ServerKeepalive.new(min_time: 1),
                              connections: -> { 0 })
      keepalive = ConnectRPC::Async::Keepalive.new(time: 0.1, timeout: 1, permit_without_calls: true)
      c.run do
        events = []
        clients = c.transport(server.origin, label: "round eager transport",
                                             connection_policy: ConnectRPC::Async::ConnectionPolicy
                                                                .new(keepalive: keepalive),
                                             connection_observer: ->(event) { events << event })
        c.perform(clients, :unary)
        first = Soak::Sampler.live_resources(clients.transport).first
        Soak.poll("the too_many_pings GOAWAY") { closed_outcomes(events).any? }
        expect([closed_outcomes(events), Soak::Sampler.calmed?(first)]).to eq([[:goaway], true])
        c.perform(clients, :unary)
        second = Soak::Sampler.live_resources(clients.transport).first
        expect(Soak::Sampler.keepalive_time(second)).to eq(0.2)
        expect(clients.transport.close(grace: 2)).to be(true)
      end
    end
  end

  include_examples "soak server keepalive", :host
end
