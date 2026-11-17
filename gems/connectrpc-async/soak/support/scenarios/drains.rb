# frozen_string_literal: true

module Soak
  # Server connection admission demanded above `max_connections`.
  module ConnectionAdmission
    MAX_CONNECTIONS = 4

    module_function

    # Holds one call on each of MAX_CONNECTIONS fresh transports, so every
    # admitted connection stays accounted (also once its age drain starts).
    # One more transport's connection must then wait in the listen backlog,
    # its call reaching no handler, and succeed once one held call releases
    # its slot.
    def saturate(campaign, server)
      Soak.poll("earlier server connections to retire before saturation") do
        server.run { |host| Sampler.session_live(host).empty? }
      end
      transports = Array.new(MAX_CONNECTIONS + 1) do |index|
        campaign.transport(server.origin, label: "round saturating transport #{index}")
      end
      ids = Array.new(MAX_CONNECTIONS) { campaign.ledger.next_id("held-connection") }
      held = ids.each_with_index.map do |id, index|
        campaign.task.async { campaign.perform(transports[index], :unary, id: id, action: "hold", timeout: nil) }
      end
      campaign.ledger.await_entered(ids)
      queued = campaign.ledger.next_id("queued-connection")
      waiting = campaign.task.async { campaign.perform(transports.last, :unary, id: queued) }
      Soak.poll("the extra connection queued in the listen backlog") do
        server.run { |host| Sampler.listener_queued?(host) }
      end
      raise Failure, "The queued connection's call reached a handler" unless campaign.ledger.entries(queued).zero?

      live = server.run { |host| Sampler.session_live(host).size }
      raise Failure, "#{live} live server connections with a cap of #{MAX_CONNECTIONS}" unless live == MAX_CONNECTIONS

      campaign.gates.open(ids.first)
      waiting.wait
      raise Failure, "The queued connection's call did not reach a handler" unless campaign.ledger.entries(queued) == 1

      ids.drop(1).each { |id| campaign.gates.open(id) }
      held.each(&:wait)
      transports.each do |clients|
        raise Failure, "A saturating transport did not close gracefully" unless clients.transport.close(grace: 2)
      end
    end
  end
end

# Scenario 2 against an HTTP/2 server kind (:host or :falcon): graceful
# GOAWAY drains, each synchronized on the client's observed connection events.
RSpec.shared_examples "soak goaway drains" do |kind|
  def closed_outcomes(events) = events.select { |event| event.type == :connection_closed }.map(&:outcome)

  it "2a age drain: keeps every call successful while max_connection_age drains connections under load (#{kind})" do
    cap = Soak::ConnectionAdmission::MAX_CONNECTIONS
    campaign("2a-age-drain/#{kind}", workload: "Transport unary/stream batches across age drains, then " \
                                               "#{cap} held connections and one queued behind max_connections #{cap}",
                                     concurrency: 6, payload_sizes: [16, 4_096], max_connection_age: 0.3,
                                     max_connections: cap) do |c|
      peer = c.peer_thread("#{kind} server")
      keepalive = ConnectRPC::Async::ServerKeepalive.new(max_connection_age: 0.3, max_connection_age_grace: 5)
      server = c.server(peer, kind: kind, keepalive: keepalive, max_connections: cap, connections: -> { 0 })
      admissions = server.run { |host| Soak::Sampler::AdmissionMeter.new(host) }
      events = []
      clients = c.transport(server.origin, label: "persistent transport", persistent: true, connections: 0,
                                           connection_observer: ->(event) { events << event })
      meter = ConnectionPeer::SocketMeter.new(clients.transport)
      shapes = %i[unary unary get server_stream bidi grpc_unary]
      c.run do
        target = closed_outcomes(events).size + 2
        expires = Soak.now + 15
        until closed_outcomes(events).size >= target
          if Soak.now > expires
            raise Soak::Failure,
                  "Two age drains did not finish within 15s: #{closed_outcomes(events)}"
          end

          c.concurrently(*shapes.map { |shape| -> { c.perform(clients, shape, size: shape == :unary ? 4_096 : 16) } })
        end
        c.peaks.observe("client sockets", meter.peak, cap: 2)
        expect(closed_outcomes(events).uniq).to eq([:goaway])
        Soak::ConnectionAdmission.saturate(c, server)
        c.peaks.observe("server connections", server.run { admissions.peak }, cap: cap)
      end
    end
  end

  it "2b idle drain: a persistent client reconnects after each server idle drain (#{kind})" do
    # The idle timer must not expire between two of the sequential calls.
    campaign("2b-idle-drain/#{kind}", workload: "Transport x7 shapes, then idle drain", concurrency: 1,
                                      payload_sizes: [16], idle_timeout: Soak::TIMER_HEADROOM) do |c|
      peer = c.peer_thread("#{kind} server")
      server = c.server(peer, kind: kind, idle_timeout: Soak::TIMER_HEADROOM, connections: -> { 0 })
      events = []
      clients = c.transport(server.origin, label: "persistent transport", persistent: true, connections: 0,
                                           connection_observer: ->(event) { events << event })
      c.run do
        before = closed_outcomes(events).size
        Soak::Builders::H2_SHAPES.each { |shape| c.perform(clients, shape) }
        Soak.poll("the idle drain's connection_closed event") { closed_outcomes(events).size > before }
        expect(closed_outcomes(events).drop(before)).to eq([:goaway])
      end
    end
  end

  it "2c drain_tls: retires the old generation under load while the new one serves (#{kind})" do
    campaign("2c-drain-tls/#{kind}", workload: "held unary on old generation, x7 shapes on new", concurrency: 3,
                                     payload_sizes: [16]) do |c|
      peer = c.peer_thread("#{kind} server")
      server = c.server(peer, kind: kind, mode: :tls, connections: -> { 1 })
      old = c.transport(server.origin, mode: :tls, label: "persistent tls transport", persistent: true, connections: 1)
      c.run do
        c.perform(old, :unary)
        generation = server.run(&:tls_generation)
        ids = Array.new(2) { c.ledger.next_id("held") }
        held = ids.map { |id| c.task.async { c.perform(old, :unary, id: id, action: "hold") } }
        c.ledger.await_entered(ids)
        reloaded = server.run { |host| host.reload_tls(**Soak::Topology::TLS.host_tls) }
        expect(reloaded).to eq(generation + 1)
        drain = c.task.async { server.run { |host| host.drain_tls(grace: 5) } }
        fresh = c.transport(server.origin, mode: :tls, label: "round tls transport")
        Soak::Builders::H2_SHAPES.each { |shape| c.perform(fresh, shape) }
        expect(drain).not_to be_finished
        ids.each { |id| c.gates.open(id) }
        held.each(&:wait)
        expect(drain.wait).to be(true)
        c.perform(old, :unary)
        expect(fresh.transport.close(grace: 2)).to be(true)
      end
    end
  end
end
