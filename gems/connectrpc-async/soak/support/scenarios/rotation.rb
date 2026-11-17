# frozen_string_literal: true

module Soak
  # A TLS provider whose version and server leaf change on each rotation. A
  # blocked call waits on the harness until released or cancelled.
  class RotatingProvider
    attr_reader :serial

    def initialize(base)
      @lock = Mutex.new
      @base = base
      @version = 0
      @blocker = nil
      rotate!
    end

    def rotate!
      tls = Topology::TLS
      @lock.synchronize do
        @version += 1
        @serial = @base + @version
        leaf = tls.credentials(@serial, "localhost", issuer: tls.tls_material.fetch(:root), usage: "serverAuth")
        @material = { version: "v#{@version}", **tls.host_tls(leaf, anchors: [tls.tls_material.fetch(:client_ca)]) }
        @serial
      end
    end

    # The next call signals `entered`, then waits for `release`.
    def block_next!(entered, release) = @lock.synchronize { @blocker = [entered, release] }

    def call
      entered, release = @lock.synchronize { @blocker.tap { @blocker = nil } }
      if entered
        entered << true
        release.pop
      end
      @lock.synchronize { @material.dup }
    end
  end
end

# Scenario 7 against an HTTP/2 server kind (:host or :falcon).
RSpec.shared_examples "soak tls rotation" do |kind|
  it "7 rotation: refreshes a provider under mTLS load, rotates client identities and retires old generations " \
     "(#{kind})" do
    campaign("7-tls-rotation/#{kind}",
             workload: "provider refresh + drain_tls with a held old-generation call, new client identity per " \
                       "round, blocked refresh interrupted by close", concurrency: 3, payload_sizes: [16]) do |c|
      peer = c.peer_thread("#{kind} servers")
      provider = Soak::RotatingProvider.new(1_000)
      server = c.server(peer, kind: kind, mode: :mtls, tls_provider: provider, connections: -> { 1 })
      blocking = Soak::RotatingProvider.new(5_000)
      tls = Soak::Topology::TLS
      current = nil
      c.cleanup { current&.transport&.close(grace: 0) }
      c.run do |round|
        old = current
        held = nil
        if old
          held_id = c.ledger.next_id("old-generation")
          held = c.task.async { c.perform(old, :unary, id: held_id, action: "hold") }
          c.ledger.await_entered([held_id])
        end
        generation = server.run(&:tls_generation)
        serial = provider.rotate!
        expect(server.run(&:refresh_tls)).to eq(generation + 1)
        client_serial = 2_000 + (10 * round) + c.random.rand(10)
        identity = tls.credentials(client_serial, "client #{client_serial}",
                                   issuer: tls.tls_material.fetch(:client_ca), usage: "clientAuth")
        current = c.transport(server.origin, mode: :mtls, identity: identity, label: "identity #{client_serial}",
                                             outlives_round: true)
        id = c.ledger.next_id("identity")
        seen = current.core.call(Soak::Topology::ECHO::ECHO, Soak::Topology.message(1),
                                 metadata: c.ledger.metadata(id, Soak::Topology::OP => "identity"),
                                 cancellation: c.shared_source.token)
        c.ledger.outcome(id, :ok)
        expect(seen.message.name).to eq(client_serial.to_s)
        leaf = Soak::Sampler.client_peer_serial(Soak::Sampler.live_resources(current.transport).first)
        expect(leaf).to eq(serial)
        Soak::Builders::H2_SHAPES.each { |shape| c.perform(current, shape) }
        if old
          # The drain runs as a peer task, so this command returns once the
          # old generation is marked draining, while its call is still held.
          drained = Thread::Queue.new
          marked = server.run do |host|
            Async { drained << host.drain_tls(grace: 5) }
            Soak::Sampler.draining(host).size
          end
          expect(marked).to be_positive
          expect(held).not_to be_finished
          c.gates.open(held_id)
          held.wait
          expect(drained.pop(timeout: 10)).to be(true)
          Soak.poll("old-generation client connection retired") do
            Soak::Sampler.live_connections(old.transport).empty?
          end
          expect(old.transport.close(grace: 2)).to be(true)
        end

        doomed = c.server(peer, kind: kind, mode: :mtls, tls_provider: blocking, persistent: false,
                                label: "#{kind} blocked refresh")
        entered = Thread::Queue.new
        release = Thread::Queue.new
        blocking.block_next!(entered, release)
        closed, refreshed = doomed.run do |host|
          refresh = Async do
            host.refresh_tls
          rescue ConnectRPC::Async::TLSConfiguration::Error => e
            e.category
          end
          entered.pop
          [host.close(grace: 0), refresh.wait]
        end
        release.close
        expect([closed, refreshed]).to eq([true, :closed])
        # Repeats the decided outcome and releases a Falcon listener.
        expect(doomed.close(grace: 0)).to be(true)
      end
    end
  end
end
