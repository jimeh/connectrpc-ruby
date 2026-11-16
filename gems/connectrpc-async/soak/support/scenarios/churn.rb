# frozen_string_literal: true

module Soak
  # Admission caps sampled while held calls keep them full.
  module Saturation
    MAX_REQUESTS = 4
    MAX_PENDING_BYTES = 40_000
    PAYLOAD = 16_384

    module_function

    def admission(campaign, server)
      policy = ConnectRPC::Async::ConnectionPolicy.new(max_pending_bytes: MAX_PENDING_BYTES)
      clients = campaign.transport(server.origin, label: "round saturated transport", max_requests: MAX_REQUESTS,
                                                  connection_policy: policy)
      transport = clients.transport
      held(campaign, Array.new(MAX_REQUESTS) { campaign.ledger.next_id("held-stream") }) do |id|
        campaign.task.async { campaign.perform(clients, :server_stream, id: id, action: "hold", timeout: nil) }
      end => streams
      campaign.perform(clients, :unary, expect: ["unavailable"])
      campaign.peaks.observe("transport admission", Sampler.transport_calls(transport).size, cap: MAX_REQUESTS)
      release(campaign, streams)

      held(campaign, Array.new(2) { campaign.ledger.next_id("held-bytes") }) do |id|
        campaign.task.async { campaign.perform(clients, :unary, id: id, action: "hold", size: PAYLOAD, timeout: nil) }
      end => unary
      campaign.perform(clients, :unary, size: PAYLOAD, expect: ["unavailable"])
      campaign.peaks.observe("transport pending bytes", Sampler.pending_bytes(transport), cap: MAX_PENDING_BYTES)
      release(campaign, unary)
      raise Failure, "Saturated transport did not close gracefully" unless transport.close(grace: 2)

      threaded(campaign, server)
    end

    def threaded(campaign, server)
      clients = campaign.threaded(server.origin, label: "round saturated threaded", max_requests: 2)
      held(campaign, Array.new(2) { campaign.ledger.next_id("held-threaded") }) do |id|
        Thread.new { campaign.perform(clients, :unary, id: id, action: "hold", timeout: nil) }
      end => callers
      campaign.on_threads(-> { campaign.perform(clients, :unary, expect: ["unavailable"]) })
      campaign.peaks.observe("threaded admission", Sampler.admission(clients.transport).first, cap: 2)
      release(campaign, callers)
      raise Failure, "Saturated threaded transport did not close gracefully" unless clients.transport.close(grace: 2)
    end

    # Starts one held call per id and waits until every handler entered.
    def held(campaign, ids, &)
      [ids, ids.map(&)].tap { campaign.ledger.await_entered(ids) }
    end

    def release(campaign, (ids, waiters))
      ids.each { |id| campaign.gates.open(id) }
      waiters.each do |waiter|
        waiter.is_a?(Thread) ? waiter.join(10) || raise(Failure, "Held caller hung") : waiter.wait
      end
    end
  end
end

# Scenario 1 against an HTTP/2 server kind (:host or :falcon).
RSpec.shared_examples "soak churn and reuse" do |kind|
  it "1 churn: reuses persistent connections and retires per-round clients over h2c, TLS and mTLS (#{kind})" do
    campaign("1-churn/#{kind}", workload: "Transport x7 shapes, ThreadedTransport x5 shapes, h2c/TLS/mTLS",
                                concurrency: 8, payload_sizes: [16, 16_384]) do |c|
      peer = c.peer_thread("#{kind} servers")
      servers = {
        h2c: c.server(peer, kind: kind, mode: :h2c, connections: -> { 2 }),
        tls: c.server(peer, kind: kind, mode: :tls, connections: -> { 0 }),
        mtls: c.server(peer, kind: kind, mode: :mtls, connections: -> { 0 })
      }
      shared = c.shared_source
      reused = c.transport(servers[:h2c].origin, label: "persistent transport", persistent: true, reuse: true)
      reused_threaded = c.threaded(servers[:h2c].origin, label: "persistent threaded", persistent: true,
                                                         reuse: true)
      c.run do
        calls = Soak::Builders::H2_SHAPES.map do |shape|
          -> { c.perform(reused, shape, cancellation: shared.token) }
        end
        calls << -> { c.perform(reused, :unary, size: 16_384, cancellation: shared.token) }
        c.concurrently(*calls)
        c.on_threads(*Soak::Builders::THREADED_SHAPES.map do |shape|
          -> { c.perform(reused_threaded, shape, cancellation: shared.token) }
        end)
        servers.each do |mode, server|
          clients = c.transport(server.origin, mode: mode, label: "round transport #{mode}")
          Soak::Builders::H2_SHAPES.each { |shape| c.perform(clients, shape) }
          expect(clients.transport.close(grace: 2)).to be(true)
        end
        threaded = c.threaded(servers[:mtls].origin, mode: :mtls, label: "round threaded mtls")
        c.on_threads(*Soak::Builders::THREADED_SHAPES.map { |shape| -> { c.perform(threaded, shape) } })
        expect(threaded.transport.close(grace: 2)).to be(true)
        Soak::Saturation.admission(c, servers[:h2c])
      end
    end
  end
end
