# frozen_string_literal: true

module Soak
  # Held calls for the failure and shutdown scenarios.
  module HeldCalls
    HELD = %i[unary server_stream grpc_unary grpc_server_stream].freeze

    module_function

    # Starts one held call per shape on the harness reactor; returns the tasks
    # once every handler entered.
    def async(campaign, clients, shapes = HELD, expect: ["unavailable"])
      ids = shapes.map { |shape| [campaign.ledger.next_id("held-#{shape}"), shape] }
      tasks = ids.map do |id, shape|
        campaign.task.async do
          campaign.perform(clients, shape, id: id, action: "hold", timeout: nil, expect: expect)
        end
      end
      campaign.ledger.await_entered(ids.map(&:first))
      [tasks, ids.map(&:first)]
    end

    # One held unary call on a plain thread, as ThreadedTransport and HTTP/1
    # callers block.
    def threaded(campaign, clients, expect: ["unavailable"])
      id = campaign.ledger.next_id("held-threaded")
      thread = Thread.new { campaign.perform(clients, :unary, id: id, action: "hold", timeout: nil, expect: expect) }
      thread.report_on_exception = false
      campaign.ledger.await_entered([id])
      [thread, id]
    end

    def finish(threads)
      threads.each { |thread| raise Failure, "Held caller thread did not finish" unless thread.join(10) }
    end

    # Every handler of these ids ended, whatever its reason.
    def ended(campaign, ids)
      ids.each { |id| Soak.poll("server-side end of #{id}") { campaign.ledger.ended(id) } }
    end
  end
end

# Scenarios 6 and 8 against an HTTP/2 server kind (:host or :falcon). Falcon
# adds HTTP1Transport clients over the same listener.
RSpec.shared_examples "soak peer failure and shutdown" do |kind|
  it "6a peer failure: a relay resets every connection mid-traffic and the client recovers (#{kind})" do
    campaign("6a-relay-reset/#{kind}", workload: "4 held calls through a TCP relay, RST, x7 shapes after",
                                       concurrency: 4, payload_sizes: [16]) do |c|
      peer = c.peer_thread("#{kind} server")
      server = c.server(peer, kind: kind, connections: -> { 1 })
      relay = c.relay(server.port)
      clients = c.transport("http://127.0.0.1:#{relay.port}", label: "persistent relayed transport",
                                                              persistent: true, connections: 1)
      c.run do
        c.perform(clients, :unary)
        held, ids = Soak::HeldCalls.async(c, clients)
        relay.reset!
        held.each(&:wait)
        Soak::HeldCalls.ended(c, ids)
        Soak::Builders::H2_SHAPES.each { |shape| c.perform(clients, shape) }
      end
    end
  end

  it "6b peer failure: closing the server with zero grace fails in-flight calls and releases everything (#{kind})" do
    campaign("6b-server-close-zero-grace/#{kind}",
             workload: "held Transport/ThreadedTransport#{'/HTTP1Transport' if kind == :falcon} calls, " \
                       "server close(grace: 0)", concurrency: 6, payload_sizes: [16]) do |c|
      peer = c.peer_thread("#{kind} servers")
      c.run do
        server = c.server(peer, kind: kind, persistent: false, label: "round #{kind}")
        clients = c.transport(server.origin, label: "round transport")
        threaded = c.threaded(server.origin, label: "round threaded")
        held, ids = Soak::HeldCalls.async(c, clients)
        blocked, threaded_id = Soak::HeldCalls.threaded(c, threaded)
        threads = [blocked]
        ids << threaded_id
        if kind == :falcon
          http1 = c.http1(server.origin, label: "round http1")
          h1_held, h1_ids = Soak::HeldCalls.async(c, http1, %i[unary server_stream])
          held.concat(h1_held)
          ids.concat(h1_ids)
        end
        expect(server.close(grace: 0)).to be(false)
        held.each(&:wait)
        Soak::HeldCalls.finish(threads)
        Soak::HeldCalls.ended(c, ids)
        [clients, threaded, http1].compact.each { |set| expect(set.transport.close(grace: 2)).to be(true) }
      end
    end
  end

  it "8 shutdown: graceful close returns true and forced close returns false for clients and servers (#{kind})" do
    campaign("8-shutdown/#{kind}", workload: "graceful then forced close of every client kind and the server",
                                   concurrency: 4, payload_sizes: [16]) do |c|
      peer = c.peer_thread("#{kind} servers")
      c.run do
        server = c.server(peer, kind: kind, persistent: false, label: "graceful #{kind}")
        clients = c.transport(server.origin, label: "graceful transport")
        threaded = c.threaded(server.origin, label: "graceful threaded")
        Soak::Builders::H2_SHAPES.each { |shape| c.perform(clients, shape) }
        c.on_threads(*Soak::Builders::THREADED_SHAPES.map { |shape| -> { c.perform(threaded, shape) } })
        owner = Soak::Sampler.owner_thread(threaded.transport)
        sets = [clients, threaded]
        if kind == :falcon
          sets << (http1 = c.http1(server.origin, label: "graceful http1"))
          Soak::Builders::H1_SHAPES.each { |shape| c.perform(http1, shape) }
        end
        sets.each { |set| expect(set.transport.close(grace: 2)).to be(true) }
        expect(owner).not_to be_alive
        expect(server.close(grace: 2)).to be(true)

        server = c.server(peer, kind: kind, persistent: false, label: "forced #{kind}")
        forced = [c.transport(server.origin, label: "forced transport")]
        held, ids = Soak::HeldCalls.async(c, forced.first, %i[unary server_stream])
        threaded = c.threaded(server.origin, label: "forced threaded")
        blocked, threaded_id = Soak::HeldCalls.threaded(c, threaded)
        forced << threaded
        ids << threaded_id
        if kind == :falcon
          forced << (http1 = c.http1(server.origin, label: "forced http1"))
          h1_held, h1_ids = Soak::HeldCalls.async(c, http1, %i[unary])
          held.concat(h1_held)
          ids.concat(h1_ids)
        end
        survivor = c.transport(server.origin, label: "server-forced transport")
        survivor_held, survivor_ids = Soak::HeldCalls.async(c, survivor, %i[unary])
        forced.each { |set| expect(set.transport.close(grace: 0)).to be(false) }
        expect(server.close(grace: 0)).to be(false)
        [*held, *survivor_held].each(&:wait)
        Soak::HeldCalls.finish([blocked])
        Soak::HeldCalls.ended(c, ids + survivor_ids)
        expect(survivor.transport.close(grace: 2)).to be(true)
      end
    end
  end
end
