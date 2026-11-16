# frozen_string_literal: true

module Soak
  # Shared steps of the stream retirement and slow-peer scenarios.
  module StreamSteps
    class Interrupted < StandardError; end
    WINDOW = 65_536
    OUTPUT_CAP = 16_384

    module_function

    # Observes every OutputBody the library creates (client and server), for
    # queue maxima. rspec-mocks restores the constructor after the example.
    def watch_outputs(example)
      bodies = Queue.new
      example.allow(Sampler::INTERNAL::OutputBody).to example.receive(:new).and_wrap_original do |original|
        original.call.tap { |body| bodies << body }
      end
      bodies
    end

    def output_peak(bodies)
      peak = 0
      peak = [peak, Sampler.output_maximum(bodies.pop)].max until bodies.empty?
      peak
    end

    # Starts a flood server stream whose reader takes one message and then
    # waits; resolves `saturated` once the transport's only stream holds a
    # full receive window and no credit, then records retained-byte peaks.
    def flood(campaign, clients, id:, protocol:, cancellation:, expect:)
      resume = Async::Promise.new
      shape = protocol == :grpc ? :grpc_server_stream : :server_stream
      call = campaign.task.async do
        campaign.perform(clients, shape, id: id, action: "flood", count: 20, timeout: nil, cancellation: cancellation,
                                         expect: expect) do |reader|
          reader.receive
          resume.wait
          reader.each { nil }
        end
      end
      [call, resume]
    end

    def await_saturation(campaign, transport)
      last = nil
      begin
        Soak.poll("a saturated flood stream") do
          stream = Sampler.client_streams(transport).first
          last = stream && { retained: Sampler.stream_retained(stream), credit: Sampler.stream_window(stream).last }
          last && last[:retained] == WINDOW && last[:credit].zero?
        end
      rescue Failure
        raise Failure, "Flood stream never reached a full window with no receive credit; last state: #{last.inspect}"
      end
      campaign.peaks.observe("client stream retained bytes", last[:retained], cap: WINDOW)
    end
  end
end

# Scenarios 3 and 5 against an HTTP/2 server kind (:host or :falcon).
RSpec.shared_examples "soak stream retirement" do |kind|
  it "3 stream retirement: cancels, expires and interrupts calls while same-connection siblings finish (#{kind})" do
    campaign("3-stream-retirement/#{kind}",
             workload: "4 held server streams (token/deadline/complete), saturated flood + cancel, threaded " \
                       "Timeout/raise/kill, x7 sibling shapes", concurrency: 12,
             payload_sizes: [16, Soak::Topology::FLOOD_MESSAGE]) do |c|
      peer = c.peer_thread("#{kind} server")
      server = c.server(peer, kind: kind, connections: -> { 2 })
      shared = c.shared_source
      clients = c.transport(server.origin, label: "persistent transport", persistent: true, reuse: true,
                                           connections: 1)
      threaded = c.threaded(server.origin, label: "persistent threaded", persistent: true, reuse: true)
      bodies = Soak::StreamSteps.watch_outputs(self)
      c.run do
        c.perform(clients, :unary, cancellation: shared.token)
        endings = { token: ["canceled"], deadline: ["deadline_exceeded"], complete: [:ok] }
        ends = [*endings.keys, endings.keys.sample(random: c.random)]
        victims = ends.each_with_index.map do |ending, index|
          [c.ledger.next_id("victim"), ending, ConnectRPC::CancellationSource.new,
           index.even? ? :server_stream : :grpc_server_stream]
        end
        calls = victims.map do |id, ending, source, shape|
          c.task.async do
            c.perform(clients, shape, id: id, action: "hold", timeout: ending == :deadline ? 0.3 : nil,
                                      cancellation: ending == :complete ? shared.token : source.token,
                                      expect: endings.fetch(ending))
          end
        end
        c.ledger.await_entered(victims.map(&:first))
        c.concurrently(*Soak::Builders::H2_SHAPES.map do |shape|
          -> { c.perform(clients, shape, cancellation: shared.token) }
        end)
        victims.each do |id, ending, source|
          ending == :token ? source.cancel : (ending == :complete && c.gates.open(id))
        end
        calls.each(&:wait)
        victims.reject { |_, ending| ending == :complete }.each do |id, ending|
          reason = Soak.poll("server-side end of #{ending} victim #{id}") { c.ledger.ended(id) }
          allowed = ending == :token ? [:canceled] : %i[canceled deadline_exceeded]
          expect(allowed).to include(reason)
        end

        flood_source = ConnectRPC::CancellationSource.new
        flood_id = c.ledger.next_id("flood")
        protocol = %i[connect grpc].sample(random: c.random)
        flood, resume = Soak::StreamSteps.flood(c, clients, id: flood_id, protocol: protocol,
                                                            cancellation: flood_source.token, expect: ["canceled"])
        Soak::StreamSteps.await_saturation(c, clients.transport)
        c.perform(clients, :unary, cancellation: shared.token)
        flood_source.cancel
        resume.resolve(true)
        flood.wait
        expect(Soak.poll("server-side end of the flood") { c.ledger.ended(flood_id) }).to eq(:canceled)
        c.peaks.observe("output queued bytes", Soak::StreamSteps.output_peak(bodies), cap: Soak::StreamSteps::OUTPUT_CAP)

        interrupts = { timeout: "Timeout::Error", raise: Soak::StreamSteps::Interrupted.name, kill: :killed }
        ids = interrupts.keys.to_h { |how| [how, c.ledger.next_id("threaded-#{how}")] }
        threads = interrupts.to_h do |how, outcome|
          caller = Thread.new do
            next c.perform(threaded, :unary, id: ids[how], action: "hold", expect: [outcome]) unless how == :timeout

            # Timeout interrupts with an exception outside StandardError and
            # converts it at its own boundary, so the outcome is recorded here.
            Timeout.timeout(0.5) { c.perform(threaded, :unary, id: ids[how], action: "hold", expect: [outcome]) }
          rescue Timeout::Error => e
            c.ledger.outcome(ids[how], e.class.name, expect: [outcome])
          end
          [how, caller]
        end
        c.ledger.await_entered(ids.values)
        c.on_threads(-> { c.perform(threaded, :unary, cancellation: shared.token) })
        threads[:raise].raise(Soak::StreamSteps::Interrupted, "interrupted")
        threads[:kill].kill
        threads.each_value { |thread| raise Soak::Failure, "Interrupted caller did not exit" unless thread.join(10) }
        c.ledger.outcome(ids[:kill], :killed, expect: [:killed])
        ids.each_value do |id|
          expect(Soak.poll("server-side end of interrupted threaded call #{id}") { c.ledger.ended(id) })
            .to eq(:canceled)
        end
      end
    end
  end

  it "5 slow peers: non-consuming reader, non-reading TCP client and unread uploads stay bounded (#{kind})" do
    campaign("5-slow-peers/#{kind}",
             workload: "paused flood reader with sibling, raw non-reading h2c client vs close(grace: 0.3), " \
                       "ignored 4 MB upload with 0.3 s deadline", concurrency: 4,
             payload_sizes: [16, Soak::Topology::FLOOD_MESSAGE]) do |c|
      peer = c.peer_thread("#{kind} servers")
      server = c.server(peer, kind: kind, connections: -> { 1 })
      clients = c.transport(server.origin, label: "persistent transport", persistent: true, reuse: true,
                                           connections: 1)
      c.run do
        id = c.ledger.next_id("paused")
        paused, resume = Soak::StreamSteps.flood(c, clients, id: id, protocol: :connect, cancellation: nil,
                                                             expect: [:ok])
        Soak::StreamSteps.await_saturation(c, clients.transport)
        Soak::Builders::H2_SHAPES.each { |shape| c.perform(clients, shape) }
        resume.resolve(true)
        paused.wait

        upload = c.ledger.next_id("ignored-upload")
        c.perform(clients, :client_stream, id: upload, action: "ignore", timeout: 0.3,
                                           expect: ["deadline_exceeded"]) do |writer|
          20.times { writer.send_message(Soak::Topology.message(Soak::Topology::FLOOD_MESSAGE)) }
        end
        expect(%i[canceled deadline_exceeded]).to include(Soak.poll("ignored upload end") { c.ledger.ended(upload) })
        c.perform(clients, :unary)

        doomed = c.server(peer, kind: kind, persistent: false, label: "#{kind} non-reading target")
        sibling = c.transport(doomed.origin, label: "sibling transport")
        stalled = c.ledger.next_id("non-reading")
        raw = Soak::RawH2.open(doomed.port, window: (2**31) - 1)
        c.track(:io, raw.socket, "non-reading client socket")
        # 20 MB exceeds what loopback socket buffers absorb, so the handler blocks.
        raw.request(Soak::Topology::SS.path, ledger_id: stalled, action: "flood", count: 100)
        c.ledger.await_entered([stalled])
        Soak.poll("server output filling the non-reading client's socket") { raw.unread >= 65_536 }
        Soak::Builders::H2_SHAPES.each { |shape| c.perform(sibling, shape) }
        expect(sibling.transport.close(grace: 2)).to be(true)
        expect(doomed.close(grace: 0.3)).to be(false)
        expect(Soak.poll("non-reading stream's handler end") { c.ledger.ended(stalled) }).not_to eq(:ok)
        raw.close
      end
    end
  end
end
