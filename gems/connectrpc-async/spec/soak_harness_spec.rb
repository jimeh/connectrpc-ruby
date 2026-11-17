# frozen_string_literal: true

require_relative "../soak/support/harness"

# Negative controls for the lifecycle campaign harness (soak/): each leak or
# overshoot that the campaign must catch is planted in a measured round of a
# small real campaign and must fail it at the intended check.
RSpec.describe "Lifecycle campaign harness" do
  around do |example|
    Dir.mktmpdir("connectrpc-soak-report-") do |directory|
      rounds = ENV.fetch("CONNECTRPC_SOAK_ROUNDS", nil)
      ENV["CONNECTRPC_SOAK_ROUNDS"] = "2"
      Soak.report = Soak::Report.new(directory)
      example.run
    ensure
      rounds ? ENV["CONNECTRPC_SOAK_ROUNDS"] = rounds : ENV.delete("CONNECTRPC_SOAK_ROUNDS")
      Soak.report = nil
    end
  end

  # Bounds only the poll that must detect a control's planted leak; the other
  # polls keep their normal bounds, and legitimate cleanup settles before the
  # leak is observed, so a slow runner cannot fail a control at another check.
  def short = 0.5

  # Runs a campaign on its own reactor; `release` undoes the planted leak on
  # that reactor before it exits.
  def control(name, settle: Soak::SETTLE_SECONDS, global: Soak::GLOBAL_SECONDS, release: -> {}, &)
    Sync do |task|
      Soak::Campaign.new(name, task: task, settle: settle, global: global).conduct(&)
    ensure
      release.call
    end
  end

  def measured?(campaign) = campaign.round_phase == :round

  it "passes a clean campaign whose rounds create and retire real clients" do
    control("clean") do |c|
      server = c.server(c.peer_thread("host"))
      c.run do
        clients = c.transport(server.origin, label: "round transport")
        c.perform(clients, :unary)
        clients.transport.close(grace: 1)
      end
    end
  end

  it "detects a descriptor leaked in a measured round" do
    pipes = []
    expect { control("leaked fd", global: short) { |c| c.run { pipes << IO.pipe if measured?(c) } } }
      .to raise_error(Soak::Failure, /global counts differ from the warmup baseline: \{fds: /)
  ensure
    pipes.flatten.each(&:close)
  end

  it "detects a thread leaked in a measured round" do
    gate = Thread::Queue.new
    threads = []
    expect do
      control("leaked thread", global: short) { |c| c.run { threads << Thread.new { gate.pop } if measured?(c) } }
    end
      .to raise_error(Soak::Failure, /global counts differ from the warmup baseline: \{threads: /)
  ensure
    gate.close
    threads.each(&:join)
  end

  it "detects an Async task leaked in a measured round" do
    tasks = []
    condition = Async::Condition.new
    expect do
      control("leaked task", global: short, release: -> { tasks.each(&:stop) }) do |c|
        c.run { tasks << c.task.async { condition.wait } if measured?(c) }
      end
    end.to raise_error(Soak::Failure, /global counts differ from the warmup baseline: \{harness_tasks: /)
  end

  it "detects a cancellation listener left registered on a shared source" do
    expect do
      control("leaked registration", settle: short) do |c|
        source = c.shared_source
        c.run { source.token.on_cancel { nil } if measured?(c) }
      end
    end.to raise_error(Soak::Failure, /shared cancellation source has 1 registrations/)
  end

  it "detects an unretired resource that no tracked client or server owns" do
    leaked = nil
    expect do
      control("unretired resource", settle: short, release: -> { leaked&.close(grace: 0) }) do |c|
        server = c.server(c.peer_thread("host"))
        c.run do
          next unless measured?(c)

          leaked = ConnectRPC::Async::Transport.new(origin: server.origin)
          c.perform(Soak::Topology::Clients.new(leaked, server.origin), :unary)
        end
      end
    end.to raise_error(Soak::Failure, /unretired ConnectRPC::Async::Internal::Resource \d+ owned by no live client/)
  end

  it "detects a round-scoped client left open at the end of its round" do
    expect do
      control("open round client", settle: short) do |c|
        server = c.server(c.peer_thread("host"))
        c.run do
          clients = c.transport(server.origin, label: "forgotten transport")
          c.perform(clients, :unary)
          clients.transport.close(grace: 1) unless measured?(c)
          c.settle!("legitimate retirement", timeout: Soak::SETTLE_SECONDS)
        end
      end
    end.to raise_error(Soak::Failure, /forgotten transport still open at the end of its round/)
  end

  it "detects a transient socket cap overshoot that is gone by the end of the round" do
    expect do
      control("socket overshoot") do |c|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        c.cleanup { bound.close }
        release = Async::Queue.new
        # One stream per connection, so a second concurrent call dials again.
        settings = { Protocol::HTTP2::Settings::MAXIMUM_CONCURRENT_STREAMS => 1 }
        peer = ConnectionPeer::RecordingServer.new(c.task, bound.sockets.first, settings: settings) do |incoming|
          release.dequeue
          incoming.send_response(Protocol::HTTP::Response[200, {}, ["held"]])
        rescue Protocol::HTTP2::Error, IOError
          nil
        end
        c.cleanup { peer.stop }
        origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
        c.run do
          clients = c.transport(origin, label: "round transport", connection_limit: 2)
          meter = ConnectionPeer::SocketMeter.new(clients.transport)
          calls = Array.new(2) { c.task.async { clients.transport.call(request("#{origin}/held")) } }
          Soak.poll("two client sockets") { meter.peak == 2 }
          2.times { release.enqueue(true) }
          calls.each(&:wait)
          clients.transport.close(grace: 1)
          c.peaks.observe("client sockets", meter.peak, cap: 1)
        end
      end
    end.to raise_error(Soak::Failure, /client sockets peaked at 2, above its cap 1/)
  end

  it "detects retained stream bytes above their cap while a reader is paused" do
    expect do
      control("retained stream bytes") do |c|
        server = c.server(c.peer_thread("host"))
        c.run do
          clients = c.transport(server.origin, label: "round transport")
          resume = Async::Promise.new
          call = c.task.async do
            c.perform(clients, :server_stream, action: "flood", count: 20) do |reader|
              reader.receive
              resume.wait
              reader.each { nil }
            end
          end
          retained = Soak.poll("retained stream bytes") do
            stream = Soak::Sampler.client_streams(clients.transport).first
            bytes = stream && Soak::Sampler.stream_retained(stream)
            bytes if bytes && bytes > 16_384
          end
          c.peaks.observe("client stream retained bytes", retained, cap: 16_384)
          resume.resolve(true)
          call.wait
          clients.transport.close(grace: 1)
        end
      end
    end.to raise_error(Soak::Failure, /client stream retained bytes peaked at \d+, above its cap 16384/)
  end

  it "detects a reused persistent connection replaced by another" do
    expect do
      control("reuse identity", settle: short) do |c|
        server = c.server(c.peer_thread("host"))
        relay = c.relay(server.port)
        clients = c.transport("http://127.0.0.1:#{relay.port}", label: "persistent transport", persistent: true,
                                                                reuse: true)
        transport = clients.transport
        c.run do
          if measured?(c)
            relay.reset!
            Soak.poll("the reset connection to leave the transport") { Soak::Sampler.live_connections(transport).empty? }
          end
          c.perform(clients, :unary)
          Soak.settle("the transport's own state", timeout: Soak::SETTLE_SECONDS) do
            Soak::Sampler.transport_problems(transport, "persistent transport")
          end
        end
      end
    end.to raise_error(Soak::Failure, /persistent transport connections changed from \[\d+\] to \[\d+\]/)
  end

  # An H2 resource with no connection, a closed raw socket and no tracked
  # tasks, as a superseded one looks, still owns a running task and never
  # completed retirement. Created in setup, its task joins the baseline.
  it "detects an unretired resource that only looks superseded" do
    owner = nil
    expect do
      control("looks superseded", settle: short, release: -> { Soak.stop(owner) }) do |c|
        condition = Async::Condition.new
        owner = c.task.async { condition.wait }
        raw = Socket.pair(:UNIX, :STREAM).each(&:close).first
        resource = Soak::Sampler::INTERNAL::Resource.new(progress: Async::Condition.new)
        resource.raw = raw
        resource.task = owner
        c.run { nil }
      end
    end.to raise_error(Soak::Failure, /unretired ConnectRPC::Async::Internal::Resource \d+ owned by no live client/)
  end

  it "detects a logical call that a handler executed twice" do
    expect do
      control("duplicate execution") do |c|
        server = c.server(c.peer_thread("host"))
        clients = c.transport(server.origin, label: "persistent transport", persistent: true)
        c.run do
          id = c.ledger.next_id("call")
          c.perform(clients, :unary, id: id)
          c.perform(clients, :unary, id: id) if measured?(c)
        end
      end
    end.to raise_error(Soak::Failure, /round 0 invariants:\n  call-\d+ executed 2 times/)
  end

  it "fails warmup as unstable when every round leaks" do
    pipes = []
    expect { control("unstable") { |c| c.run { pipes << IO.pipe } } }
      .to raise_error(Soak::Unstable, /unstable did not stabilize in 3 warmups: \{.*fds: \d+.*\} then \{.*fds: \d+/)
  ensure
    pipes.flatten.each(&:close)
  end

  it "runs every cleanup after a failing one and reports the first error" do
    ran = []
    expect do
      control("failing cleanup") do |c|
        c.cleanup(:stop) { ran << :stop }
        c.cleanup { ran << :close }
        c.cleanup { raise "later close failure" }
        c.cleanup { raise "first close failure" }
        c.run { nil }
      end
    end.to raise_error(RuntimeError, "first close failure")
    expect(ran).to eq(%i[close stop])
  end
end
