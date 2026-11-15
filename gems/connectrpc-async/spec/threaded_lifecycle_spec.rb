# frozen_string_literal: true

require "rbconfig"

RSpec.describe "ThreadedTransport close, owner failure and recovery" do
  def gated(entered, release)
    server(echo: lambda { |message, _|
      entered << message.name
      release.pop unless message.name.start_with?("free")
      message
    })
  end

  it "drains an in-flight call gracefully, shares the outcome and refuses later calls" do
    entered = Thread::Queue.new
    release = Thread::Queue.new
    peer = threaded_host(server: gated(entered, release))
    port = peer.peer.port
    origin = "http://127.0.0.1:#{port}"
    transport = threaded(origin: origin)
    typed = client(transport, origin)
    pending = Thread.new { typed.echo(AsyncHelpers::MESSAGE.new(name: "pending")) }
    pop_within(entered, "the pending call in the handler")
    closers = 2.times.map { Thread.new { transport.close(grace: 5) } }
    eventually("closers waiting") { transport.connection_state == :closing && closers.all? { |t| blocked?(t) } }
    expect { typed.echo(AsyncHelpers::MESSAGE.new(name: "free late")) }.to unavailable_error
    release << true
    expect(value_within(pending, "the drained call").message.name).to eq("pending")
    expect(closers.map { |closer| value_within(closer, "a closer") }).to eq([true, true])
    expect([transport.close(grace: 0), transport.connection_state, owner_threads(port)]).to eq([true, :closed, []])
    expect { typed.echo(AsyncHelpers::MESSAGE.new(name: "free after")) }.to unavailable_error
  ensure
    release&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  it "forces teardown with zero grace and reports false" do
    entered = Thread::Queue.new
    release = Thread::Queue.new
    peer = threaded_host(server: gated(entered, release))
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    pending = Thread.new { capture_error { client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "cut")) } }
    pop_within(entered, "the cut call in the handler")
    expect(transport.close(grace: 0)).to be(false)
    error = value_within(pending, "the cut call")
    expect([error.class, error.code]).to eq([ConnectRPC::Error, "unavailable"])
    expect(admitted(transport)).to eq(0)
  ensure
    release&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  it "returns false for a stalled owner after grace plus the allowance and wakes pending callers" do
    peer = threaded_host(server: server)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    typed = client(transport, origin)
    typed.echo(AsyncHelpers::MESSAGE.new(name: "warm"))
    owner = engine_of(transport).thread
    gate = hold_owner(transport)
    pending = Thread.new { capture_error { typed.echo(AsyncHelpers::MESSAGE.new(name: "stuck"), timeout: nil) } }
    eventually("caller admitted and waiting") { admitted(transport) == 1 && blocked?(pending) }
    started = monotonic
    expect(transport.close(grace: 0.2)).to be(false)
    expect(monotonic - started).to be_between(0.3, 1.0)
    error = value_within(pending, "the stalled caller")
    expect([error.class, error.code]).to eq([ConnectRPC::Error, "unavailable"])
    expect(owner).to be_alive
    gate << true
    expect(owner.join(5)).to equal(owner)
    expect([transport.close, admitted(transport), engine_of(transport).state]).to eq([false, 0, :stopped])
  ensure
    gate&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  it "re-raises only in an interrupted closer while another closer receives the shared outcome" do
    peer = threaded_host(server: server)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "warm"))
    gate = hold_owner(transport)
    interrupt = Class.new(StandardError)
    closers = 2.times.map { Thread.new { capture_error { transport.close(grace: 1) } || :returned } }
    eventually("both closers waiting") { transport.connection_state == :closing && closers.all? { |t| blocked?(t) } }
    closers.first.raise(interrupt, "closer interrupted")
    expect(value_within(closers.first, "the interrupted closer")).to be_a(interrupt)
    expect(transport.connection_state).to eq(:closing)
    expect(value_within(closers.last, "the other closer")).to eq(:returned)
    expect(transport.close).to be(false)
  ensure
    gate&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  it "fails pending callers with unavailable when the owner fails and restarts on the next call" do
    entered = Thread::Queue.new
    release = Thread::Queue.new
    peer = threaded_host(server: gated(entered, release))
    origin = "http://127.0.0.1:#{peer.peer.port}"
    events = Thread::Queue.new
    transport = threaded(origin: origin, connection_observer: ->(event) { events << event })
    typed = client(transport, origin)
    pending = Thread.new { capture_error { typed.echo(AsyncHelpers::MESSAGE.new(name: "pending"), timeout: nil) } }
    pop_within(entered, "the pending call in the handler")
    failed = engine_of(transport)
    started = monotonic
    failed.run_on_owner(-> { raise "injected owner failure" })
    error = value_within(pending, "the pending caller after the owner failed")
    expect([error.class, error.code]).to eq([ConnectRPC::Error, "unavailable"])
    expect(monotonic - started).to be < 1
    pop_until(events, "closed state event") { |event| event.type == :state_changed && event.state == :closed }
    expect(failed.thread.join(5)).to equal(failed.thread)
    expect([transport.connection_state, admitted(transport)]).to eq([:idle, 0])
    expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "free restarted")).message.name).to eq("free restarted")
    expect(engine_of(transport).thread).not_to equal(failed.thread)
    expect(peer.peer.accepts.size).to eq(2)
  ensure
    release&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  it "fails pending callers with unavailable when the owner thread is killed and restarts" do
    entered = Thread::Queue.new
    release = Thread::Queue.new
    peer = threaded_host(server: gated(entered, release))
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    typed = client(transport, origin)
    pending = Thread.new { capture_error { typed.echo(AsyncHelpers::MESSAGE.new(name: "pending"), timeout: nil) } }
    pop_within(entered, "the pending call in the handler")
    killed = engine_of(transport)
    killed.thread.kill
    error = value_within(pending, "the pending caller after the owner was killed", timeout: 5)
    expect([error.class, error.code]).to eq([ConnectRPC::Error, "unavailable"])
    expect(killed.thread.join(5)).to equal(killed.thread)
    expect(killed.state).to eq(:stopped)
    expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "free restarted")).message.name).to eq("free restarted")
    expect(engine_of(transport)).not_to equal(killed)
  ensure
    release&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  # The trace holds the owner inside settle, after reactor_done is set but
  # before the reply is published, while the kill arrives.
  it "finishes settling a reply when the owner is killed inside the settle bookkeeping" do
    peer = threaded_host(server: server)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    typed = client(transport, origin)
    typed.echo(AsyncHelpers::MESSAGE.new(name: "warm"))
    killed = engine_of(transport)
    pause = ThreadedHelpers::OwnerPause.new(killed, :settle, "call.reactor_done = true")
    caller = Thread.new { typed.echo(AsyncHelpers::MESSAGE.new(name: "settled"), timeout: nil) }
    pop_within(pause.held, "the owner to pause inside settle")
    killed.thread.kill
    pause.open
    reply = value_within(caller, "the caller after the owner was killed inside settle", timeout: 5)
    expect(reply.message.name).to eq("settled")
    expect(killed.thread.join(5)).to equal(killed.thread)
    expect([killed.state, killed.admission.count, killed.admission.bytes, killed.calls.size]).to eq([:stopped, 0, 0, 0])
    expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "replacement")).message.name).to eq("replacement")
    expect(engine_of(transport)).not_to equal(killed)
  ensure
    pause&.open
    transport&.close(grace: 0)
    peer&.stop
  end

  # The trace holds the owner inside release, after the count is returned but
  # before the bytes and mailbox, while the kill arrives.
  it "finishes releasing an abandoned call when the owner is killed inside the release bookkeeping" do
    entered = Thread::Queue.new
    release = Thread::Queue.new
    peer = threaded_host(server: gated(entered, release))
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    typed = client(transport, origin)
    typed.echo(AsyncHelpers::MESSAGE.new(name: "free warm"))
    killed = engine_of(transport)
    caller = Thread.new { typed.echo(AsyncHelpers::MESSAGE.new(name: "abandoned"), timeout: nil) }
    pop_until(entered, "the abandoned call in the handler") { |name| name == "abandoned" }
    pause = ThreadedHelpers::OwnerPause.new(killed, :release, "@admission.count -= 1")
    caller.kill
    expect(caller.join(5)).to equal(caller)
    pop_within(pause.held, "the owner to pause inside release")
    killed.thread.kill
    pause.open
    expect(killed.thread.join(5)).to equal(killed.thread)
    expect([killed.state, killed.admission.count, killed.admission.bytes, killed.calls.size]).to eq([:stopped, 0, 0, 0])
    expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "free replacement")).message.name).to eq("free replacement")
    expect(engine_of(transport)).not_to equal(killed)
  ensure
    pause&.open
    release&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  it "lets a process that never closes the transport exit promptly" do
    peer = threaded_host(server: server)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    root = File.expand_path("../../..", __dir__)
    script = <<~RUBY
      require "connectrpc/async"
      transport = ConnectRPC::Async::ThreadedTransport.new(origin: ARGV.fetch(0))
      request = ConnectRPC::TransportRequest.new(url: URI("\#{ARGV.fetch(0)}/missing"), http_method: "POST",
                                                 headers: {}, body: "", response_limit: 1024)
      puts transport.call(request).status
      $stdout.flush
    RUBY
    output, writer = IO.pipe
    errors, error_writer = IO.pipe
    pid = Process.spawn(RbConfig.ruby, "-I", File.join(root, "lib"), "-I", File.join(root, "gems/connectrpc-async/lib"),
                        "-e", script, origin, out: writer, err: error_writer)
    [writer, error_writer].each(&:close)
    waiter = Process.detach(pid)
    status = waiter.join(15)&.value
    Process.kill("KILL", pid) unless status
    # Stderr is diagnostic only; Ruby 4 warns that IO::Buffer is experimental.
    diagnostics = errors.read
    expect([status&.success?, output.read]).to eq([true, "404\n"]), diagnostics
  ensure
    Process.kill("KILL", pid) if waiter&.alive?
    [output, errors].each { |io| io&.close }
    peer&.stop
  end

  it "fails fast during backoff, recovers after a restart and serves a wait-for-ready caller" do
    peer = threaded_host(server: server)
    port = peer.peer.port
    origin = "http://127.0.0.1:#{port}"
    fast = threaded(origin: origin)
    ready = threaded(origin: origin,
                     connection_policy: ConnectRPC::Async::ConnectionPolicy.new(wait_for_ready: true))
    [fast, ready].each { |transport| client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "warm")) }
    fast_clock = install_thread_clock(fast)
    ready_clock = install_thread_clock(ready)
    peer.stop
    peer = nil
    eventually("both connections lost") { [fast, ready].map(&:connection_state) == %i[idle idle] }
    expect { client(fast, origin).echo(AsyncHelpers::MESSAGE.new(name: "refused")) }.to unavailable_error
    fast_clock.next_delay
    expect(fast.connection_state).to eq(:transient_failure)
    started = monotonic
    expect { client(fast, origin).echo(AsyncHelpers::MESSAGE.new(name: "backoff")) }.to unavailable_error
    expect(monotonic - started).to be < 1
    waiting = Thread.new { client(ready, origin).echo(AsyncHelpers::MESSAGE.new(name: "waited"), timeout: 10) }
    ready_clock.next_delay
    expect([ready.connection_state, waiting.alive?]).to eq([:transient_failure, true])
    peer = threaded_host(server: server, port: port)
    [fast_clock, ready_clock].each(&:fire)
    expect(value_within(waiting, "the wait-for-ready caller").message.name).to eq("waited")
    expect(client(fast, origin).echo(AsyncHelpers::MESSAGE.new(name: "recovered")).message.name).to eq("recovered")
    expect(peer.peer.accepts.size).to eq(2)
  ensure
    [fast, ready].each { |transport| transport&.close(grace: 0) }
    peer&.stop
  end
end
