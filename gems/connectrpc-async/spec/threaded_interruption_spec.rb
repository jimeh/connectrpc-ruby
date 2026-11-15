# frozen_string_literal: true

RSpec.describe "ThreadedTransport deadlines, interruption and cancellation" do
  def gated_host(contexts, entered, release)
    threaded_host(server: server(echo: lambda { |message, context|
      contexts[message.name] = context
      entered << message.name
      release.pop unless message.name.start_with?("free")
      message
    }))
  end

  it "fails a call at its deadline plus the allowance while the reactor is stalled, then releases admission" do
    received = []
    peer = threaded_host(server: server(echo: lambda { |message, _|
      received << message.name
      message
    }))
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    typed = client(transport, origin)
    typed.echo(AsyncHelpers::MESSAGE.new(name: "warm"))
    gate = hold_owner(transport)
    started = monotonic
    error = capture_error { typed.echo(AsyncHelpers::MESSAGE.new(name: "stalled"), timeout: 0.3) }
    duration = monotonic - started
    expect([error.class, error.code]).to eq([ConnectRPC::Error, "deadline_exceeded"])
    expect(duration).to be_between(0.3, 0.3 + 0.1 + 0.5)
    expect(admitted(transport)).to eq(1)
    gate << true
    eventually("admission released after the reactor resumes") { admitted(transport).zero? }
    expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "after")).message.name).to eq("after")
    expect([received, peer.peer.accepts.size]).to eq([%w[warm after], 1])
  ensure
    gate&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  it "reports canceled, not deadline_exceeded, for cancellation accepted while the reactor is stalled" do
    peer = threaded_host(server: server)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    typed = client(transport, origin)
    typed.echo(AsyncHelpers::MESSAGE.new(name: "warm"))
    gate = hold_owner(transport)
    source = ConnectRPC::CancellationSource.new
    caller = Thread.new do
      capture_error { typed.echo(AsyncHelpers::MESSAGE.new(name: "cancel"), timeout: 5, cancellation: source.token) }
    end
    eventually("caller admitted and waiting") { admitted(transport) == 1 && blocked?(caller) }
    started = monotonic
    Thread.new { source.cancel }.join
    error = caller.value
    expect([error.class, error.code]).to eq([ConnectRPC::Error, "canceled"])
    expect(monotonic - started).to be < 1
    gate << true
    eventually("admission released") { admitted(transport).zero? }
  ensure
    gate&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  [
    [:timeout, Timeout::Error],
    [:raise, Class.new(StandardError)]
  ].each do |kind, error_class|
    it "resets the stream when a #{kind} interrupts a blocked caller and keeps the sibling and connection" do
      contexts = {}
      entered = Thread::Queue.new
      release = Thread::Queue.new
      peer = gated_host(contexts, entered, release)
      origin = "http://127.0.0.1:#{peer.peer.port}"
      transport = threaded(origin: origin)
      typed = client(transport, origin)
      sibling = Thread.new { typed.echo(AsyncHelpers::MESSAGE.new(name: "sibling"), timeout: nil) }
      expect(entered.pop).to eq("sibling")
      victim = Thread.new do
        capture_error do
          if kind == :timeout
            # The 2 s Timeout is the interruption under test; the handler is entered long before.
            Timeout.timeout(2) { typed.echo(AsyncHelpers::MESSAGE.new(name: "victim"), timeout: nil) }
          else
            typed.echo(AsyncHelpers::MESSAGE.new(name: "victim"), timeout: nil)
          end
        end
      end
      expect(entered.pop).to eq("victim")
      victim.raise(error_class, "interrupted") if kind == :raise
      expect(victim.value).to be_a(error_class)
      eventually("actual peer CANCEL reached the handler Context") { contexts.fetch("victim").cancelled? }
      expect(contexts.fetch("victim").cancellation_reason).to eq(:canceled)
      expect(contexts.fetch("sibling")).not_to be_cancelled
      2.times { release << true }
      expect(sibling.value.message.name).to eq("sibling")
      eventually("both admissions released") { admitted(transport).zero? }
      expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "free after")).message.name).to eq("free after")
      expect(peer.peer.accepts.size).to eq(1)
    ensure
      3.times { release&.<<(true) }
      transport&.close(grace: 0)
      peer&.stop
    end
  end

  it "cancels through a token from another thread and the server observes the reset" do
    contexts = {}
    entered = Thread::Queue.new
    release = Thread::Queue.new
    peer = gated_host(contexts, entered, release)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    typed = client(transport, origin)
    source = ConnectRPC::CancellationSource.new
    caller = Thread.new do
      capture_error { typed.echo(AsyncHelpers::MESSAGE.new(name: "token"), timeout: nil, cancellation: source.token) }
    end
    expect(entered.pop).to eq("token")
    Thread.new { source.cancel }.join
    error = caller.value
    expect([error.class, error.code, error.cause]).to eq([ConnectRPC::Error, "canceled", nil])
    eventually("actual peer CANCEL reached the handler Context") { contexts.fetch("token").cancelled? }
    release << true
    eventually("admission released") { admitted(transport).zero? }
    expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "free next")).message.name).to eq("free next")
    expect(peer.peer.accepts.size).to eq(1)
  ensure
    release&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  it "detaches the lifecycle so a late reactor failure never reaches an interrupted caller's handle" do
    contexts = {}
    entered = Thread::Queue.new
    release = Thread::Queue.new
    peer = gated_host(contexts, entered, release)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    calls = Queue.new
    lifecycle = Object.new
    lifecycle.define_singleton_method(:activate) do |&block|
      calls << [:activate, Thread.current]
      block.call
    ensure
      calls << [:activated, Thread.current]
    end
    lifecycle.define_singleton_method(:record_failure) { |error| calls << [:record_failure, error] }
    body = AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new(name: "observed"))
    outgoing = request("#{origin}/example.v1.EchoService/Echo", body: body,
                                                                headers: { "content-type" => ["application/proto"] })
    outgoing.lifecycle = lifecycle
    caller = Thread.new { capture_error { transport.call(outgoing) } }
    expect(entered.pop).to eq("observed")
    started = monotonic
    caller.raise(Class.new(StandardError), "interrupted")
    expect(caller.value.message).to eq("interrupted")
    expect(monotonic - started).to be < 1
    eventually("actual peer CANCEL reached the handler Context") { contexts.fetch("observed").cancelled? }
    eventually("the reactor finished the abandoned call") { admitted(transport).zero? }
    observed = []
    observed << calls.pop until calls.empty?
    owner = engine_of(transport).thread
    expect(observed).to eq([[:activate, owner], [:activated, owner]])
  ensure
    release&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end
end
