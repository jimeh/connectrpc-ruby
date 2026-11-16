# frozen_string_literal: true

RSpec.describe "ThreadedTransport keepalive" do
  def keepalive_policy(permit: false)
    keepalive = ConnectRPC::Async::Keepalive.new(time: 10, timeout: 5, permit_without_calls: permit)
    ConnectRPC::Async::ConnectionPolicy.new(keepalive: keepalive)
  end

  def inner_resources(transport)
    _, resources = on_owner(transport) { engine_of(transport).inner.instance_variable_get(:@resources).dup }
    resources
  end

  # Retires the warm-up connection, whose monitor read the real clock, so the
  # next call opens one timed by the installed clock.
  def install_keepalive_clock(transport)
    clock = install_thread_clock(transport, ThreadedHelpers::AdvanceClock.new)
    on_owner(transport) { engine_of(transport).inner.instance_variable_get(:@resources).each(&:abort) }
    clock
  end

  def payload(sequence) = [sequence].pack("Q>")

  def held_peer(release, invocations)
    threaded_recording_peer do |incoming|
      invocations << incoming.path
      release.pop if incoming.path.start_with?("/long")
      incoming.send_response(Protocol::HTTP::Response[200, {}, [incoming.path]])
    rescue Protocol::HTTP2::Error, IOError
      nil
    end
  end

  it "retires a black-holed connection and fails the waiting caller once" do
    release = Thread::Queue.new
    invocations = Thread::Queue.new
    peer = held_peer(release, invocations)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    events = Thread::Queue.new
    transport = threaded(origin: origin, connection_policy: keepalive_policy,
                         connection_observer: ->(event) { events << event })
    expect(transport.call(request("#{origin}/warm")).body).to eq("/warm")
    clock = install_keepalive_clock(transport)
    pop_until(events, "warm connection closed") { |event| event.type == :connection_closed }
    peer.peer.ack_pings = false
    caller = Thread.new { capture_error { transport.call(request("#{origin}/long")) } }
    pop_until(invocations, "long call invoked") { |path| path == "/long" }
    eventually("monitor sleeping") { clock.dues == [10.0] }
    clock.advance(10)
    eventually("keepalive PING on the wire") { peer.peer.ping_payloads(1) == [payload(1)] }
    eventually("probe deadline armed") { clock.dues == [15.0] }
    clock.advance(5)
    error = value_within(caller, "the black-holed caller")
    expect([error.class, error.code]).to eq([ConnectRPC::Error, "unavailable"])
    closed = pop_until(events, "keepalive retirement reported") { |event| event.type == :connection_closed }
    expect(closed.outcome).to eq(:keepalive_timeout)
    expect(transport.call(request("#{origin}/after")).body).to eq("/after")
    expect([peer.peer.accepted, peer.peer.headers_frames.size]).to eq([3, 3])
  ensure
    3.times { release&.<<(true) }
    transport&.close(grace: 0)
    peer&.stop
  end

  it "keeps pinging after the calling thread returns and joins the monitor at close" do
    invocations = Thread::Queue.new
    peer = held_peer(Thread::Queue.new, invocations)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin, connection_policy: keepalive_policy(permit: true))
    expect(transport.call(request("#{origin}/warm")).body).to eq("/warm")
    clock = install_keepalive_clock(transport)
    expect(Thread.new { transport.call(request("#{origin}/one")).body }.value).to eq("/one")
    resource = inner_resources(transport).last
    _, monitor = on_owner(transport) { resource.instance_variable_get(:@maintenance).first }
    eventually("monitor sleeping") { clock.dues == [10.0] }
    clock.advance(10)
    eventually("idle PING after the call returned") { peer.peer.ping_payloads(1) == [payload(1)] }
    eventually("next probe scheduled from the ACK") { clock.dues == [20.0] }
    expect(monitor).not_to be_finished
    expect(transport.close(grace: 5)).to be(true)
    expect(monitor).to be_finished
    expect(peer.peer.goaways.map(&:first)).to eq([1])
  ensure
    transport&.close(grace: 0)
    peer&.stop
  end
end
