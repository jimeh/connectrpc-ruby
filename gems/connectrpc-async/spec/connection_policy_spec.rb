# frozen_string_literal: true

RSpec.describe ConnectRPC::Async::ConnectionPolicy do
  it "defaults to Go-like values as frozen comparable values" do
    policy = described_class.new
    expect(policy.backoff).to eq(ConnectRPC::Async::Backoff.new(base: 1.0, multiplier: 1.6, jitter: 0.2, max: 30.0))
    expect([policy.wait_for_ready, policy.max_pending_bytes]).to eq([false, 16 * 1024 * 1024])
    expect([policy.retry_refused, policy.max_draining, policy.drain_timeout]).to eq([true, 1, nil])
    expect(policy.keepalive).to be_nil
    expect([policy, policy.backoff]).to all(be_frozen)
    expect(policy).to eq(described_class.new)
    expect(policy.hash).to eq(described_class.new.hash)
    expect(policy).not_to eq(described_class.new(max_pending_bytes: nil))
    [{ retry_refused: false }, { max_draining: 2 }, { drain_timeout: 30 },
     { keepalive: ConnectRPC::Async::Keepalive.new(time: 300) }].each do |options|
      expect(described_class.new(**options)).not_to eq(policy)
      expect(described_class.new(**options)).to eq(described_class.new(**options))
      expect(described_class.new(**options).hash).to eq(described_class.new(**options).hash)
    end
  end

  it "turns each feature off" do
    policy = described_class.new(backoff: nil, max_pending_bytes: nil, retry_refused: false, max_draining: 0)
    expect([policy.backoff, policy.max_pending_bytes, policy.wait_for_ready]).to eq([nil, nil, false])
    expect([policy.retry_refused, policy.max_draining, policy.drain_timeout]).to eq([false, 0, nil])
    expect(described_class.new(drain_timeout: 0.25).drain_timeout).to eq(0.25)
    expect(described_class.new(wait_for_ready: true).wait_for_ready).to be(true)
  end

  [
    [{ backoff: :fast }, "backoff must be a Backoff or nil"],
    [{ wait_for_ready: nil }, "wait_for_ready must be true or false"],
    [{ wait_for_ready: true, backoff: nil }, "wait_for_ready requires a backoff"],
    [{ max_pending_bytes: 0 }, "max_pending_bytes must be a positive integer or nil"],
    [{ max_pending_bytes: 1.5 }, "max_pending_bytes must be a positive integer or nil"],
    [{ retry_refused: nil }, "retry_refused must be true or false"],
    [{ max_draining: -1 }, "max_draining must be a nonnegative integer"],
    [{ max_draining: 1.0 }, "max_draining must be a nonnegative integer"],
    [{ drain_timeout: 0 }, "drain_timeout must be finite positive seconds"],
    [{ drain_timeout: Float::INFINITY }, "drain_timeout must be finite positive seconds"],
    [{ drain_timeout: false }, "drain_timeout must be finite positive seconds"],
    [{ keepalive: 300 }, "keepalive must be a Keepalive or nil"]
  ].each do |options, message|
    it "rejects #{options.inspect}" do
      expect { described_class.new(**options) }.to raise_error(ArgumentError, message)
    end
  end

  [
    [{ base: 0 }, /base/], [{ base: Float::INFINITY }, /base/], [{ base: "1" }, /base/],
    [{ multiplier: 0.99 }, /multiplier/], [{ multiplier: Float::NAN }, /multiplier/],
    [{ jitter: -0.1 }, /jitter/], [{ jitter: 1 }, /jitter/], [{ jitter: Complex(0.1, 0) }, /jitter/],
    [{ base: 2, max: 1 }, /max/], [{ max: Float::INFINITY }, /max/]
  ].each do |options, message|
    it "rejects backoff #{options.inspect}" do
      expect { ConnectRPC::Async::Backoff.new(**options) }.to raise_error(ArgumentError, message)
    end
  end

  it "keeps Keepalive as an alias of core's class" do
    expect(ConnectRPC::Async::Keepalive).to equal(ConnectRPC::Keepalive)
    expect(ConnectRPC::Async::ConnectionPolicy.new(keepalive: ConnectRPC::Keepalive.new(time: 1)).keepalive)
      .to eq(ConnectRPC::Async::Keepalive.new(time: 1))
  end

  it "builds a frozen comparable keepalive with grpc-go's timeout default" do
    keepalive = ConnectRPC::Async::Keepalive.new(time: 300)
    expect([keepalive.time, keepalive.timeout, keepalive.permit_without_calls]).to eq([300, 20, false])
    expect(keepalive).to be_frozen
    expect(keepalive).to eq(ConnectRPC::Async::Keepalive.new(time: 300, timeout: 20, permit_without_calls: false))
    expect(keepalive.hash).to eq(ConnectRPC::Async::Keepalive.new(time: 300).hash)
    expect(keepalive).not_to eq(ConnectRPC::Async::Keepalive.new(time: 300, permit_without_calls: true))
    expect(keepalive).not_to eq(ConnectRPC::Async::Keepalive.new(time: 300, timeout: 1))
    expect(keepalive).not_to eq(Object.new)
    expect(ConnectRPC::Async::Keepalive.new(time: 0.5, timeout: 0.25).time).to eq(0.5)
  end

  [
    [{}, ArgumentError, /time/], [{ time: 0 }, ArgumentError, /time must be finite positive seconds/],
    [{ time: Float::INFINITY }, ArgumentError, /time/], [{ time: "1" }, ArgumentError, /time/],
    [{ time: 1, timeout: 0 }, ArgumentError, /timeout must be finite positive seconds/],
    [{ time: 1, timeout: Float::NAN }, ArgumentError, /timeout/],
    [{ time: 1, permit_without_calls: nil }, ArgumentError, "permit_without_calls must be true or false"]
  ].each do |options, error, message|
    it "rejects keepalive #{options.inspect}" do
      expect { ConnectRPC::Async::Keepalive.new(**options) }.to raise_error(error, message)
    end
  end

  it "accepts a constant delay and zero jitter" do
    backoff = ConnectRPC::Async::Backoff.new(base: 2, multiplier: 1, jitter: 0, max: 2)
    expect([backoff.base, backoff.multiplier, backoff.jitter, backoff.max]).to eq([2, 1, 0, 2])
  end
end

RSpec.describe "HTTP/2 Transport connection options and observation" do
  it "validates the policy and observer at construction" do
    reactor do
      origin = "http://127.0.0.1:1"
      expect do
        ConnectRPC::Async::Transport.new(origin: origin, connection_policy: nil)
      end.to raise_error(ArgumentError, "connection_policy must be a ConnectionPolicy")
      expect do
        ConnectRPC::Async::Transport.new(origin: origin, connection_observer: Object.new)
      end.to raise_error(ArgumentError, "connection_observer must respond to call")
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      expect(transport.connection_state).to eq(:idle)
      expect(transport.inspect).to include("connection_state=idle")
      expect(transport.close(grace: 0)).to be(false)
      expect(transport.connection_state).to eq(:closed)
    end
  end

  it "publishes frozen fixed-field events after bookkeeping and ignores a raising observer" do
    reactor do
      host = ConnectRPC::Async::Host.new(server: server).start
      origin = "http://127.0.0.1:#{host.port}"
      events = []
      states = []
      transport = nil
      observer = lambda do |event|
        events << event
        states << transport.connection_state
        raise "observer failure"
      end
      transport = ConnectRPC::Async::Transport.new(origin: origin, connection_observer: observer)
      expect(client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "ok")).message.name).to eq("ok")
      transport.close(grace: 1)
      expect(events).to all(be_frozen)
      expect(events.map { |event| event.to_h.keys }.uniq).to eq(
        [%i[type state previous_state outcome duration active draining pending_calls pending_bytes]]
      )
      # The waiting call leaves before its lease, and close's drain removes
      # the connection before the closed state.
      expect(events.map { |event| [event.type, event.previous_state, event.state, event.outcome] }).to eq(
        [[:state_changed, :idle, :connecting, nil], [:state_changed, :connecting, :ready, nil],
         [:attempt_finished, nil, :ready, :ready], [:counts_changed, nil, :ready, nil],
         [:state_changed, :ready, :closing, nil], [:counts_changed, nil, :closing, nil],
         [:state_changed, :closing, :closed, nil]]
      )
      expect(states).to eq(events.map(&:state))
      expect(events.map { |event| [event.active, event.draining, event.pending_calls] }).to eq(
        [[0, 0, 1], [1, 0, 1], [1, 0, 1], [1, 0, 0], [1, 0, 0], [0, 0, 0], [0, 0, 0]]
      )
      ready = events[2]
      expect([ready.active, ready.draining, ready.pending_calls, ready.pending_bytes]).to eq([1, 0, 1, 4])
      expect(ready.duration).to be >= 0
    ensure
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  it "rejects reentrant calls and close from an observer while allowing observation" do
    reactor do
      host = ConnectRPC::Async::Host.new(server: server).start
      origin = "http://127.0.0.1:#{host.port}"
      outcomes = []
      transport = nil
      observer = lambda do |event|
        next unless event.type == :attempt_finished

        outcomes << transport.connection_state << transport.inspect.include?("connection_state=ready")
        stream = ConnectRPC::StreamRequest.new(url: URI(origin), http_method: "POST", headers: {})
        reentries = [-> { client(transport, origin).echo(AsyncHelpers::MESSAGE.new) },
                     -> { transport.close(grace: 0) },
                     -> { transport.open_stream(stream) { nil } },
                     -> { transport.open_grpc_stream(stream) { nil } }]
        reentries.each do |reentry|
          reentry.call
          outcomes << :entered
        rescue AsyncHelpers::LIFECYCLE_ERROR => e
          outcomes << e.message
        end
      end
      transport = ConnectRPC::Async::Transport.new(origin: origin, connection_observer: observer)
      expect(client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "ok")).message.name).to eq("ok")
      expect(outcomes).to eq([:ready, true,
                              *(["Transport calls and close are not allowed from a connection observer"] * 4)])
      expect(transport.instance_variable_get(:@calls)).to be_empty
      expect(transport.close(grace: 1)).to be(true)
    ensure
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end
end
