# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../../../spec/fixtures/generation/ruby", __dir__))
require "generation/streaming_connect"
require_relative "support/threaded_stream_helpers"
require_relative "support/per_call_helpers"

RSpec.describe "PerCallTransport connection close, close budget and outcome precedence" do
  include ThreadedStreamHelpers
  include PerCallHelpers

  def no_error_goaway = [0, 0, Protocol::HTTP2::Error::NO_ERROR, ""]

  PerCallHelpers::PATHS.each do |path|
    it "closes a cooperative peer's connection with GOAWAY(NO_ERROR) and waits for its EOF on the #{path} path" do
      peer = threaded_recording_peer
      origin = "http://127.0.0.1:#{peer.peer.port}"
      transport = per_call
      with_boundaries do |boundaries|
        response = on_path(path) { transport.call(request("#{origin}/ok")) }
        expect([response.status, response.body]).to eq([200, "ok"])
        expect_released(boundaries.take.first)
        expect([peer.peer.accepted, peer.peer.goaways]).to eq([1, [no_error_goaway]])
        # The client read the peer's EOF, so the peer had already ended the connection.
        eventually("the peer's connection closed") { peer.peer.connections.first.closed? }
      end
    ensure
      peer&.stop
    end

    it "keeps a successful result when the peer withholds FIN, bounded by the cleanup allowance, on the #{path} path" do
      peer = withholding_peer
      origin = "http://127.0.0.1:#{peer.peer.port}"
      transport = per_call
      with_boundaries do |boundaries|
        response, elapsed = on_path(path) do
          started = monotonic
          [transport.call(request("#{origin}/ok", deadline: ConnectRPC::Deadline.new(5))), monotonic - started]
        end
        expect([response.status, response.body]).to eq([200, "ok"])
        # The drain waited its whole 100 ms budget for a FIN that never came.
        expect(elapsed).to be_between(AsyncHelpers::INTERNAL::CLEANUP_SECONDS, 1)
        expect_released(boundaries.take.first)
        expect(peer.peer.goaways).to eq([no_error_goaway])
      end
    ensure
      peer&.stop
    end

    # A controlled clock leaves 10 ms of the deadline when the response is
    # sent, so the close budget is that remainder rather than the 100 ms
    # cleanup allowance, and the transport still returns its result.
    it "bounds the close by the deadline's remainder on the #{path} path" do
      now = [0.0]
      deadline = ConnectRPC::Deadline.new(5, clock: -> { now.first })
      sent = Thread::Queue.new
      peer = withholding_peer do |incoming|
        now[0] = 4.99
        sent << monotonic
        incoming.send_response(Protocol::HTTP::Response[200, {}, ["ok"]])
      end
      origin = "http://127.0.0.1:#{peer.peer.port}"
      transport = per_call
      with_boundaries do |boundaries|
        response, returned = on_path(path) { [transport.call(request("#{origin}/ok", deadline: deadline)), monotonic] }
        expect([response.status, response.body]).to eq([200, "ok"])
        expect(returned - pop_within(sent, "the response")).to be < 0.08
        expect_released(boundaries.take.first)
        expect(peer.peer.goaways).to eq([no_error_goaway])
      end
    ensure
      peer&.stop
    end

    it "closes raw, without GOAWAY, once the deadline has passed on the #{path} path" do
      now = [0.0]
      deadline = ConnectRPC::Deadline.new(5, clock: -> { now.first })
      peer = withholding_peer do |incoming|
        now[0] = 6
        incoming.send_response(Protocol::HTTP::Response[200, {}, ["ok"]])
      end
      origin = "http://127.0.0.1:#{peer.peer.port}"
      transport = per_call
      with_boundaries do |boundaries|
        error = on_path(path) { transport.call(request("#{origin}/late", deadline: deadline)) }
        expect([error.class, error.code]).to eq([ConnectRPC::Error, "deadline_exceeded"])
        expect_released(boundaries.take.first)
        expect([peer.peer.accepted, peer.peer.goaways]).to eq([1, []])
      end
    ensure
      peer&.stop
    end

    it "never lets a close failure replace the result or a stream block's own exception on the #{path} path" do
      peer = threaded_host(server: mixed_server)
      origin = "http://127.0.0.1:#{peer.peer.port}"
      transport = per_call
      failures = Thread::Queue.new
      # Each inner close fails, on the owner thread or in the caller's reactor.
      trace = TracePoint.new(:call) do
        failures << true
        raise IOError, "injected close failure"
      end
      trace.enable(target: ConnectRPC::Async::Transport.instance_method(:close_owned))
      application = Class.new(StandardError)
      typed = mixed_client(transport, origin)
      with_boundaries do |boundaries|
        results = on_path(path) do
          [typed.lookup(item("kept")).message.name,
           capture_error { typed.watch(item("ss")) { |_| raise application, "from the block" } }]
        end
        expect(results.first).to eq("kept")
        expect([results.last.class, results.last.message]).to eq([application, "from the block"])
        expect(2.times.map { pop_within(failures, "an injected close failure") }).to eq([true, true])
        boundaries.take.each { |observation| expect_released(observation) }
      end
    ensure
      trace&.disable
      peer&.stop
    end
  end

  # The owner thread is stalled in native-style blocking while it dials. The
  # caller returns at its deadline plus the allowance, its close gives up on
  # the stalled owner at the zero grace plus the allowance, and the owner
  # exits by itself once released.
  it "returns at the bound while the owner is stalled and lets the owner exit once released" do
    peer = threaded_host(server: server)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    held = Thread::Queue.new
    gate = Thread::Queue.new
    trace = TracePoint.new(:call) do
      held << Thread.current
      Fiber.blocking { gate.pop }
    end
    trace.enable(target: ConnectRPC::Async::Transport.instance_method(:dial))
    transport = per_call
    caller = Thread.new do
      started = monotonic
      [capture_error { client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "stalled"), timeout: 0.3) },
       monotonic - started]
    end
    owner = pop_within(held, "the owner to stall in dial")
    error, elapsed = value_within(caller, "the stalled caller")
    expect([error.class, error.code]).to eq([ConnectRPC::Error, "deadline_exceeded"])
    expect(elapsed).to be_between(0.3, 0.3 + 0.1 + 0.1 + 0.5)
    expect(owner).to be_alive
    trace.disable
    gate << true
    expect(owner.join(5)).to equal(owner)
    expect(peer.peer.accepts.size).to be <= 1
  ensure
    trace&.disable
    gate&.<<(true)
    peer&.stop
  end
end
