# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../../../spec/fixtures/generation/ruby", __dir__))
require "generation/streaming_connect"
require_relative "support/threaded_stream_helpers"
require_relative "support/per_call_helpers"

# A stream block that rescues its deadline error and returns runs the stream's
# terminal wait. When an operation's wait already expired at the deadline
# plus the allowance, that terminal wait shares its bound instead of adding
# another allowance for a stalled owner (#198).
RSpec.describe "ThreadedTransport and PerCallTransport stream terminal wait after a deadline" do
  include ThreadedStreamHelpers
  include PerCallHelpers

  # The client cleanup allowance, which stalled-owner bounds extend by.
  allowance = AsyncHelpers::INTERNAL::CLEANUP_SECONDS
  # The stream deadline, in seconds.
  def seconds = 0.3

  let(:unanswered) { Thread::Queue.new }
  let(:peer) { threaded_recording_peer { |_incoming| unanswered.pop(timeout: 10) } }
  let(:origin) { "http://127.0.0.1:#{peer.peer.port}" }
  let(:entered) { Thread::Queue.new }
  let(:release) { Thread::Queue.new }
  let(:waits) { Thread::Queue.new }

  # Holds the owner thread in native-style blocking when it starts the
  # stream's response_head, until the example releases it.
  def stall_response_head
    engine_type = ConnectRPC::Async::ThreadedTransport.const_get(:Engine)
    entered = self.entered
    release = self.release
    trace = TracePoint.new(:call) do |point|
      next unless point.binding.local_variable_get(:name) == :response_head

      entered << Thread.current
      Fiber.blocking { release.pop(timeout: 10) or raise "owner release not signaled" }
    end
    trace.enable(target: engine_type.instance_method(:forward))
    trace
  end

  # Records each stream wait's status as it returns, on the caller's thread.
  def record_waits
    waits = self.waits
    TracePoint.new(:return) { |point| waits << point.return_value.first }
              .tap { |trace| trace.enable(target: ConnectRPC::Async::ThreadedTransport.instance_method(:await_stream)) }
  end

  def statuses = Array.new(waits.size) { waits.pop }

  # Opens a stream whose block rescues the deadline error of its first
  # response_head, runs `after_rescue`, and returns at once. Returns the
  # escaping error with the start, rescue and return times.
  def rescuing_stream(transport, after_rescue: nil)
    rescued = nil
    started = monotonic
    error = capture_error_or_value do
      transport.open_stream(stream_request(origin, "/stall", deadline: ConnectRPC::Deadline.new(seconds))) do |exchange|
        exchange.response_head
      rescue ConnectRPC::Error => e
        raise unless e.code == "deadline_exceeded"

        rescued = monotonic
        after_rescue&.call
        :rescued
      end
    end
    [error, started, rescued, monotonic]
  end

  after do
    release << true
    unanswered << true
    peer.stop
  end

  describe "ThreadedTransport" do
    it "returns a rescued deadline error without another allowance while the owner stays stalled" do
      stall = stall_response_head
      wait_trace = record_waits
      transport = threaded(origin: origin)
      error, started, rescued, returned = rescuing_stream(transport)
      expect([error.class, error.code]).to eq([ConnectRPC::Error, "deadline_exceeded"])
      owner = pop_within(entered, "the owner stalled in response_head")
      # The operation's own wait spends the one allowance past the deadline.
      expect(rescued - started).to be_between(seconds + allowance, seconds + allowance + 0.5)
      # The terminal wait shares the operation's expired bound.
      expect(returned - rescued).to be < allowance / 2
      expect(statuses).to eq(%i[reply deadline deadline])
      expect([owner.alive?, admitted(transport)]).to eq([true, 1])
      release << true
      eventually("admission released after the owner resumes") { admitted(transport).zero? }
    ensure
      stall&.disable
      wait_trace&.disable
      release << true
      transport&.close(grace: 0)
    end

    # The owner answers the operation with its own deadline error, so no
    # wait expired, and the terminal wait still takes the owner's outcome
    # before the call returns.
    it "takes a responsive owner's outcome after it answered with a deadline error" do
      wait_trace = record_waits
      transport = threaded(origin: origin)
      error, started, rescued, = rescuing_stream(transport)
      expect([error.class, error.code]).to eq([ConnectRPC::Error, "deadline_exceeded"])
      expect(rescued - started).to be < seconds + (allowance / 2)
      expect(statuses).to eq(%i[reply reply result])
      expect(admitted(transport)).to eq(0)
    ensure
      wait_trace&.disable
      transport&.close(grace: 0)
    end
  end

  describe "PerCallTransport" do
    # The terminal wait and the close share the allowances: the operation
    # spends one past the deadline and the close gives the owner one more.
    it "returns within two allowances of the deadline while the owner stays stalled" do
      stall = stall_response_head
      wait_trace = record_waits
      error, started, rescued, returned = on_path(:threaded) { rescuing_stream(per_call) }
      expect([error.class, error.code]).to eq([ConnectRPC::Error, "deadline_exceeded"])
      owner = pop_within(entered, "the owner stalled in response_head")
      expect(rescued - started).to be_between(seconds + allowance, seconds + allowance + 0.5)
      # The close after the terminal wait waits its own full allowance.
      expect(returned - rescued).to be_between(allowance, allowance * 1.5)
      expect(returned - started).to be < seconds + (2 * allowance) + 0.5
      expect(statuses).to eq(%i[reply deadline deadline])
      expect(owner).to be_alive
      stall.disable
      release << true
      expect(owner.join(5)).to equal(owner)
    ensure
      stall&.disable
      wait_trace&.disable
    end

    # The owner recovers once the operation's wait expired, and the block
    # returns at once, so the terminal wait shares the expired bound. The
    # close still gives the recovered owner its allowance and joins it.
    it "joins an owner that recovers after the expired wait" do
      stall = stall_response_head
      with_boundaries do |boundaries|
        resume = lambda do
          pop_within(entered, "the owner stalled in response_head")
          release << true
        end
        error, = on_path(:threaded) { rescuing_stream(per_call, after_rescue: resume) }
        expect([error.class, error.code]).to eq([ConnectRPC::Error, "deadline_exceeded"])
        expect_released(boundaries.take.first)
      end
    ensure
      stall&.disable
    end
  end
end
