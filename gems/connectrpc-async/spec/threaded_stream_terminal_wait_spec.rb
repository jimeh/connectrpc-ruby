# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../../../spec/fixtures/generation/ruby", __dir__))
require "generation/streaming_connect"
require_relative "support/threaded_stream_helpers"
require_relative "support/per_call_helpers"

# A stream block that rescues its deadline error and returns runs the stream's
# terminal wait. Once an operation's wait ended past the deadline without the
# owner's answer, the terminal wait shares that operation's deadline bound
# instead of adding another allowance for a stalled owner (#198).
RSpec.describe "ThreadedTransport and PerCallTransport stream terminal wait after a deadline" do
  include ThreadedStreamHelpers
  include PerCallHelpers

  # The client cleanup allowance, which stalled-owner bounds extend by.
  allowance = AsyncHelpers::INTERNAL::CLEANUP_SECONDS
  # The stream deadline, in seconds.
  def seconds = 0.5

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

  # Holds the owner thread as it returns from its run, after it published
  # its outcome, until another thread joins it. Returns the held owners and
  # a release for the example's ensure.
  def hold_exiting_owners
    lock = Mutex.new
    gates = Hash.new { |hash, thread| hash[thread] = Thread::Queue.new }
    gate = ->(thread) { lock.synchronize { gates[thread] } }
    held = Thread::Queue.new
    hold = TracePoint.new(:return) do
      held << Thread.current
      gate.call(Thread.current).pop
    end
    hold.enable(target: ConnectRPC::Async::ThreadedTransport.const_get(:Engine).instance_method(:own))
    joins = TracePoint.new(:c_call) do |point|
      gate.call(point.self) << true if point.method_id == :join && point.self.is_a?(Thread)
    end
    joins.enable
    release = lambda do
      [hold, joins].each(&:disable)
      lock.synchronize { gates.each_value { |queue| queue << true } }
    end
    [held, release]
  end

  # Opens a stream whose block rescues the deadline error of its operation,
  # by default a response_head, runs `after_rescue`, and returns at once.
  # Returns the escaping error with the start, rescue and return times.
  def rescuing_stream(transport, deadline: seconds, after_rescue: nil, &operation)
    operation ||= lambda(&:response_head)
    rescued = nil
    started = monotonic
    error = capture_error_or_value do
      transport.open_stream(stream_request(origin, "/stall",
                                           deadline: ConnectRPC::Deadline.new(deadline))) do |exchange|
        operation.call(exchange)
      rescue ConnectRPC::Error => e
        raise unless e.code == "deadline_exceeded"

        rescued = monotonic
        after_rescue&.call
        :rescued
      end
    end
    raise "the block never rescued a deadline error: #{error.inspect}" unless rescued

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

    # The block returns past the deadline while a read on another thread
    # still waits for the stalled owner. The terminal wait starts later than
    # that read, and still ends when the read's wait expires.
    it "ends a terminal wait already in progress when a concurrent operation's wait expires" do
      stall = stall_response_head
      transport = threaded(origin: origin)
      reader = nil
      deadline = nil
      started = monotonic
      error = capture_error_or_value do
        deadline = ConnectRPC::Deadline.new(seconds)
        transport.open_stream(stream_request(origin, "/stall", deadline: deadline)) do |exchange|
          reader = Thread.new { capture_error { exchange.response_head } }
          pop_within(entered, "the owner stalled in response_head")
          eventually("the deadline to pass") { !deadline.remaining.positive? }
          # Returning well inside the read's allowance, rather than at the
          # deadline, is the condition under test.
          sleep(allowance / 2)
        end
      end
      returned = monotonic
      read_error = value_within(reader, "the concurrent read")
      expect([error.class, error.code, read_error.class, read_error.code])
        .to eq([ConnectRPC::Error, "deadline_exceeded", ConnectRPC::Error, "deadline_exceeded"])
      expect(returned - started).to be_between(seconds + allowance, seconds + allowance + 0.3)
      expect(admitted(transport)).to eq(1)
      release << true
      eventually("admission released after the owner resumes") { admitted(transport).zero? }
    ensure
      stall&.disable
      release << true
      transport&.close(grace: 0)
    end

    # The deadline's clock runs at a twentieth of real speed, so the
    # operation's timeout expires after the deadline as projected to
    # monotonic time while core's deadline still has time left, and core's
    # arbiter does not accept the deadline. Core reports unavailable, or the
    # clock itself raises a deadline_exceeded Error at that arbitration.
    # Either way the stream shares no deadline bound, and the exit still
    # waits for an owner that recovers after that projected bound.
    {
      "core reports as unavailable" => [false, "unavailable"],
      "the deadline's clock fails with its own deadline_exceeded" => [true, "deadline_exceeded"]
    }.each do |label, (clock_fails, code)|
      it "shares no deadline bound for an operation timeout that #{label}" do
        stall = stall_response_head
        wait_trace = record_waits
        transport = threaded(origin: origin)
        exiting = Thread::Queue.new
        exit_trace = TracePoint.new(:call) { exiting << true }
        exit_trace.enable(target: ConnectRPC::Async::ThreadedTransport.instance_method(:exit_stream))
        # Arms the clock to fail once, at the arbitration after the
        # operation's timeout expired.
        armed = false
        arm_trace = TracePoint.new(:return) { |point| armed = clock_fails if point.return_value.first == :timeout }
        arm_trace.enable(target: ConnectRPC::Async::ThreadedTransport.instance_method(:await_stream))
        releaser = Thread.new do
          pop_within(exiting, "the block exit")
          # Releasing the owner after the projected deadline bound, 0.3 s
          # after the operation's expiry, has passed is the condition under
          # test.
          sleep(0.5)
          release << true
        end
        clock_start = monotonic
        clock = lambda do
          if armed
            armed = false
            raise ConnectRPC::Error.new(:deadline_exceeded, "clock failure")
          end
          (monotonic - clock_start) / 20
        end
        deadline = ConnectRPC::Deadline.new(0.5, clock: clock)
        operation_error = nil
        error = capture_error_or_value do
          transport.open_stream(stream_request(origin, "/stall", deadline: deadline)) do |exchange|
            exchange.response_head(timeout: 0.2)
          rescue ConnectRPC::Error => e
            operation_error = e
            :rescued
          end
        end
        expect(operation_error&.code).to eq(code)
        expect(statuses).to eq(%i[reply timeout result])
        expect([error.class, deadline.expired?]).to eq([ConnectRPC::Error, false])
        expect(error.code).not_to eq("deadline_exceeded")
        value_within(releaser, "the owner release")
      ensure
        stall&.disable
        wait_trace&.disable
        exit_trace&.disable
        arm_trace&.disable
        release << true
        transport&.close(grace: 0)
      end
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

    # The operation's own timeout plus the allowance expires after the
    # deadline but before the deadline's own bound, so core reports
    # deadline_exceeded. The terminal wait shares the deadline bound, and the
    # close adds the second allowance.
    it "returns within two allowances of the deadline when an operation timeout expires past it" do
      stall = stall_response_head
      wait_trace = record_waits
      deadline = 0.8
      error, started, _rescued, returned = on_path(:threaded) do
        rescuing_stream(per_call, deadline: deadline) { |exchange| exchange.response_head(timeout: 0.5) }
      end
      expect([error.class, error.code]).to eq([ConnectRPC::Error, "deadline_exceeded"])
      owner = pop_within(entered, "the owner stalled in response_head")
      expect(statuses).to eq(%i[reply timeout deadline])
      expect(returned - started).to be_between(deadline + (2 * allowance), deadline + (2 * allowance) + 0.4)
      stall.disable
      release << true
      expect(owner.join(5)).to equal(owner)
    ensure
      stall&.disable
      wait_trace&.disable
    end

    # The owner recovers once the operation's wait expired, and the block
    # returns at once, so the terminal wait shares the expired bound. The
    # recovered owner is held after it publishes its outcome until a thread
    # joins it, so the call's boundary finds it gone only if the close
    # joined it.
    it "joins an owner that recovers after the expired wait" do
      stall = stall_response_head
      held, release_owners = hold_exiting_owners
      with_boundaries do |boundaries|
        resume = lambda do
          pop_within(entered, "the owner stalled in response_head")
          release << true
        end
        error, = on_path(:threaded) { rescuing_stream(per_call, after_rescue: resume) }
        expect([error.class, error.code]).to eq([ConnectRPC::Error, "deadline_exceeded"])
        expect(pop_within(held, "the owner held as it exits")).to be_a(Thread)
        expect_released(boundaries.take.first)
      end
    ensure
      stall&.disable
      release_owners&.call
    end
  end
end
