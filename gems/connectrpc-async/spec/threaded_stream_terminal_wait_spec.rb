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

  # Runs a stream whose deadline clock runs at a twentieth of real speed:
  # core's deadline stays live while the transport's waits, which project it
  # to monotonic time when they start, expire. The block rescues its
  # operation's error and returns, and the owner, stalled in response_head,
  # recovers `release_delay` after the block exits. `cancel_after` cancels
  # the call's source that long after the owner stalled; `faults` arm the
  # clock as slow_clock describes. Returns the operation's error, the call's
  # error and the deadline.
  def slow_clock_stream(transport, operation_timeout:, cancel_after: nil, release_delay: 0.5, **faults)
    clock, traces = slow_clock(**faults)
    exiting = Thread::Queue.new
    exit_trace = TracePoint.new(:call) { exiting << true }
    exit_trace.enable(target: ConnectRPC::Async::ThreadedTransport.instance_method(:exit_stream))
    traces << exit_trace
    deadline = ConnectRPC::Deadline.new(0.5, clock: clock)
    source = ConnectRPC::CancellationSource.new if cancel_after
    helpers = slow_clock_helpers(exiting, release_delay, source, cancel_after)
    operation_error = nil
    request = stream_request(origin, "/stall", deadline: deadline, cancellation: source&.token)
    error = capture_error_or_value do
      transport.open_stream(request) do |exchange|
        exchange.response_head(timeout: operation_timeout)
      rescue ConnectRPC::Error => e
        operation_error = e
        :rescued
      end
    end
    helpers.each { |thread| value_within(thread, "a helper thread") }
    [operation_error, error, deadline]
  ensure
    traces&.each(&:disable)
  end

  # A clock at a twentieth of real speed, with faults armed when an
  # await_stream returns the given status. fail_on: the clock's next read
  # raises its own deadline_exceeded Error. step_on: the clock jumps past the
  # deadline once the arbiter's next reason read returns. Returns the clock
  # and the traces to disable.
  def slow_clock(fail_on: nil, step_on: nil)
    armed = stepping = jumped = false
    arm = TracePoint.new(:return) do |point|
      status = point.return_value.first
      armed = true if status == fail_on
      stepping = true if status == step_on
    end
    arm.enable(target: ConnectRPC::Async::ThreadedTransport.instance_method(:await_stream))
    step = TracePoint.new(:return) { jumped = true if stepping }
    step.enable(target: ConnectRPC::CallCancellation.instance_method(:reason))
    started = monotonic
    clock = lambda do
      if armed
        armed = false
        raise ConnectRPC::Error.new(:deadline_exceeded, "clock failure")
      end
      ((monotonic - started) / 20) + (jumped ? 1 : 0)
    end
    [clock, [arm, step]]
  end

  # The owner's release after the block exits and the optional cancellation.
  def slow_clock_helpers(exiting, release_delay, source, cancel_after)
    helpers = [Thread.new do
      pop_within(exiting, "the block exit")
      # Releasing the owner after a wrongly shared bound would have ended
      # the exit wait is the condition under test.
      sleep(release_delay)
      release << true
    end]
    return helpers unless source

    helpers << Thread.new do
      pop_within(entered, "the owner stalled in response_head")
      # Cancelling after the operation's deadline bound minus one
      # allowance, so the cancellation outlasts that bound, is the
      # condition under test.
      sleep(cancel_after)
      source.cancel
    end
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
      threaded_type = ConnectRPC::Async::ThreadedTransport
      moments = Thread::Queue.new
      exit_trace = TracePoint.new(:call) { moments << [:exit, monotonic] }
      exit_trace.enable(target: threaded_type.instance_method(:exit_stream))
      expiry_trace = TracePoint.new(:call) { moments << [:read_expired, monotonic] }
      expiry_trace.enable(target: threaded_type.instance_method(:expire_operation))
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
      # The exit wait was already in progress when the read's wait expired.
      expect(Array.new(moments.size) { moments.pop }.sort_by(&:last).map(&:first)).to eq(%i[exit read_expired])
      expect(admitted(transport)).to eq(1)
      release << true
      eventually("admission released after the owner resumes") { admitted(transport).zero? }
    ensure
      stall&.disable
      exit_trace&.disable
      expiry_trace&.disable
      release << true
      transport&.close(grace: 0)
    end

    # Core's arbiter decides each unanswered operation's error once, and
    # only a deadline_exceeded decision shares the deadline bound. Each case
    # recovers the owner after the bound a wrong share would publish but
    # before the exit wait's own bound, so the exit takes the owner's
    # outcome only if nothing was shared.
    {
      "an operation timeout that core reports as unavailable" =>
        [{ operation_timeout: 0.2 }, %w[unavailable timeout], [:live, nil]],
      "an operation timeout whose arbitration the clock fails with its own deadline_exceeded" =>
        [{ operation_timeout: 0.2, fail_on: :timeout }, %w[clock timeout], [:live, nil]],
      "a deadline expiry whose arbitration the clock fails with its own deadline_exceeded" =>
        [{ operation_timeout: nil, fail_on: :deadline }, %w[clock deadline], [:live, nil]],
      "a deadline expiry after core accepted a cancellation" =>
        [{ operation_timeout: nil, cancel_after: 1.0, release_delay: 0.2 }, %w[canceled deadline], [:live, "canceled"]],
      "an operation timeout whose deadline core accepts only after its single arbitration" =>
        [{ operation_timeout: 0.2, step_on: :timeout }, %w[unavailable timeout], [:expired, "deadline_exceeded"]]
    }.each do |label, (options, (operation, expiry), (deadline_state, code))|
      it "shares no deadline bound for #{label}" do
        stall = stall_response_head
        wait_trace = record_waits
        transport = threaded(origin: origin)
        operation_error, error, deadline = slow_clock_stream(transport, **options)
        if operation == "clock"
          expect([operation_error.class, operation_error.code, operation_error.message])
            .to eq([ConnectRPC::Error, "deadline_exceeded", "clock failure"])
        else
          expect(operation_error&.code).to eq(operation)
        end
        expect(statuses).to eq([:reply, expiry.to_sym, :result])
        expect(error).to be_a(ConnectRPC::Error)
        expect(deadline.expired?).to be(deadline_state == :expired)
        code ? expect(error.code).to(eq(code)) : expect(error.code).not_to(eq("deadline_exceeded"))
      ensure
        stall&.disable
        wait_trace&.disable
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
