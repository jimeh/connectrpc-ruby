# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../../../spec/fixtures/generation/ruby", __dir__))
require "generation/streaming_connect"
require_relative "support/threaded_stream_helpers"
require_relative "support/per_call_helpers"

RSpec.describe "PerCallTransport connection close, close budget and outcome precedence" do
  include ThreadedStreamHelpers
  include PerCallHelpers

  # The client cleanup allowance, which is also the close's reserve before the deadline.
  allowance = AsyncHelpers::INTERNAL::CLEANUP_SECONDS

  def no_error_goaway = [0, 0, Protocol::HTTP2::Error::NO_ERROR, ""]

  # A raw peer's Connect unary proto reply for Mixed.Lookup.
  def proto_reply(name)
    Protocol::HTTP::Response[200, { "content-type" => "application/proto" }, [ThreadedStreamHelpers::ITEM.encode(item(name))]]
  end

  # One call of the given kind through the public client, reduced to the
  # names it received.
  def near_deadline_call(kind, typed, timeout)
    return [typed.lookup(item("u"), timeout: timeout).message.name] if kind.end_with?("unary")

    names = []
    typed.watch(item("s"), timeout: timeout) { |reader| reader.each { |message| names << message.name } }
    names
  end

  # A recording raw peer that answers a Connect server stream with one
  # message, "s".
  # Reads the request first: a response that ends before the client wrote
  # its request resets the stream and fails that write.
  def one_message_peer
    threaded_recording_peer do |incoming|
      incoming.body&.read
      body = Protocol::HTTP::Body::Writable.new
      incoming.send_response(Protocol::HTTP::Response[200, { "content-type" => "application/connect+proto" }, body])
      body.write(envelope(item("s")))
      body.write([2, 2, "{}"].pack("CNa*"))
      body.close_write
    end
  end

  # The peer reads the client's frames in order, then its EOF, after which
  # it closes its connection, or, withholding its FIN, would have closed it;
  # a graceful close's GOAWAY would precede that.
  def expect_raw_close(peer)
    server = peer.peer
    withholding = server.is_a?(PerCallHelpers::FinWithholdingServer)
    eventually("the peer to read the client's EOF") do
      withholding ? server.close_requests.any? : server.connections.first&.closed?
    end
    expect(server.goaways).to eq([])
  end

  # A Connect server stream for the given deadline, reduced to the names it
  # received; the block runs after the messages, in the caller's fiber.
  def watch_names(typed, deadline)
    names = []
    typed.watch(item("s"), deadline: deadline) do |reader|
      reader.each { |message| names << message.name }
      yield if block_given?
    end
    names
  end

  PerCallHelpers::PATHS.each do |path|
    # The handler answers about 90 ms before the client's deadline, from a
    # peer that never sends FIN. The graceful close must leave the raw close
    # and the joins inside the deadline, or core's own recheck after the
    # transport returns turns the success into deadline_exceeded.
    %w[connect_unary connect_stream grpc_unary grpc_stream].each do |kind|
      it "returns a #{kind} result that arrives just before the deadline as success on the #{path} path" do
        timeout = 0.5
        respond_at = [nil]
        # Wall-clock positioning is the scenario: the reply lands near the real deadline.
        near = -> { sleep([respond_at.first - monotonic, 0].max) }
        peer = withholding_host(server: mixed_server(
          lookup: lambda { |message, _|
            near.call
            message
          },
          watch: lambda { |message, writer, _|
            near.call
            writer.send_message(message)
          }
        ))
        protocol = kind.start_with?("grpc") ? :grpc : :connect
        typed = mixed_client(per_call, "http://127.0.0.1:#{peer.peer.port}", protocol: protocol)
        with_boundaries do |boundaries|
          result = on_path(path) do
            # The client's deadline starts after this, so at least 90 ms of it remain at the reply.
            respond_at[0] = monotonic + timeout - 0.09
            near_deadline_call(kind, typed, timeout)
          end
          expect(result).to eq([kind.end_with?("unary") ? "u" : "s"])
          expect_released(boundaries.take.first)
        end
      ensure
        peer&.stop
      end
    end

    # About 50 ms more than the reserve, one cleanup allowance, remains of the
    # deadline when the reply leaves a peer that never sends FIN. The FIN wait
    # takes all but the reserve, so the call returns with the reserve, and no
    # more, still on its deadline.
    it "waits for FIN until one reserve before the deadline when just over one reserve remains on the #{path} path" do
      deadline = [nil]
      left_at_reply = Thread::Queue.new
      peer = withholding_peer do |incoming|
        # Wall-clock positioning is the scenario: the reply leaves about 50 ms before the reserve starts.
        sleep([deadline.first.remaining - allowance - 0.05, 0].max)
        left_at_reply << deadline.first.remaining
        incoming.send_response(proto_reply("u"))
      end
      typed = mixed_client(per_call, "http://127.0.0.1:#{peer.peer.port}")
      with_boundaries do |boundaries|
        result, left = on_path(path) do
          deadline[0] = ConnectRPC::Deadline.new(allowance + 0.4)
          [typed.lookup(item("u"), deadline: deadline.first).message.name, deadline.first.remaining]
        end
        expect(result).to eq("u")
        expect(pop_within(left_at_reply, "the reply")).to be_between(allowance, allowance + 0.1)
        # A wait that ended any earlier or later would leave more or less than the reserve.
        expect(left).to be_between(allowance - 0.04, allowance + 0.01)
        expect_released(boundaries.take.first)
        # The peer records the GOAWAY on its own reader, which can trail the call's return.
        eventually("the peer to record the GOAWAY") { peer.peer.goaways.any? }
        expect(peer.peer.goaways).to eq([no_error_goaway])
      end
    ensure
      peer&.stop
    end

    # The deadline's clock runs at a twentieth of real time. A Connect server
    # stream delivers four messages, each well inside the time its read still
    # has, but the stream outlasts the deadline's remaining time at the call's
    # start, as measured in real time, by more than one cleanup allowance. The
    # call still has most of its time on its own clock, so the close follows
    # that time: it stays graceful and joins the owner before the call returns.
    it "closes gracefully and joins the call while a slow custom deadline clock has time left on the #{path} path" do
      # Over one reserve, so the close stays graceful on the deadline's own clock.
      timeout = allowance + 0.2
      peer = threaded_recording_peer do |incoming|
        body = Protocol::HTTP::Body::Writable.new
        incoming.send_response(Protocol::HTTP::Response[200, { "content-type" => "application/connect+proto" }, body])
        4.times do |index|
          # Wall-clock positioning is the scenario: together the gaps outlast the timeout in real time.
          sleep(timeout / 2)
          body.write(envelope(item("s#{index}")))
        end
        body.write([2, 2, "{}"].pack("CNa*"))
        body.close_write
      end
      typed = mixed_client(per_call, "http://127.0.0.1:#{peer.peer.port}")
      with_boundaries do |boundaries|
        result = on_path(path) do
          base = monotonic
          deadline = ConnectRPC::Deadline.new(timeout, clock: -> { (monotonic - base) / 20 })
          names = []
          typed.watch(item("s"), deadline: deadline) { |reader| reader.each { |message| names << message.name } }
          names
        end
        expect(result).to eq(%w[s0 s1 s2 s3])
        expect_released(boundaries.take.first)
        # The peer records the GOAWAY on its own reader, which can trail the call's return.
        eventually("the peer to record the GOAWAY") { peer.peer.goaways.any? }
        expect(peer.peer.goaways).to eq([no_error_goaway])
      end
    ensure
      peer&.stop
    end

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

    [5, nil].each do |timeout|
      it "keeps a successful result when the peer withholds FIN, bounded by the cleanup allowance, " \
         "with #{timeout ? 'a' : 'no'} deadline on the #{path} path" do
        peer = withholding_peer
        origin = "http://127.0.0.1:#{peer.peer.port}"
        transport = per_call
        with_boundaries do |boundaries|
          response, elapsed = on_path(path) do
            started = monotonic
            deadline = timeout && ConnectRPC::Deadline.new(timeout)
            [transport.call(request("#{origin}/ok", deadline: deadline)), monotonic - started]
          end
          expect([response.status, response.body]).to eq([200, "ok"])
          # The drain waited its whole one-allowance budget for a FIN that never came.
          expect(elapsed).to be_between(allowance, allowance + 0.9)
          expect_released(boundaries.take.first)
          # The peer records the GOAWAY on its own reader, which can trail the call's return.
          eventually("the peer to record the GOAWAY") { peer.peer.goaways.any? }
          expect(peer.peer.goaways).to eq([no_error_goaway])
        end
      ensure
        peer&.stop
      end
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
        expect(peer.peer.accepted).to eq(1)
        expect_raw_close(peer)
      end
    ensure
      peer&.stop
    end

    # The close starts well after a default-clock deadline has passed, as for
    # a caller delayed after its exchange. The close is raw, and the owner
    # still gets its cleanup allowance from the start of the close.
    it "closes raw and joins every task when the close starts long after the deadline on the #{path} path" do
      peer = threaded_recording_peer
      origin = "http://127.0.0.1:#{peer.peer.port}"
      deadline = ConnectRPC::Deadline.new(0.2)
      # The caller's delay past the deadline is the condition under test: the
      # close starts two cleanup allowances after it.
      late = TracePoint.new(:call) { sleep(deadline.remaining + (allowance * 2)) }
      late.enable(target: PerCallHelpers::PER_CALL.instance_method(:close_plan))
      with_boundaries do |boundaries|
        response = on_path(path) { per_call.call(request("#{origin}/late", deadline: deadline)) }
        expect(response.status).to eq(200)
        expect_released(boundaries.take.first)
        expect_raw_close(peer)
      end
    ensure
      late&.disable
      peer&.stop
    end

    # The deadline's clock fails while the close budget is computed, and only
    # then. Teardown still runs, with a raw close and so no GOAWAY, and the
    # call keeps its own outcome: the stream's message, or the stream block's
    # own exception.
    %i[result block_error].each do |outcome|
      it "tears down raw and keeps the #{outcome} when the deadline clock fails computing the close budget " \
         "on the #{path} path" do
        peer = one_message_peer
        typed = mixed_client(per_call, "http://127.0.0.1:#{peer.peer.port}")
        planning = [false]
        trace = TracePoint.new(:call, :return) { |point| planning[0] = point.event == :call }
        trace.enable(target: PerCallHelpers::PER_CALL.instance_method(:close_plan))
        clock_failure = Class.new(StandardError)
        application = Class.new(StandardError)
        with_boundaries do |boundaries|
          result = on_path(path) do
            deadline = ConnectRPC::Deadline.new(5, clock: lambda {
              raise clock_failure, "clock failed" if planning.first

              monotonic
            })
            watch_names(typed, deadline) { raise application, "from the block" if outcome == :block_error }
          end
          aggregate_failures do
            if outcome == :result
              expect(result).to eq(["s"])
            else
              expect([result.class, result.message]).to eq([application, "from the block"])
            end
            expect_released(boundaries.take.first)
          end
          expect_raw_close(peer)
        end
      ensure
        trace&.disable
        peer&.stop
      end
    end

    # While the close budget is computed, the clock runs past the cleanup
    # allowance and then either leaves by throw, which no rescue sees, or
    # reports the deadline expired. A cutoff sampled before it ran would
    # already have passed, leaving no time for the close or the owner join.
    # Either way the close is raw and the full teardown runs; the throw then
    # reaches its catch, and the reported expiry leaves the stream's message.
    { throws: :thrown, expires: ["s"] }.each do |exit, outcome|
      it "tears down raw when a slow deadline clock #{exit} computing the close budget on the #{path} path" do
        peer = one_message_peer
        typed = mixed_client(per_call, "http://127.0.0.1:#{peer.peer.port}")
        planning = [false]
        trace = TracePoint.new(:call, :return) { |point| planning[0] = point.event == :call }
        trace.enable(target: PerCallHelpers::PER_CALL.instance_method(:close_plan))
        with_boundaries do |boundaries|
          result = on_path(path) do
            deadline = ConnectRPC::Deadline.new(5, clock: lambda {
              next monotonic unless planning.first

              # The clock's own running time is the condition under test.
              sleep(allowance * 2)
              throw :clock_left, :thrown if exit == :throws
              monotonic + 1000
            })
            catch(:clock_left) { watch_names(typed, deadline) }
          end
          expect(result).to eq(outcome)
          expect_released(boundaries.take.first)
          expect_raw_close(peer)
        end
      ensure
        trace&.disable
        peer&.stop
      end
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

  # The deadline's clock yields, and never returns, while the close budget is
  # computed in the caller's fiber. Meanwhile the caller's own with_timeout
  # fires. With Async's class the timeout is the caller's: it is raised once
  # the raw close and every join have finished. With the caller's own
  # StandardError class it cannot be told from the clock's own failure, the
  # documented limitation: the close is raw and the call keeps its outcome.
  { "Async::TimeoutError" => Async::TimeoutError, "a custom class" => Class.new(StandardError) }
    .each do |label, error_class|
    kept = error_class != Async::TimeoutError
    it "#{kept ? 'keeps the outcome after' : 're-raises'} a with_timeout (#{label}) that fires while a yielding " \
       "deadline clock computes the close budget, after a raw teardown, on the inline path" do
      peer = one_message_peer
      typed = mixed_client(per_call, "http://127.0.0.1:#{peer.peer.port}")
      planning = [false]
      trace = TracePoint.new(:call, :return) { |point| planning[0] = point.event == :call }
      trace.enable(target: PerCallHelpers::PER_CALL.instance_method(:close_plan))
      with_boundaries do |boundaries|
        error, names = reactor do |task|
          names = nil
          error = capture_error do
            task.with_timeout(5, error_class) do |timeout|
              clock = lambda do
                if planning.first
                  # The caller's timer fires at the reactor's next turn, while this clock waits.
                  timeout.duration = 0
                  Async::Promise.new.wait
                end
                monotonic
              end
              names = watch_names(typed, ConnectRPC::Deadline.new(5, clock: clock))
            end
          end
          [error, names]
        end
        aggregate_failures do
          if kept
            expect([error, names]).to eq([nil, ["s"]])
          else
            expect([error.class, error&.message, names]).to eq([error_class, "execution expired", nil])
          end
          expect_released(boundaries.take.first)
        end
        expect_raw_close(peer)
      end
    ensure
      trace&.disable
      peer&.stop
    end
  end

  # An exception raised into the caller while the close budget is computed:
  # with the default clock, which cannot fail, any exception is the caller's,
  # and with a custom clock so is one that is not a StandardError. It is
  # raised once the raw close and every join have finished.
  custom_clock = -> { Process.clock_gettime(Process::CLOCK_MONOTONIC) }
  { "a StandardError with the default clock" => [Class.new(StandardError), nil],
    "a non-StandardError with a custom clock" => [Class.new(Exception), custom_clock] } # rubocop:disable Lint/InheritException
    .each do |label, (interrupt, clock)|
    it "re-raises #{label} raised into the caller while the close budget is computed, after a raw teardown, " \
       "on the inline path" do
      peer = one_message_peer
      typed = mixed_client(per_call, "http://127.0.0.1:#{peer.peer.port}")
      trace = TracePoint.new(:call) { raise interrupt, "while the budget is computed" }
      with_boundaries do |boundaries|
        error, names = reactor do
          names = nil
          deadline = clock ? ConnectRPC::Deadline.new(5, clock: clock) : ConnectRPC::Deadline.new(5)
          error = capture_error do
            names = watch_names(typed, deadline) do
              trace.enable(target: PerCallHelpers::PER_CALL.instance_method(:close_plan))
            end
          end
          [error, names]
        end
        aggregate_failures do
          expect([error.class, error&.message, names]).to eq([interrupt, "while the budget is computed", nil])
          expect_released(boundaries.take.first)
        end
        expect_raw_close(peer)
      end
    ensure
      trace&.disable
      peer&.stop
    end
  end

  # The deadline's clock reads fiber-local state that the caller sets once
  # the stream's messages are in: then half an allowance remains on the
  # caller's clock, less than the reserve, which leaves no time for a graceful
  # close, while any other fiber's clock still reads the start, with almost 5
  # seconds left. The close budget comes from the caller's own fiber, so the
  # close is raw.
  it "computes the close budget from a fiber-local custom clock in the caller's fiber on the inline path" do
    peer = one_message_peer
    typed = mixed_client(per_call, "http://127.0.0.1:#{peer.peer.port}")
    with_boundaries do |boundaries|
      names = reactor do
        deadline = ConnectRPC::Deadline.new(5, clock: -> { Thread.current[:per_call_clock] || 0.0 })
        watch_names(typed, deadline) { Thread.current[:per_call_clock] = 5 - (allowance / 2) }
      ensure
        Thread.current[:per_call_clock] = nil
      end
      expect(names).to eq(["s"])
      expect_released(boundaries.take.first)
      expect_raw_close(peer)
    end
  ensure
    peer&.stop
  end

  # A Thread#raise lands while the deadline's clock computes the close budget.
  # Interrupts are deferred there, so the clock is not interrupted and the
  # interrupt is not mistaken for the clock's own failure: it is raised once
  # the close has joined the owner.
  it "re-raises a Thread#raise that lands while the deadline clock computes the close budget after teardown " \
     "on the threaded path" do
    peer = threaded_host(server: mixed_server)
    typed = mixed_client(per_call, "http://127.0.0.1:#{peer.peer.port}")
    planning = [false]
    trace = TracePoint.new(:call, :return) { |point| planning[0] = point.event == :call }
    trace.enable(target: PerCallHelpers::PER_CALL.instance_method(:close_plan))
    entered = Thread::Queue.new
    gate = Thread::Queue.new
    interrupt = Class.new(StandardError)
    clock = lambda do
      if planning.first
        entered << true
        gate.pop
      end
      monotonic
    end
    with_boundaries do |boundaries|
      victim = Thread.new do
        names = []
        error = capture_error do
          typed.watch(item("s"), deadline: ConnectRPC::Deadline.new(5, clock: clock)) do |reader|
            reader.each { |message| names << message.name }
          end
        end
        [error, names]
      end
      pop_within(entered, "the clock to compute the close budget")
      # Queued for the victim, which defers it until its close accepts interrupts.
      victim.raise(interrupt, "while the budget is computed")
      gate << true
      error, names = value_within(victim, "the interrupted caller")
      expect([error.class, error.message, names]).to eq([interrupt, "while the budget is computed", ["s"]])
      expect_released(boundaries.take.first)
    end
  ensure
    trace&.disable
    gate&.<<(true)
    peer&.stop
  end

  # Holds every owner thread on the line after its engine marked itself
  # stopped and woke the caller, until a thread joins it. Returns the held
  # owners, in order, and a release for the example's ensure.
  def hold_stopped_owners
    finalize = ConnectRPC::Async::ThreadedTransport.const_get(:Engine).instance_method(:finalize)
    source, first = finalize.source_location
    line = first + File.readlines(source).drop(first - 1).index do |text|
      text.strip == "@front.send(:owner_finished, self, outcome)"
    end
    lock = Mutex.new
    gates = Hash.new { |hash, thread| hash[thread] = Thread::Queue.new }
    gate = ->(thread) { lock.synchronize { gates[thread] } }
    held = Thread::Queue.new
    hold = TracePoint.new(:line) do
      held << Thread.current
      gate.call(Thread.current).pop
    end
    hold.enable(target: finalize, target_line: line)
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

  # Each owner fails to build its inner transport. Admitted, the call is
  # admitted by the failing owner, as when owner construction fails. Replaced,
  # each owner has stopped before the caller's admission reaches it, so the
  # front starts all three attempts. The close finds a stopped engine, so it
  # publishes its outcome without the owner having exited.
  { admitted: 1, replaced: 3 }.each do |kind, count|
    it "joins every owner thread the call started when its owner stops (#{kind})" do
      threaded = ConnectRPC::Async::ThreadedTransport
      failure = TracePoint.new(:call) do |point|
        admitted = -> { point.self.instance_variable_get(:@engine)&.calls&.any? }
        eventually("the call admitted", &admitted) if kind == :admitted
        raise IOError, "inner construction failed"
      end
      failure.enable(target: PerCallHelpers::PER_CALL.const_get(:Threaded).instance_method(:build_inner))
      admission = TracePoint.new(:call) { |point| eventually("the owner stopped") { point.self.state == :stopped } }
      admission.enable(target: threaded.const_get(:Engine).instance_method(:admit)) if kind == :replaced
      held, release = hold_stopped_owners
      with_boundaries do |boundaries|
        error = on_path(:threaded) { per_call.call(request("http://127.0.0.1:1/stopped")) }
        expect([error.class, error.code, error.message])
          .to eq([ConnectRPC::Error, "unavailable", "HTTP/2 transport owner stopped"])
        owners = count.times.map { pop_within(held, "a stopped owner") }
        expect(owners.map(&:alive?)).to eq([false] * count)
        observation = boundaries.take.first
        expect(observation.fetch(:owners)).to eq(owners)
        expect_released(observation, dials: 0)
      end
    ensure
      [failure, admission].each { |trace| trace&.disable }
      release&.call
    end
  end

  # The owner thread is stalled in native-style blocking while it dials. The
  # caller's wait returns at its deadline plus the allowance; the close then
  # gives the owner one more allowance from its start before giving up on it,
  # and the owner exits by itself once released.
  it "returns two allowances after the deadline while the owner is stalled and lets the owner exit once released" do
    peer = threaded_host(server: server)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    held = Thread::Queue.new
    gate = Thread::Queue.new
    trace = TracePoint.new(:call) do
      held << Thread.current
      Fiber.blocking { gate.pop }
    end
    trace.enable(target: ConnectRPC::Async::Transport.instance_method(:dial))
    awaited = Thread::Queue.new
    waits = TracePoint.new(:return) { awaited << monotonic }
    waits.enable(target: ConnectRPC::Async::ThreadedTransport.instance_method(:await))
    transport = per_call
    caller = Thread.new do
      started = monotonic
      [capture_error { client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "stalled"), timeout: 0.3) },
       started, monotonic]
    end
    owner = pop_within(held, "the owner to stall in dial")
    error, started, returned = value_within(caller, "the stalled caller")
    expect([error.class, error.code]).to eq([ConnectRPC::Error, "deadline_exceeded"])
    # The close after the caller's wait adds one allowance of its own.
    expect(returned - pop_within(awaited, "the caller's wait to return")).to be_between(allowance, allowance + 0.05)
    expect(returned - started).to be_between(0.3 + (2 * allowance), 0.3 + (2 * allowance) + 0.09)
    expect(owner).to be_alive
    trace.disable
    gate << true
    expect(owner.join(5)).to equal(owner)
    expect(peer.peer.accepts.size).to be <= 1
  ensure
    trace&.disable
    waits&.disable
    gate&.<<(true)
    peer&.stop
  end

  # A stream operation times out while the owner is stalled; the owner then
  # recovers, and the caller works on past the deadline and leaves the block
  # with an error, so no later wait sees the owner answer. The recovered owner
  # still gets an allowance from the start of that late close and is joined.
  it "joins an owner that recovered after an expired wait when the close starts late" do
    peer = threaded_recording_peer
    origin = "http://127.0.0.1:#{peer.peer.port}"
    engine_type = ConnectRPC::Async::ThreadedTransport.const_get(:Engine)
    entered = Thread::Queue.new
    release = Thread::Queue.new
    heartbeats = Thread::Queue.new
    waits = Thread::Queue.new
    stall = TracePoint.new(:call) do |point|
      next unless point.binding.local_variable_get(:name) == :response_head

      entered << point.self
      Fiber.blocking { release.pop(timeout: 5) or raise "owner release not signaled" }
    end
    stall.enable(target: engine_type.instance_method(:forward))
    wait_trace = TracePoint.new(:return) { |point| waits << point.return_value.first }
    wait_trace.enable(target: ConnectRPC::Async::ThreadedTransport.instance_method(:await_stream))
    with_boundaries do |boundaries|
      # The operation's wait, its zero timeout plus one allowance, expires first.
      deadline = ConnectRPC::Deadline.new(allowance + 0.5)
      error = on_path(:threaded) do
        per_call.open_stream(stream_request(origin, "/recover", deadline: deadline)) do |exchange|
          exchange.response_head(timeout: 0)
        rescue ConnectRPC::Error => e
          raise unless e.code == "unavailable"

          engine = pop_within(entered, "the owner to start response_head")
          release << true
          engine.run_on_owner(-> { heartbeats << Thread.current })
          pop_within(heartbeats, "the recovered owner to process a command")
          # The caller's work past the deadline and its allowance is the
          # condition under test.
          sleep(deadline.remaining + (allowance * 1.2))
          raise "after the deadline"
        end
      end
      # Core reports the deadline, which passed before the block failed.
      expect([error.class, error.code]).to eq([ConnectRPC::Error, "deadline_exceeded"])
      expect(Array.new(waits.size) { waits.pop }).to eq(%i[reply timeout])
      expect_released(boundaries.take.first)
    end
  ensure
    stall&.disable
    wait_trace&.disable
    release&.push(true)
    peer&.stop
  end

  # The caller interrupts its own stream wait well before the deadline,
  # rescues it, works past the deadline and leaves the block with an error,
  # so no later wait reaches the owner. The early interrupt says nothing
  # about the healthy owner, which is joined after the late close.
  it "joins a healthy owner after an early interrupted wait and a late close" do
    unanswered = Thread::Queue.new
    peer = threaded_recording_peer { |_incoming| unanswered.pop(timeout: 5) }
    origin = "http://127.0.0.1:#{peer.peer.port}"
    failure = Class.new(StandardError)
    interrupted = false
    with_boundaries do |boundaries|
      deadline = ConnectRPC::Deadline.new(0.4)
      error = on_path(:threaded) do
        per_call.open_stream(stream_request(origin, "/early", deadline: deadline)) do |exchange|
          begin
            Timeout.timeout(0.05) { exchange.response_head }
          rescue Timeout::Error
            interrupted = deadline.remaining.positive?
          end
          # The caller's work past the deadline and its allowance is the
          # condition under test.
          sleep(deadline.remaining + (allowance * 1.2))
          raise failure, "after the deadline"
        end
      end
      # The interrupt reached the block before the deadline; core reports the
      # deadline, which passed before the block failed.
      expect([interrupted, error.class, error.code]).to eq([true, ConnectRPC::Error, "deadline_exceeded"])
      expect_released(boundaries.take.first)
    end
  ensure
    unanswered&.push(true)
    peer&.stop
  end
end
