# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../../../spec/fixtures/generation/ruby", __dir__))
require "generation/streaming_connect"
require_relative "support/threaded_stream_helpers"

RSpec.describe "ThreadedTransport stream completion outcome" do
  include ThreadedStreamHelpers

  def recording_bus(outcomes, completions)
    observer = Object.new
    observer.define_singleton_method(:processing_finished) { |value| outcomes << value }
    observer.define_singleton_method(:finished) { |value| completions << value }
    subscriber = Object.new
    subscriber.define_singleton_method(:start) { |_| observer }
    ConnectRPC::Instrumentation.new(subscribers: [subscriber])
  end

  # Makes every later owned connection release fail after it succeeds, on the
  # owner, and reports each injected failure to +fired+. With +hold+, the
  # owner's cleanup waits there before raising.
  def fail_release(transport, fired = nil, hold = nil)
    on_owner(transport) do
      manager = engine_of(transport).inner.instance_variable_get(:@manager)
      release = manager.method(:release)
      manager.define_singleton_method(:release) do |connection|
        release.call(connection)
        fired&.<<(true)
        hold&.pop
        raise IOError, "private owned cleanup failure"
      end
    end
  end

  def exited_mailbox(transport) = stream_mailboxes(transport).find(&:exited)
  def busy_streams(transport) = stream_mailboxes(transport).count(&:busy)

  # A raw lifecycle handle that records the engine's late failure reporting.
  def reporting_lifecycle(log)
    lifecycle = Object.new
    lifecycle.define_singleton_method(:activate) { |&block| block.call }
    lifecycle.define_singleton_method(:record_failure) { |error| log << [:record_failure, error.message] }
    lifecycle.define_singleton_method(:finish) { |delivery:, error: nil| log << [:finish, delivery, error&.message] }
    lifecycle
  end

  it "holds a normal block exit until the engine returns the stream's outcome" do
    peer = threaded_host(server: mixed_server)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    mixed_client(transport, origin).lookup(item("warm"))
    gates = Thread::Queue.new
    caller = Thread.new do
      transport.open_stream(stream_request(origin, watch_method)) do |exchange|
        exchange.write(envelope(item("held")))
        exchange.close_send
        exchange.read(1024) while exchange.read(1024)
        exchange.finish
        gates << hold_owner(transport)
        :value
      end
    end
    gate = pop_within(gates, "the owner held inside the block")
    eventually("the caller waiting for the terminal outcome") { exited_mailbox(transport) && blocked?(caller) }
    expect([caller.alive?, admitted(transport)]).to eq([true, 1])
    gate << true
    expect(value_within(caller, "the released caller")).to eq(:value)
    expect(admitted(transport)).to eq(0)
  ensure
    gate&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  it "fails with deadline_exceeded, never success, when the owner stalls after a normal block exit" do
    peer = threaded_host(server: mixed_server)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    mixed_client(transport, origin).lookup(item("warm"))
    gates = Thread::Queue.new
    deadline = ConnectRPC::Deadline.new(1)
    caller = Thread.new do
      capture_error do
        transport.open_stream(stream_request(origin, watch_method, deadline: deadline)) do |exchange|
          exchange.write(envelope(item("stalled")))
          exchange.close_send
          exchange.read(1024) while exchange.read(1024)
          gates << hold_owner(transport)
          :value
        end
      end
    end
    gate = pop_within(gates, "the owner held inside the block")
    error = value_within(caller, "the stalled exit")
    expect([error.class, error.code]).to eq([ConnectRPC::Error, "deadline_exceeded"])
    expect(deadline.remaining).to be > -0.5
    expect(admitted(transport)).to eq(1)
    gate << true
    eventually("admission released after the owner resumes") { admitted(transport).zero? }
  ensure
    gate&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  %i[lookup watch].each do |name|
    it "reports a late gRPC cleanup failure after a #{name} block returns to the caller" do
      peer = threaded_host(server: mixed_server)
      origin = "http://127.0.0.1:#{peer.peer.port}"
      transport = threaded(origin: origin)
      outcomes = []
      completions = []
      typed = mixed_client(transport, origin, protocol: :grpc, instrumentation: recording_bus(outcomes, completions))
      expect(typed.lookup(item("warm")).message.name).to eq("warm")
      fail_release(transport)
      error = capture_error do
        name == :lookup ? typed.lookup(item("late")) : typed.watch(item("late")) { |reader| reader.each { nil } }
      end
      expect(error).to be_a(ConnectRPC::Error)
      expect([error.code, error.message]).to eq(["unavailable", "HTTP/2 transport cleanup failed"])
      expect(error.cause.message).to eq("private owned cleanup failure")
      expect(outcomes.last.normalized_error).to equal(error)
      expect(completions.last.delivery).to eq(:aborted)
      expect(completions.last.error.message).to eq("private owned cleanup failure")
      expect(admitted(transport)).to eq(0)
    ensure
      transport&.close(grace: 0)
      peer&.stop
    end
  end

  # The remote error escapes the block after the stream finished, so the caller
  # waits for the engine and its delivery abort is reported as on Transport.
  it "keeps a delivered remote gRPC error and reports the owner's later cleanup failure as a delivery abort" do
    peer = threaded_host(server: mixed_server(watch: lambda { |message, writer, _|
      writer.send_message(message)
      raise ConnectRPC::Error.new(:not_found, "remote", trailers: { "x-end" => %w[c d] })
    }))
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    outcomes = []
    completions = []
    typed = mixed_client(transport, origin, protocol: :grpc, instrumentation: recording_bus(outcomes, completions))
    typed.lookup(item("warm"))
    fired = Thread::Queue.new
    fail_release(transport, fired)
    names = []
    error = capture_error { typed.watch(item("remote")) { |reader| reader.each { |reply| names << reply.name } } }
    expect([error.class, error.code, error.message, error.cause]).to eq([ConnectRPC::Error, "not_found", "remote", nil])
    expect([names, error.trailers["x-end"]]).to eq([%w[remote], %w[c d]])
    expect(pop_within(fired, "the injected cleanup failure", timeout: 0)).to be(true)
    expect(outcomes.last.code).to eq("not_found")
    expect([completions.last.delivery, completions.last.error.class, completions.last.error&.message])
      .to eq([:aborted, IOError, "private owned cleanup failure"])
    expect(admitted(transport)).to eq(0)
  ensure
    transport&.close(grace: 0)
    peer&.stop
  end

  # After the block's normal exit opens the gate for terminal cleanup, an
  # operation finishing on another thread must not close it again.
  it "reports a late gRPC cleanup failure after another thread's operation completes past the block's exit" do
    entered = Thread::Queue.new
    release = Thread::Queue.new
    peer = threaded_host(server: mixed_server(watch: lambda { |message, writer, _|
      writer.send_message(item("first"))
      entered << message.name
      release.pop
      writer.send_message(item("second"))
    }))
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    mixed_client(transport, origin).lookup(item("warm"))
    fired = Thread::Queue.new
    hold = Thread::Queue.new
    fail_release(transport, fired, hold)
    log = Thread::Queue.new
    headers = { "content-type" => ["application/grpc+proto"], "te" => ["trailers"] }
    request = stream_request(origin, watch_method, headers: headers, lifecycle: reporting_lifecycle(log))
    exchanges = Thread::Queue.new
    proceed = Thread::Queue.new
    scope = Thread.new do
      capture_error do
        transport.open_grpc_stream(request) do |exchange|
          exchange.write(envelope(item("shared")))
          exchange.close_send
          exchanges << [exchange, exchange.read(1024)]
          proceed.pop
          :value
        end
      end
    end
    exchange, first = pop_within(exchanges, "the shared exchange after the first message")
    expect(first).to eq(envelope(item("first")))
    pop_within(entered, "the stream in the handler")
    operation = Thread.new do
      exchange.read(1024)
    rescue StandardError => e
      e
    end
    eventually("the other thread's read waiting on the owner") do
      busy_streams(transport) == 1 && blocked?(operation)
    end
    proceed << true
    eventually("the scope waiting for the terminal outcome") { exited_mailbox(transport) && blocked?(scope) }
    release << true
    expect(value_within(operation, "the other thread's read")).to start_with(envelope(item("second")))
    pop_within(fired, "the owner's cleanup in the failing release")
    hold << true
    error = value_within(scope, "the block's terminal outcome")
    expect([error.class, error.code, error.message])
      .to eq([ConnectRPC::Error, "unavailable", "HTTP/2 transport cleanup failed"])
    expect(Array.new(log.size) { log.pop })
      .to eq([[:record_failure, "private owned cleanup failure"], [:finish, :aborted, "private owned cleanup failure"]])
    expect(admitted(transport)).to eq(0)
  ensure
    hold&.<<(true)
    proceed&.<<(true)
    release&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  it "reports a late gRPC cleanup failure after another thread's operation is refused past the block's exit" do
    peer = threaded_host(server: mixed_server)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    mixed_client(transport, origin).lookup(item("warm"))
    fired = Thread::Queue.new
    hold = Thread::Queue.new
    fail_release(transport, fired, hold)
    log = Thread::Queue.new
    headers = { "content-type" => ["application/grpc+proto"], "te" => ["trailers"] }
    request = stream_request(origin, watch_method, headers: headers, lifecycle: reporting_lifecycle(log))
    exchanges = Thread::Queue.new
    scope = Thread.new do
      capture_error do
        transport.open_grpc_stream(request) do |exchange|
          exchange.write(envelope(item("refused")))
          exchange.close_send
          loop do
            chunk = exchange.read(1024)
            break if chunk.nil? || chunk.empty?
          end
          exchanges << exchange
          :value
        end
      end
    end
    exchange = pop_within(exchanges, "the exchange after EOF")
    pop_within(fired, "the owner's cleanup in the failing release")
    refused = capture_error { exchange.write(envelope(item("late"))) }
    expect([refused.class, refused.code, refused.message])
      .to eq([ConnectRPC::Error, "unavailable", "HTTP/2 exchange is retired"])
    hold << true
    error = value_within(scope, "the block's terminal outcome")
    expect([error.class, error.code, error.message])
      .to eq([ConnectRPC::Error, "unavailable", "HTTP/2 transport cleanup failed"])
    expect(Array.new(log.size) { log.pop })
      .to eq([[:record_failure, "private owned cleanup failure"], [:finish, :aborted, "private owned cleanup failure"]])
    expect(admitted(transport)).to eq(0)
  ensure
    hold&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  # Core raises remote errors from inside the transport block after the
  # exchange finished. Each must release admission before it returns.
  {
    "gRPC unary" => %i[grpc lookup], "gRPC server-stream" => %i[grpc watch],
    "Connect server-stream" => %i[connect watch]
  }.each do |label, (protocol, name)|
    it "releases admission before each back-to-back #{label} remote error returns under max_requests: 1" do
      remote = ->(*) { raise ConnectRPC::Error.new(:not_found, "remote") }
      peer = threaded_host(server: mixed_server(lookup: remote, watch: remote))
      origin = "http://127.0.0.1:#{peer.peer.port}"
      transport = threaded(origin: origin, max_requests: 1)
      typed = mixed_client(transport, origin, protocol: protocol)
      calls = Thread.new do
        Array.new(20) do
          error = capture_error do
            name == :lookup ? typed.lookup(item("x")) : typed.watch(item("x")) { |reader| reader.each { nil } }
          end
          [error.class, error.code, error.message, admitted(transport)]
        end
      end
      expect(value_within(calls, "the back-to-back calls")).to eq([[ConnectRPC::Error, "not_found", "remote", 0]] * 20)
    ensure
      transport&.close(grace: 0)
      peer&.stop
    end
  end

  # Only a ConnectRPC::Error after a successful finish, as core raises remote
  # errors, waits, and only for the cleanup allowance. Every other exit,
  # including a Timeout or other StandardError after finish, does not wait.
  {
    finished_error: true, finished_standard_error: false, finished_timeout: false, finished_exception: false,
    finished_throw: false, unfinished_error: false
  }.each do |kind, waits|
    it "#{waits ? 'waits only the allowance' : 'does not wait'} for a stalled owner on a #{kind} exit" do
      peer = threaded_host(server: mixed_server)
      origin = "http://127.0.0.1:#{peer.peer.port}"
      transport = threaded(origin: origin)
      mixed_client(transport, origin).lookup(item("warm"))
      # unfinished_error raises a remote-style error before finish, which must not wait either.
      failure = { finished_error: ConnectRPC::Error, unfinished_error: ConnectRPC::Error,
                  finished_timeout: Timeout::Error }.fetch(kind) do
        Class.new(kind == :finished_exception ? Exception : StandardError)
      end
      gates = Thread::Queue.new
      caller = Thread.new do
        started = nil
        outcome = capture_error do
          catch(:out) do
            transport.open_stream(stream_request(origin, watch_method)) do |exchange|
              exchange.write(envelope(item("stalled")))
              exchange.close_send
              exchange.read(1024) while exchange.read(1024)
              exchange.finish unless kind == :unfinished_error
              gates << hold_owner(transport)
              started = monotonic
              throw :out, :thrown if kind == :finished_throw
              raise failure == ConnectRPC::Error ? failure.new(:not_found, "application") : failure.new("application")
            end
          end
        end
        [outcome, monotonic - started]
      end
      gate = pop_within(gates, "the owner held inside the block")
      outcome, waited = value_within(caller, "the block's exit")
      expect([outcome.class, outcome.message]).to eq([failure, "application"]) unless kind == :finished_throw
      expect(outcome).to be_nil if kind == :finished_throw
      if waits
        expect(waited).to be_between(0.1, 0.1 + 0.5)
      else
        expect(waited).to be < 0.1
      end
      expect(admitted(transport)).to eq(1)
      gate << true
      eventually("admission released after the owner resumes") { admitted(transport).zero? }
    ensure
      gate&.<<(true)
      transport&.close(grace: 0)
      peer&.stop
    end
  end

  it "enqueues an explicit abandon without waiting and keeps the normal outcome" do
    contexts = {}
    release = Thread::Queue.new
    peer = threaded_host(server: mixed_server(watch: lambda { |message, writer, context|
      contexts[message.name] = context
      writer.send_message(message)
      release.pop
    }))
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    typed = mixed_client(transport, origin)
    typed.lookup(item("warm"))
    gates = Thread::Queue.new
    durations = Thread::Queue.new
    caller = Thread.new do
      typed.watch(item("abandoned"), timeout: nil) do |reader|
        reader.receive
        gates << hold_owner(transport)
        started = monotonic
        reader.abandon
        durations << (monotonic - started)
        :left
      end
    end
    gate = pop_within(gates, "the owner held inside the block")
    expect(pop_within(durations, "the abandon to return")).to be < 0.5
    eventually("the caller waiting for the terminal outcome") { exited_mailbox(transport) && blocked?(caller) }
    gate << true
    expect(value_within(caller, "the abandoned stream")).to be_nil
    eventually("actual peer CANCEL reached the handler") { contexts.fetch("abandoned").cancelled? }
    expect(admitted(transport)).to eq(0)
  ensure
    gate&.<<(true)
    release&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end
end
