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

  # Makes the next owned connection release fail after it succeeds, on the owner.
  def fail_release(transport)
    on_owner(transport) do
      manager = engine_of(transport).inner.instance_variable_get(:@manager)
      release = manager.method(:release)
      manager.define_singleton_method(:release) do |connection|
        release.call(connection)
        raise IOError, "private owned cleanup failure"
      end
    end
  end

  def exited_mailbox(transport) = stream_mailboxes(transport).find(&:exited)

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
      expect([error.code, error.message, error.cause]).to eq(["unavailable", "HTTP/2 transport cleanup failed", nil])
      expect(outcomes.last.normalized_error).to equal(error)
      expect(completions.last.delivery).to eq(:aborted)
      expect(completions.last.error.message).to eq("private owned cleanup failure")
      expect(admitted(transport)).to eq(0)
    ensure
      transport&.close(grace: 0)
      peer&.stop
    end
  end

  # Exceptional exits never wait, so the engine's late delivery abort is not
  # reported; the remote error itself reaches the caller unchanged.
  it "keeps a delivered remote gRPC error when the owner's later cleanup fails" do
    peer = threaded_host(server: mixed_server(watch: lambda { |message, writer, _|
      writer.send_message(message)
      raise ConnectRPC::Error.new(:not_found, "remote", trailers: { "x-end" => %w[c d] })
    }))
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    typed = mixed_client(transport, origin, protocol: :grpc)
    typed.lookup(item("warm"))
    fail_release(transport)
    names = []
    error = capture_error { typed.watch(item("remote")) { |reader| reader.each { |reply| names << reply.name } } }
    expect([error.class, error.code, error.message, error.cause]).to eq([ConnectRPC::Error, "not_found", "remote", nil])
    expect([names, error.trailers["x-end"]]).to eq([%w[remote], %w[c d]])
    eventually("admission released") { admitted(transport).zero? }
  ensure
    transport&.close(grace: 0)
    peer&.stop
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
