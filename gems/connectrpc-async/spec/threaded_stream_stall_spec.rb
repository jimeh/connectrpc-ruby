# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../../../spec/fixtures/generation/ruby", __dir__))
require "generation/streaming_connect"
require_relative "support/threaded_stream_helpers"

RSpec.describe "ThreadedTransport streams with stalled consumers, owners and close" do
  include ThreadedStreamHelpers

  def gated_host(entered, release)
    threaded_host(server: mixed_server(watch: lambda { |message, writer, _|
      entered << message.name
      release.pop
      writer.send_message(message)
    }))
  end

  def busy_streams(transport) = stream_mailboxes(transport).count(&:busy)

  # The smallest stream receive window left among the inner Transport's streams.
  def receive_windows(transport)
    on_owner(transport) do
      engine_of(transport).inner.instance_variable_get(:@calls).keys.filter_map do |operation|
        response = operation[:response]
        response.stream.local_window.available if response
      end
    end.last
  end

  it "keeps another thread's stream progressing while a consumer holds a flow-controlled stream" do
    sent = Thread::Queue.new
    peer = threaded_host(server: mixed_server(watch: lambda { |message, writer, _|
      count = message.name == "stalled" ? 40 : 2
      count.times do |index|
        writer.send_message(item("#{message.name}-#{index}-#{'x' * 8192}"))
        sent << [message.name, index]
      end
    }))
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    typed = mixed_client(transport, origin)
    first = Thread::Queue.new
    resume = Thread::Queue.new
    stalled = Thread.new do
      count = 0
      typed.watch(item("stalled"), timeout: nil) do |reader|
        first << reader.receive.name
        resume.pop
        reader.each { count += 1 }
      end
      count + 1
    end
    expect(pop_within(first, "the stalled consumer's first message")).to start_with("stalled-0-")
    # Real backpressure: the stalled stream's receive window is spent and the
    # handler has not produced every message.
    eventually("the stalled stream's receive window exhausted") { receive_windows(transport) == [0] }
    produced = []
    produced << sent.pop until sent.empty?
    expect(produced.size).to be < 40
    sibling = Thread.new do
      names = []
      typed.watch(item("sibling"), timeout: nil) { |reader| reader.each { |reply| names << reply.name[0, 9] } }
      names
    end
    expect(value_within(sibling, "the sibling beside the stalled consumer")).to eq(%w[sibling-0 sibling-1])
    expect(receive_windows(transport)).to eq([0])
    resume << true
    expect(value_within(stalled, "the resumed consumer")).to eq(40)
    expect([peer.peer.accepts.size, admitted(transport)]).to eq([1, 0])
  ensure
    resume&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  it "fails a stalled-owner read at the deadline plus the allowance and holds admission until release" do
    entered = Thread::Queue.new
    release = Thread::Queue.new
    peer = gated_host(entered, release)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    typed = mixed_client(transport, origin)
    started = nil
    caller = Thread.new do
      started = monotonic
      capture_error { typed.watch(item("stalled"), timeout: 0.6) { |reader| reader.each { nil } } }
    end
    pop_within(entered, "the stream in the handler")
    eventually("the read waiting on the owner") { busy_streams(transport) == 1 && blocked?(caller) }
    gate = hold_owner(transport)
    error = value_within(caller, "the stalled read")
    expect([error.class, error.code]).to eq([ConnectRPC::Error, "deadline_exceeded"])
    expect(monotonic - started).to be_between(0.6, 0.6 + 0.1 + 0.5)
    expect(admitted(transport)).to eq(1)
    gate << true
    eventually("admission released after the owner resumes") { admitted(transport).zero? }
  ensure
    gate&.<<(true)
    release&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  it "fails a stalled-owner operation at its own timeout plus the allowance with unavailable" do
    peer = threaded_host(server: mixed_server)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    mixed_client(transport, origin).lookup(item("warm"))
    gates = Thread::Queue.new
    caller = Thread.new do
      capture_error do
        transport.open_stream(stream_request(origin, watch_method)) do |exchange|
          gates << hold_owner(transport)
          started = monotonic
          begin
            exchange.response_head(timeout: 0.2)
          ensure
            gates << (monotonic - started)
          end
        end
      end
    end
    gate = pop_within(gates, "the owner held inside the block")
    expect(pop_within(gates, "the timed operation")).to be_between(0.2, 0.2 + 0.1 + 0.5)
    error = value_within(caller, "the expired operation")
    expect([error.class, error.code, error.message])
      .to eq([ConnectRPC::Error, "unavailable", "HTTP/2 transport failed"])
    expect(admitted(transport)).to eq(1)
    gate << true
    eventually("admission released after the owner resumes") { admitted(transport).zero? }
  ensure
    gate&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  it "reports canceled, not deadline_exceeded, for cancellation accepted while the owner stalls a read" do
    entered = Thread::Queue.new
    release = Thread::Queue.new
    peer = gated_host(entered, release)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    typed = mixed_client(transport, origin)
    source = ConnectRPC::CancellationSource.new
    caller = Thread.new do
      capture_error do
        typed.watch(item("cancel"), timeout: 5, cancellation: source.token) { |reader| reader.each { nil } }
      end
    end
    pop_within(entered, "the stream in the handler")
    eventually("the read waiting on the owner") { busy_streams(transport) == 1 && blocked?(caller) }
    gate = hold_owner(transport)
    started = monotonic
    value_within(Thread.new { source.cancel }, "the cancelling thread")
    error = value_within(caller, "the cancelled read")
    expect([error.class, error.code]).to eq([ConnectRPC::Error, "canceled"])
    expect(monotonic - started).to be < 1
    expect(admitted(transport)).to eq(1)
    gate << true
    eventually("admission released") { admitted(transport).zero? }
  ensure
    gate&.<<(true)
    release&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  it "wakes a pending stream operation with unavailable when the owner fails, then restarts" do
    entered = Thread::Queue.new
    release = Thread::Queue.new
    peer = gated_host(entered, release)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    typed = mixed_client(transport, origin)
    caller = Thread.new do
      capture_error { typed.watch(item("pending"), timeout: nil) { |reader| reader.each { nil } } }
    end
    pop_within(entered, "the stream in the handler")
    eventually("the read waiting on the owner") { busy_streams(transport) == 1 && blocked?(caller) }
    failed = engine_of(transport)
    started = monotonic
    failed.run_on_owner(-> { raise "injected owner failure" })
    error = value_within(caller, "the pending stream after the owner failed")
    expect([error.class, error.code]).to eq([ConnectRPC::Error, "unavailable"])
    expect(monotonic - started).to be < 1
    expect(failed.thread.join(5)).to equal(failed.thread)
    expect(admitted(transport)).to eq(0)
    release << true
    expect(typed.lookup(item("restarted")).message.name).to eq("restarted")
    expect(engine_of(transport).thread).not_to equal(failed.thread)
  ensure
    release&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  it "drains an open stream during graceful close and refuses new streams" do
    entered = Thread::Queue.new
    release = Thread::Queue.new
    peer = gated_host(entered, release)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    typed = mixed_client(transport, origin)
    caller = Thread.new do
      names = []
      typed.watch(item("draining"), timeout: nil) { |reader| reader.each { |reply| names << reply.name } }
      names
    end
    pop_within(entered, "the stream in the handler")
    closer = Thread.new { transport.close(grace: 5) }
    eventually("the closer waiting") { transport.connection_state == :closing && blocked?(closer) }
    expect { typed.watch(item("late"), timeout: nil) { nil } }.to unavailable_error
    release << true
    expect(value_within(caller, "the drained stream")).to eq(%w[draining])
    expect(value_within(closer, "the closer")).to be(true)
    expect(admitted(transport)).to eq(0)
  ensure
    release&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  it "returns false from close with a stalled owner and wakes a pending stream operation" do
    entered = Thread::Queue.new
    release = Thread::Queue.new
    peer = gated_host(entered, release)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    typed = mixed_client(transport, origin)
    caller = Thread.new { capture_error { typed.watch(item("stuck"), timeout: nil) { |reader| reader.each { nil } } } }
    pop_within(entered, "the stream in the handler")
    eventually("the read waiting on the owner") { busy_streams(transport) == 1 && blocked?(caller) }
    owner = engine_of(transport).thread
    gate = hold_owner(transport)
    started = monotonic
    expect(transport.close(grace: 0.2)).to be(false)
    expect(monotonic - started).to be_between(0.3, 1.0)
    error = value_within(caller, "the stalled stream")
    expect([error.class, error.code]).to eq([ConnectRPC::Error, "unavailable"])
    gate << true
    expect(owner.join(5)).to equal(owner)
    expect(admitted(transport)).to eq(0)
  ensure
    gate&.<<(true)
    release&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end
end
