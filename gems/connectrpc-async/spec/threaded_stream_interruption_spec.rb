# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../../../spec/fixtures/generation/ruby", __dir__))
require "generation/streaming_connect"
require "protocol/http/body/writable"
require_relative "support/threaded_stream_helpers"

RSpec.describe "ThreadedTransport stream interruption" do
  include ThreadedStreamHelpers

  # Each watch sends one message, then waits for a release before its last.
  def gated_watch_host(contexts, entered, release)
    threaded_host(server: mixed_server(watch: lambda { |message, writer, context|
      contexts[message.name] = context
      writer.send_message(message)
      entered << message.name
      release.pop
      writer.send_message(item("#{message.name}-last"))
    }))
  end

  def busy_streams(transport) = stream_mailboxes(transport).count(&:busy)

  [
    [:timeout, Timeout::Error],
    [:raise, Class.new(StandardError)]
  ].each do |kind, error_class|
    it "resets only the stream whose nil-deadline read a #{kind} interrupts and holds admission until retirement" do
      contexts = {}
      entered = Thread::Queue.new
      release = Thread::Queue.new
      peer = gated_watch_host(contexts, entered, release)
      origin = "http://127.0.0.1:#{peer.peer.port}"
      transport = threaded(origin: origin)
      typed = mixed_client(transport, origin)
      sibling = Thread.new do
        names = []
        typed.watch(item("sibling"), timeout: nil) { |reader| reader.each { |reply| names << reply.name } }
        names
      end
      expect(pop_within(entered, "the sibling in the handler")).to eq("sibling")
      first = Thread::Queue.new
      victim = Thread.new do
        capture_error do
          work = lambda do
            typed.watch(item("victim"), timeout: nil) do |reader|
              first << reader.receive.name
              reader.receive
            end
          end
          # The 2 s Timeout is the interruption under test; the read blocks long before.
          kind == :timeout ? Timeout.timeout(2) { work.call } : work.call
        end
      end
      expect(pop_within(entered, "the victim in the handler")).to eq("victim")
      expect(pop_within(first, "the victim's first message")).to eq("victim")
      eventually("both second reads waiting on the owner") { busy_streams(transport) == 2 && blocked?(victim) }
      gate = hold_owner(transport)
      victim.raise(error_class, "interrupted") if kind == :raise
      expect(value_within(victim, "the interrupted victim")).to be_a(error_class)
      expect(admitted(transport)).to eq(2)
      gate << true
      eventually("actual peer CANCEL reached the victim handler") { contexts.fetch("victim").cancelled? }
      expect(contexts.fetch("sibling")).not_to be_cancelled
      eventually("the victim retired") { admitted(transport) == 1 }
      2.times { release << true }
      expect(value_within(sibling, "the sibling")).to eq(%w[sibling sibling-last])
      expect(admitted(transport)).to eq(0)
      expect(mixed_client(transport, origin).lookup(item("after")).message.name).to eq("after")
      expect(peer.peer.accepts.size).to eq(1)
    ensure
      gate&.<<(true)
      3.times { release&.<<(true) }
      transport&.close(grace: 0)
      peer&.stop
    end
  end

  # The block rescues the interrupt and stays inside, so only the operation's
  # own break can reset the stream before the block exits.
  it "resets a stream whose write is blocked by flow control at the interrupt, while a sibling progresses" do
    sibling_body = Protocol::HTTP::Body::Writable.new
    peer = threaded_recording_peer(settings: { Protocol::HTTP2::Settings::INITIAL_WINDOW_SIZE => 0 }) do |incoming|
      body = incoming.path == "/sibling" ? sibling_body : Protocol::HTTP::Body::Writable.new
      incoming.send_response(Protocol::HTTP::Response[200, {}, body])
    end
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    chunks = Thread::Queue.new
    sibling = Thread.new do
      transport.open_stream(stream_request(origin, "/sibling")) do |exchange|
        exchange.response_head
        2.times.map { chunks << exchange.read(64) }
      end
    end
    sibling_body.write("before")
    expect(pop_within(chunks, "the sibling's first chunk")).to eq("before")
    interrupt = Class.new(StandardError)
    writes = Thread::Queue.new
    rescued = Thread::Queue.new
    hold = Thread::Queue.new
    victim = Thread.new do
      capture_error do
        transport.open_stream(stream_request(origin, "/victim")) do |exchange|
          loop do
            exchange.write("x" * 16_384)
            writes << true
          end
        rescue interrupt => e
          rescued << e
          hold.pop
          :returned
        end
      end
    end
    2.times { pop_within(writes, "the queued and in-flight fragments") }
    eventually("a third write blocked by the zero window") do
      victim_stream = stream_mailboxes(transport).max_by(&:seq)
      victim_stream.busy && victim_stream.seq >= 3 && blocked?(victim)
    end
    expect(peer.peer.data_frames).to be_empty
    victim.raise(interrupt, "interrupted")
    expect(pop_within(rescued, "the original interrupt inside the block")).to be_a(interrupt)
    eventually("the victim stream reset on the wire while its block still runs") do
      peer.peer.frames.any? { |type, id, _| type == Protocol::HTTP2::ResetStreamFrame::TYPE && id == 3 }
    end
    sibling_body.write("after")
    expect(pop_within(chunks, "the sibling after the reset")).to eq("after")
    hold << true
    error = value_within(victim, "the victim's normal return after a broken stream")
    expect(error).to be_a(ConnectRPC::Error)
    expect(error.code).to eq("canceled")
    sibling_body.close_write
    value_within(sibling, "the sibling")
    eventually("both streams retired") { admitted(transport).zero? }
    expect(peer.peer.accepted).to eq(1)
  ensure
    hold&.<<(true)
    sibling_body&.close_write
    transport&.close(grace: 0)
    peer&.stop
  end

  it "resets a stream when Thread#kill stops its blocked read" do
    contexts = {}
    entered = Thread::Queue.new
    release = Thread::Queue.new
    peer = gated_watch_host(contexts, entered, release)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    typed = mixed_client(transport, origin)
    victim = Thread.new { typed.watch(item("killed"), timeout: nil) { |reader| reader.each { nil } } }
    pop_within(entered, "the killed stream in the handler")
    eventually("the read waiting on the owner") { busy_streams(transport) == 1 && blocked?(victim) }
    victim.kill
    expect(victim.join(5)).to equal(victim)
    eventually("actual peer CANCEL reached the handler") { contexts.fetch("killed").cancelled? }
    eventually("admission released") { admitted(transport).zero? }
    release << true
    expect(mixed_client(transport, origin).lookup(item("next")).message.name).to eq("next")
    expect(peer.peer.accepts.size).to eq(1)
  ensure
    release&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  # Ruby cannot read the current mask, so the block runs interruptible even
  # inside an enclosing handle_interrupt(... => :never) region.
  it "interrupts a blocked stream read inside an enclosing never-interrupt region" do
    contexts = {}
    entered = Thread::Queue.new
    release = Thread::Queue.new
    peer = gated_watch_host(contexts, entered, release)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    typed = mixed_client(transport, origin)
    interrupt = Class.new(StandardError)
    victim = Thread.new do
      capture_error do
        Thread.handle_interrupt(Object => :never) do
          typed.watch(item("masked"), timeout: nil) { |reader| reader.each { nil } }
        end
      end
    end
    pop_within(entered, "the masked stream in the handler")
    eventually("the read waiting on the owner") { busy_streams(transport) == 1 && blocked?(victim) }
    victim.raise(interrupt, "interrupted")
    expect(value_within(victim, "the masked caller")).to be_a(interrupt)
    eventually("actual peer CANCEL reached the handler") { contexts.fetch("masked").cancelled? }
    eventually("admission released") { admitted(transport).zero? }
  ensure
    release&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  it "keeps late reactor lifecycle use away from an interrupted caller's handle" do
    contexts = {}
    entered = Thread::Queue.new
    release = Thread::Queue.new
    peer = gated_watch_host(contexts, entered, release)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    log = []
    guard = Mutex.new
    record = ->(event) { guard.synchronize { log << event } }
    lifecycle = Object.new
    lifecycle.define_singleton_method(:activate) do |&block|
      record.call(:activate)
      block.call
    ensure
      record.call(:activated)
    end
    lifecycle.define_singleton_method(:record_failure) { |_| record.call(:record_failure) }
    lifecycle.define_singleton_method(:finish) { |**| record.call(:finish) }
    interrupt = Class.new(StandardError)
    victim = Thread.new do
      capture_error do
        transport.open_stream(stream_request(origin, watch_method, lifecycle: lifecycle)) do |exchange|
          exchange.write(envelope(item("observed")))
          exchange.close_send
          exchange.read(1024) while exchange.read(1024)
        end
      end
    end
    pop_within(entered, "the observed stream in the handler")
    # One unmatched activation means the reactor is inside the blocked read.
    eventually("the blocked read inside its activation") do
      busy_streams(transport) == 1 && guard.synchronize { log.count(:activate) - log.count(:activated) == 1 }
    end
    marker = guard.synchronize { log.size }
    victim.raise(interrupt, "interrupted")
    expect(value_within(victim, "the interrupted caller")).to be_a(interrupt)
    eventually("actual peer CANCEL reached the handler") { contexts.fetch("observed").cancelled? }
    eventually("the reactor retired the stream") { admitted(transport).zero? }
    expect(guard.synchronize { log.drop(marker) }).to eq([:activated])
  ensure
    release&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end
end
