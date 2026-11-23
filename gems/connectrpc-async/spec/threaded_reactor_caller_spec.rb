# frozen_string_literal: true

require "open3"

$LOAD_PATH.unshift(File.expand_path("../../../spec/fixtures/generation/ruby", __dir__))
require "generation/streaming_connect"
require_relative "support/threaded_stream_helpers"

# A caller inside a reactor skips the caller-side interrupt masks, so a fiber
# suspended on a contended lock never defers interrupts for the whole reactor
# thread (#201). Ordinary threads keep the masks.
RSpec.describe "ThreadedTransport callers inside a reactor" do
  include ThreadedStreamHelpers

  # Holds the admission lock on the owner thread until the gate opens, so the
  # next caller to take it suspends or blocks there.
  def hold_admission(transport)
    engine = engine_of(transport)
    held = Thread::Queue.new
    gate = Thread::Queue.new
    engine.run_on_owner(lambda {
      engine.admission.lock.synchronize do
        held << true
        Fiber.blocking { gate.pop }
      end
    })
    pop_within(held, "the owner to hold the admission lock")
    gate
  end

  def frames(trace) = (trace || []).join("\n")

  def gated_host(contexts, entered, release)
    threaded_host(server: server(echo: lambda { |message, context|
      contexts[message.name] = context
      entered << message.name
      release.pop unless message.name.start_with?("free")
      message
    }))
  end

  # The child runs the reactor on its main thread, where Ruby's default SIGINT
  # handler raises Interrupt; RSpec traps SIGINT in this process.
  def sigint_child = <<~RUBY
    $stdout.sync = true
    require "connectrpc/async"
    kind = ARGV.fetch(0).to_sym
    url = URI("http://127.0.0.1:9/svc/Method")
    transport = ConnectRPC::Async::ThreadedTransport.new(origin: "http://127.0.0.1:9")
    engine = transport.send(:live_engine)
    held = Thread::Queue.new
    gate = Thread::Queue.new
    engine.run_on_owner(lambda {
      engine.admission.lock.synchronize do
        held << true
        Fiber.blocking { gate.pop }
      end
    })
    held.pop
    begin
      Sync do |task|
        caller = task.async do
          case kind
          when :unary
            transport.call(ConnectRPC::TransportRequest.new(url: url, http_method: "POST", headers: {}, body: "x",
                                                            response_limit: 100))
          when :stream
            transport.open_stream(ConnectRPC::StreamRequest.new(url: url, http_method: "POST", headers: {})) { nil }
          else
            transport.close(grace: 0)
          end
        end
        # Async runs the new task until it first suspends: on the held lock.
        frame = caller.fiber.backtrace.find { |line| line.include?("Mutex#synchronize") }
        index = caller.fiber.backtrace.index(frame)
        puts "suspended=\#{caller.fiber.backtrace[index + 1][/Engine#\\w+/]}"
        Process.kill(:INT, Process.pid)
        caller.wait
      end
      puts "outcome=returned"
    rescue Interrupt
      puts "outcome=Interrupt"
    rescue Exception => e
      puts "outcome=\#{e.class}: \#{e.message}"
    ensure
      gate << true
    end
    puts "admitted=\#{engine.admission.lock.synchronize { engine.admission.count }}"
    transport.close(grace: 0)
    puts "closed"
  RUBY

  def run_child(kind, timeout: 30)
    Open3.popen3(RbConfig.ruby, "-e", sigint_child, kind.to_s) do |stdin, stdout, stderr, wait|
      stdin.close
      unless wait.join(timeout)
        Process.kill(:KILL, wait.pid)
        raise "SIGINT child #{kind} timed out; stdout: #{stdout.read.inspect}"
      end
      [stdout.read, stderr.read, wait.value]
    end
  end

  {
    unary: "Engine#admit", stream: "Engine#admit", close: "Engine#request_close"
  }.each do |kind, frame|
    it "ends Sync with Interrupt on SIGINT while a #{kind} caller waits for the contended admission lock" do
      output, errors, status = run_child(kind)
      expect([output.lines(chomp: true), status.success?]).to eq(
        [["suspended=#{frame}", "outcome=Interrupt", "admitted=0", "closed"], true]
      ), "stderr: #{errors}"
    end
  end

  it "stops a caller waiting for the contended admission lock at once and admits nothing" do
    peer = threaded_host(server: server)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    typed = client(transport, origin)
    typed.echo(AsyncHelpers::MESSAGE.new(name: "warm"))
    gate = hold_admission(transport)
    error = Sync do |task|
      caller = task.async { capture_error { typed.echo(AsyncHelpers::MESSAGE.new(name: "stopped")) } }
      expect(frames(caller.fiber.backtrace)).to include("Engine#admit")
      caller.stop
      caller.wait
    end
    expect(error).to be_a(Async::Cancel)
    expect(frames(error.backtrace)).to include("Engine#admit")
    gate << true
    expect(admitted(transport)).to eq(0)
    expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "after")).message.name).to eq("after")
  ensure
    gate&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  # Masks still defer Thread#raise on an ordinary thread until the call's
  # wait, so the call is admitted and then abandoned, never half settled.
  it "defers Thread#raise into an ordinary thread caller waiting for the admission lock until its wait" do
    contexts = {}
    entered = Thread::Queue.new
    release = Thread::Queue.new
    peer = gated_host(contexts, entered, release)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    typed = client(transport, origin)
    typed.echo(AsyncHelpers::MESSAGE.new(name: "free warm"))
    gate = hold_admission(transport)
    interrupt = Class.new(StandardError)
    caller = Thread.new { capture_error { typed.echo(AsyncHelpers::MESSAGE.new(name: "masked"), timeout: nil) } }
    eventually("the caller blocked on the admission lock") do
      blocked?(caller) && frames(caller.backtrace).include?("Engine#admit")
    end
    caller.raise(interrupt, "interrupted")
    gate << true
    error = value_within(caller, "the interrupted caller")
    expect(error).to be_a(interrupt)
    expect(frames(error.backtrace)).to include("#await")
    expect(frames(error.backtrace)).not_to include("Engine#admit")
    release << true
    eventually("admission released") { admitted(transport).zero? }
  ensure
    gate&.<<(true)
    release&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  [
    [:stop, Async::Cancel],
    [:with_timeout, Async::TimeoutError]
  ].each do |kind, error_class|
    it "resets a reactor caller's unary call on #{kind} and releases admission" do
      contexts = {}
      entered = Thread::Queue.new
      release = Thread::Queue.new
      peer = gated_host(contexts, entered, release)
      origin = "http://127.0.0.1:#{peer.peer.port}"
      transport = threaded(origin: origin)
      typed = client(transport, origin)
      error = Sync do |task|
        caller = task.async do |inner|
          capture_error do
            work = -> { typed.echo(AsyncHelpers::MESSAGE.new(name: "victim"), timeout: nil) }
            # The 2 s bound is the cancellation under test; the handler is entered long before.
            kind == :with_timeout ? inner.with_timeout(2) { work.call } : work.call
          end
        end
        expect(pop_within(entered, "the victim in the handler")).to eq("victim")
        caller.stop if kind == :stop
        caller.wait
      end
      expect(error).to be_a(error_class)
      eventually("actual peer CANCEL reached the handler Context") { contexts.fetch("victim").cancelled? }
      expect(contexts.fetch("victim").cancellation_reason).to eq(:canceled)
      release << true
      eventually("admission released") { admitted(transport).zero? }
      expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "free next")).message.name).to eq("free next")
      expect(peer.peer.accepts.size).to eq(1)
    ensure
      release&.<<(true)
      transport&.close(grace: 0)
      peer&.stop
    end
  end

  it "resets a reactor caller's stream when its task stops during a read and releases admission" do
    contexts = {}
    entered = Thread::Queue.new
    release = Thread::Queue.new
    peer = threaded_host(server: mixed_server(watch: lambda { |message, writer, context|
      contexts[message.name] = context
      writer.send_message(message)
      entered << message.name
      release.pop
      writer.send_message(item("#{message.name}-last"))
    }))
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    typed = mixed_client(transport, origin)
    first = Thread::Queue.new
    error = Sync do |task|
      caller = task.async do
        capture_error do
          typed.watch(item("victim"), timeout: nil) do |reader|
            first << reader.receive.name
            reader.receive
          end
        end
      end
      expect(pop_within(entered, "the victim in the handler")).to eq("victim")
      expect(pop_within(first, "the victim's first message")).to eq("victim")
      eventually("the second read waiting on the owner") { stream_mailboxes(transport).one?(&:busy) }
      caller.stop
      caller.wait
    end
    expect(error).to be_a(Async::Cancel)
    eventually("actual peer CANCEL reached the handler Context") { contexts.fetch("victim").cancelled? }
    release << true
    eventually("admission released") { admitted(transport).zero? }
    names = []
    typed.watch(item("next"), timeout: nil) do |reader|
      release << true
      reader.each { |reply| names << reply.name }
    end
    expect(names).to eq(%w[next next-last])
  ensure
    2.times { release&.<<(true) }
    transport&.close(grace: 0)
    peer&.stop
  end
end
