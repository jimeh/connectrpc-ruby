# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../../../spec/fixtures/generation/ruby", __dir__))
require "generation/streaming_connect"
require_relative "support/threaded_stream_helpers"
require_relative "support/per_call_helpers"

RSpec.describe "PerCallTransport deadlines, cancellation and interrupts" do
  include ThreadedStreamHelpers
  include PerCallHelpers

  # Handlers record their Context, report entry and wait for a release.
  def gated_host(contexts, entered, release)
    threaded_host(server: server(echo: lambda { |message, context|
      contexts[message.name] = context
      entered << message.name
      release.pop
      message
    }))
  end

  def expect_peer_cancel(contexts, name, reasons = [:canceled])
    eventually("actual peer CANCEL reached the #{name} handler Context") { contexts.fetch(name).cancelled? }
    expect(reasons).to include(contexts.fetch(name).cancellation_reason)
  end

  # A peer that accepts TCP but holds TLS and its first SETTINGS, so the
  # client stays in connection setup.
  def setup_holding_peer
    ThreadedHelpers::PeerThread.new do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      gate = Async::Queue.new
      peer = ConnectionPeer::RecordingServer.new(task, bound.sockets.first, gate: gate)
      peer.define_singleton_method(:port) { bound.sockets.first.local_address.ip_port }
      [peer, lambda {
        # Held connections continue to their client's EOF and end.
        peer.accepted.times { gate.enqueue(true) }
        peer.stop
        bound.close
      }]
    end
  end

  def threaded_class = ConnectRPC::Async::PerCallTransport.const_get(:Threaded)

  # A raw peer that records each request's entry and never answers it, so
  # the client's stream stays open until the client resets it.
  def holding_peer(entered) = reset_recording_peer { |incoming| entered << incoming.path }

  def expect_cancel_reset(peer)
    eventually("RST_STREAM(CANCEL) at the peer") { !peer.peer.resets.empty? }
    expect(peer.peer.resets).to eq([[0, 1, Protocol::HTTP2::Error::CANCEL]])
  end

  # Holds the stream's own cleanup task, once, as it starts retiring the
  # stream, until the returned gate opens. `started` resolves on entry.
  def hold_stream_cleanup(started, gate)
    armed = [true]
    trace = TracePoint.new(:call) do
      next unless armed.shift

      started.call
      gate.call
    end
    trace.enable(target: ConnectRPC::Async::Transport.instance_method(:retire_stopped_output))
    trace
  end

  context "on the threaded path" do
    attr_reader :contexts, :entered, :release

    before do
      @contexts = {}
      @entered = Thread::Queue.new
      @release = Thread::Queue.new
    end

    after { 4.times { release << true } }

    # The raw peer has no deadline of its own and never answers, so the
    # reset it records can only be the client's.
    it "fails at the deadline and resets the stream" do
      peer = holding_peer(entered)
      origin = "http://127.0.0.1:#{peer.peer.port}"
      typed = client(per_call, origin)
      with_boundaries do |boundaries|
        error = on_path(:threaded) { typed.echo(AsyncHelpers::MESSAGE.new(name: "late"), timeout: 0.3) }
        expect([error.class, error.code]).to eq([ConnectRPC::Error, "deadline_exceeded"])
        expect(pop_within(entered, "the request at the peer")).to eq("/example.v1.EchoService/Echo")
        expect_released(boundaries.take.first)
        expect_cancel_reset(peer)
      end
    ensure
      peer&.stop
    end

    it "cancels through a token from another thread and resets the stream" do
      peer = gated_host(contexts, entered, release)
      origin = "http://127.0.0.1:#{peer.peer.port}"
      typed = client(per_call, origin)
      source = ConnectRPC::CancellationSource.new
      with_boundaries do |boundaries|
        caller = Thread.new do
          capture_error { typed.echo(AsyncHelpers::MESSAGE.new(name: "token"), cancellation: source.token) }
        end
        expect(pop_within(entered, "the token call in the handler")).to eq("token")
        value_within(Thread.new { source.cancel }, "the cancelling thread")
        error = value_within(caller, "the cancelled caller")
        expect([error.class, error.code]).to eq([ConnectRPC::Error, "canceled"])
        expect_peer_cancel(contexts, "token")
        expect_released(boundaries.take.first)
      end
    ensure
      peer&.stop
    end

    [
      [:timeout, Timeout::Error],
      [:raise, Class.new(StandardError)]
    ].each do |kind, error_class|
      it "propagates a #{kind} interrupt unchanged once the owner thread has exited, and resets the stream" do
        peer = gated_host(contexts, entered, release)
        origin = "http://127.0.0.1:#{peer.peer.port}"
        typed = client(per_call, origin)
        with_boundaries do |boundaries|
          victim = Thread.new do
            capture_error do
              if kind == :timeout
                # The 2 s Timeout is the interruption under test; the handler is entered long before.
                Timeout.timeout(2) { typed.echo(AsyncHelpers::MESSAGE.new(name: "victim")) }
              else
                typed.echo(AsyncHelpers::MESSAGE.new(name: "victim"))
              end
            end
          end
          expect(pop_within(entered, "the victim in the handler")).to eq("victim")
          victim.raise(error_class, "interrupted") if kind == :raise
          expect(value_within(victim, "the interrupted victim")).to be_a(error_class)
          expect_peer_cancel(contexts, "victim")
          expect_released(boundaries.take.first)
        end
      ensure
        peer&.stop
      end
    end

    it "resets the stream and joins the owner when Thread#kill stops the caller" do
      peer = gated_host(contexts, entered, release)
      origin = "http://127.0.0.1:#{peer.peer.port}"
      typed = client(per_call, origin)
      with_boundaries do |boundaries|
        victim = Thread.new { typed.echo(AsyncHelpers::MESSAGE.new(name: "killed")) }
        expect(pop_within(entered, "the killed call in the handler")).to eq("killed")
        victim.kill
        expect(victim.join(5)).to equal(victim)
        expect_peer_cancel(contexts, "killed")
        expect_released(boundaries.take.first)
      end
    ensure
      peer&.stop
    end

    it "unwinds an interrupt during connection setup without leaving the owner or its socket" do
      peer = setup_holding_peer
      origin = "http://127.0.0.1:#{peer.peer.port}"
      transport = per_call
      interrupt = Class.new(StandardError)
      with_boundaries do |boundaries|
        victim = Thread.new { capture_error { transport.call(request("#{origin}/setup")) } }
        eventually("the peer accepted the connection under setup") { peer.peer.accepted == 1 }
        victim.raise(interrupt, "during setup")
        expect(value_within(victim, "the victim interrupted during setup")).to be_a(interrupt)
        expect_released(boundaries.take.first)
        expect(peer.peer.headers_frames).to be_empty
      end
    ensure
      peer&.stop
    end

    # The caller is held just before its close, so both interrupts are
    # deferred into the close's own wait. The owner still drains gracefully
    # and is joined before the interrupt propagates.
    [1, 2].each do |count|
      it "joins the owner before re-raising #{count} interrupt(s) landing in the close" do
        peer = threaded_recording_peer
        origin = "http://127.0.0.1:#{peer.peer.port}"
        transport = per_call
        interrupt = Class.new(StandardError)
        start = Thread::Queue.new
        pause = nil
        with_boundaries do |boundaries|
          victim = Thread.new do
            start.pop
            capture_error { transport.call(request("#{origin}/closing")) }
          end
          close_joined = threaded_class.instance_method(:close_joined)
          pause = ThreadedHelpers::LinePause.new(close_joined, victim, "@cutoff = cutoff")
          start << true
          pop_within(pause.held, "the caller to reach its close")
          count.times { |index| victim.raise(interrupt, "interrupt #{index}") }
          pause.open
          error = value_within(victim, "the caller interrupted in close")
          expect([error.class, error.message]).to eq([interrupt, "interrupt #{count - 1}"])
          expect_released(boundaries.take.first)
          # The peer records the GOAWAY on its own reader, which can trail the call's return.
          eventually("the peer to record the GOAWAY") { peer.peer.goaways.any? }
          expect(peer.peer.goaways.map { |_, _, code| code }).to eq([Protocol::HTTP2::Error::NO_ERROR])
        end
      ensure
        pause&.open
        peer&.stop
      end
    end
  end

  # The stream block returns, so the caller waits for the owner's terminal
  # outcome while the owner's own stream cleanup is held. An interrupt lands
  # in that wait; the caller is then held before its close, so the owner can
  # finish only after the interrupt has landed.
  it "joins the owner before re-raising an interrupt that lands while the stream exits on the threaded path" do
    peer = threaded_host(server: mixed_server)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    interrupt = Class.new(StandardError)
    held = Thread::Queue.new
    owner_gate = Thread::Queue.new
    cleanup = hold_stream_cleanup(-> { held << Thread.current }, -> { Fiber.blocking { owner_gate.pop } })
    pause = nil
    with_boundaries do |boundaries|
      start = Thread::Queue.new
      victim = Thread.new do
        start.pop
        capture_error { mixed_client(per_call, origin).watch(item("exit")) { |reader| reader.each { nil } } }
      end
      pause = ThreadedHelpers::LinePause.new(threaded_class.instance_method(:close_joined), victim,
                                             "@cutoff = cutoff")
      start << true
      owner = pop_within(held, "the owner's stream cleanup")
      expect(owner).not_to equal(victim)
      victim.raise(interrupt, "while the stream exits")
      pop_within(pause.held, "the interrupted caller to reach its close")
      owner_gate << true
      pause.open
      error = value_within(victim, "the caller interrupted in the stream exit")
      expect([error.class, error.message]).to eq([interrupt, "while the stream exits"])
      observation = boundaries.take.first
      expect(observation.fetch(:owners)).to eq([owner])
      expect_released(observation)
    end
  ensure
    cleanup&.disable
    owner_gate&.<<(true)
    pause&.open
    peer&.stop
  end

  context "on the inline path" do
    def gated_reactor_host(entered, release)
      target = server(echo: lambda { |message, _context|
        entered.enqueue(message.name)
        release.dequeue
        message
      })
      ConnectRPC::Async::Host.new(server: target).start
    end

    # The raw peer has no deadline of its own and never answers, so the
    # reset it records can only be the client's.
    it "fails at the deadline, resets the stream and leaves no task under the caller" do
      entered = Thread::Queue.new
      peer = holding_peer(entered)
      origin = "http://127.0.0.1:#{peer.peer.port}"
      with_boundaries do |boundaries|
        reactor do |task|
          children = task.children.to_a
          error = capture_error { client(per_call, origin).echo(AsyncHelpers::MESSAGE.new(name: "late"), timeout: 0.3) }
          expect([error.class, error.code]).to eq([ConnectRPC::Error, "deadline_exceeded"])
          expect(task.children.to_a).to eq(children)
        end
        expect(pop_within(entered, "the request at the peer")).to eq("/example.v1.EchoService/Echo")
        expect_released(boundaries.take.first)
        expect_cancel_reset(peer)
      end
    ensure
      peer&.stop
    end

    it "keeps the reactor serving a sibling while a call waits on its handler" do
      with_boundaries do |boundaries|
        reactor do |task|
          entered = Async::Queue.new
          release = Async::Queue.new
          host = gated_reactor_host(entered, release)
          origin = "http://127.0.0.1:#{host.port}"
          call = task.async { client(per_call, origin).echo(AsyncHelpers::MESSAGE.new(name: "slow")).message.name }
          entered.dequeue
          ticks = Async::Queue.new
          sibling = task.async { 5.times { |index| ticks.enqueue(index) } }
          expect(5.times.map { ticks.dequeue }).to eq([0, 1, 2, 3, 4])
          sibling.wait
          expect(call).not_to be_finished
          release.enqueue(true)
          expect(call.wait).to eq("slow")
        ensure
          host&.close(grace: 0)
        end
        expect_released(boundaries.take.first)
      end
    end

    it "ends a cancelled caller task after teardown and resets the stream" do
      entered = Thread::Queue.new
      peer = holding_peer(entered)
      origin = "http://127.0.0.1:#{peer.peer.port}"
      with_boundaries do |boundaries|
        reactor do |task|
          victim = task.async(finished: false) { client(per_call, origin).echo(AsyncHelpers::MESSAGE.new(name: "v")) }
          expect(entered.pop(timeout: 5)).to eq("/example.v1.EchoService/Echo")
          victim.cancel
          expect(victim.wait).to be_nil
          expect(victim).to be_cancelled
        end
        expect_released(boundaries.take.first)
        expect_cancel_reset(peer)
      end
    ensure
      peer&.stop
    end

    it "re-raises an enclosing with_timeout unchanged and resets the stream" do
      entered = Thread::Queue.new
      peer = holding_peer(entered)
      origin = "http://127.0.0.1:#{peer.peer.port}"
      with_boundaries do |boundaries|
        error = reactor do |task|
          typed = client(per_call, origin)
          # The handler is entered long before the 0.5 s timeout fires.
          capture_error { task.with_timeout(0.5) { typed.echo(AsyncHelpers::MESSAGE.new(name: "wt")) } }
        end
        expect(error.class).to eq(Async::TimeoutError)
        expect(entered.pop(timeout: 5)).to eq("/example.v1.EchoService/Echo")
        expect_released(boundaries.take.first)
        expect_cancel_reset(peer)
      end
    ensure
      peer&.stop
    end

    # The closer is held until the caller's own with_timeout, raising its own
    # StandardError class, has fired into the caller's wait for the close.
    it "re-raises an enclosing with_timeout with a custom error class that fires during the close" do
      peer = threaded_recording_peer
      origin = "http://127.0.0.1:#{peer.peer.port}"
      closing = false
      fired = nil
      custom = Class.new(StandardError) do
        define_method(:initialize) do |*arguments|
          super(*arguments)
          fired.resolve(closing)
        end
      end
      hold = TracePoint.new(:call) do
        closing = true
        fired.wait
      end
      hold.enable(target: ConnectRPC::Async::Transport.instance_method(:close_owned))
      with_boundaries do |boundaries|
        error, during_close = reactor do |task|
          fired = Async::Promise.new
          # The response arrives long before the 0.5 s timeout fires.
          [capture_error { task.with_timeout(0.5, custom) { per_call.call(request("#{origin}/custom")) } }, fired.wait]
        end
        expect(during_close).to be(true)
        expect(error).to be_a(custom).and(have_attributes(message: "execution expired"))
        expect_released(boundaries.take.first)
      end
    ensure
      hold&.disable
      peer&.stop
    end

    it "unwinds a cancellation during connection setup without leaving tasks or its socket" do
      peer = setup_holding_peer
      origin = "http://127.0.0.1:#{peer.peer.port}"
      with_boundaries do |boundaries|
        reactor do |task|
          victim = task.async(finished: false) { per_call.call(request("#{origin}/setup")) }
          wait_until(task, "the peer accepted the connection under setup") { peer.peer.accepted == 1 }
          victim.cancel
          expect(victim.wait).to be_nil
          expect(task.children.to_a).to eq([])
        end
        expect_released(boundaries.take.first)
      end
    ensure
      peer&.stop
    end

    # An interrupt landing inside construction, after the lifetime guard
    # started but before its watcher, still ends every task the call created.
    it "unwinds an interrupt during construction" do
      interrupt = Class.new(StandardError)
      method = ConnectRPC::Async::Transport.instance_method(:install_owner_guard)
      file, first = method.source_location
      watcher = first + File.readlines(file).drop(first - 1).index do |line|
        line.strip == "@owner_watcher = @owner.async do |watcher|"
      end
      pause = TracePoint.new(:line) do |point|
        raise interrupt, "during construction" if point.lineno == watcher
      end
      pause.enable(target: method)
      with_boundaries do |boundaries|
        error = reactor do |task|
          children = task.children.to_a
          error = capture_error { per_call.call(request("http://127.0.0.1:1/construct")) }
          expect(task.children.to_a).to eq(children)
          error
        end
        expect([error.class, error.message]).to eq([interrupt, "during construction"])
        observation = boundaries.take.first
        expect(observation.values_at(:scope_finished, :attached, :live, :sockets)).to eq([true, false, [], []])
      end
    ensure
      pause&.disable
    end

    # A second interrupt lands as the unwinding call starts cancelling its
    # scope, so the guard left without a watcher is not yet cancelled. The
    # join still cancels it and finishes, then raises the later interrupt.
    it "finishes the scope when a second interrupt lands while a cut-short construction unwinds" do
      first = Class.new(StandardError)
      second = Class.new(StandardError)
      method = ConnectRPC::Async::Transport.instance_method(:install_owner_guard)
      file, start = method.source_location
      watcher = start + File.readlines(file).drop(start - 1).index do |line|
        line.strip == "@owner_watcher = @owner.async do |watcher|"
      end
      caller_task = nil
      raised = false
      pause = TracePoint.new(:line) { |point| raise first, "during construction" if point.lineno == watcher }
      # Only the cancel of the call's scope, the caller task's one child.
      again = TracePoint.new(:call) do |point|
        next if raised || !point.binding.local_variable_get(:task)&.parent.equal?(caller_task)

        raised = true
        raise second, "while unwinding"
      end
      pause.enable(target: method)
      again.enable(target: AsyncHelpers::INTERNAL.method(:cancel))
      with_boundaries do |boundaries|
        error = reactor do |task|
          caller_task = task
          children = task.children.to_a
          error = capture_error { per_call.call(request("http://127.0.0.1:1/construct")) }
          expect(task.children.to_a).to eq(children)
          error
        end
        expect([raised, error.class, error.message, error.cause&.class]).to eq([true, second, "while unwinding", first])
        observation = boundaries.take.first
        expect(observation.values_at(:scope_finished, :attached, :live, :sockets)).to eq([true, false, [], []])
      end
    ensure
      again&.disable
      pause&.disable
    end

    # The stream block fails and the transport's stream cleanup task is held
    # as it starts, so the caller's task is cancelled while it waits for that
    # cleanup. Core keeps the block's own exception over the cancellation.
    it "finishes teardown when cancelled during stream cleanup" do
      started = nil
      gate = nil
      victim = nil
      returned = false
      cancels = []
      trace = hold_stream_cleanup(-> { started.resolve(true) }, -> { gate.wait })
      cleaned = TracePoint.new(:return) { returned = true }
      cleaned.enable(target: ConnectRPC::Async::Transport.instance_method(:cleanup))
      delivered = TracePoint.new(:raise) do |point|
        cancels << point.raised_exception if Async::Task.current?.equal?(victim)
      end
      application = Class.new(StandardError)
      with_boundaries do |boundaries|
        reactor do |task|
          host = ConnectRPC::Async::Host.new(server: mixed_server(watch: lambda { |message, writer, _|
            loop { writer.send_message(message) }
          })).start
          origin = "http://127.0.0.1:#{host.port}"
          before = task.children.to_a.reject(&:finished?)
          started = Async::Promise.new
          gate = Async::Promise.new
          victim = task.async(finished: false) do
            mixed_client(per_call, origin).watch(item("s")) do |reader|
              reader.receive
              raise application, "from the block"
            end
          end
          started.wait
          expect(returned).to be(false)
          delivered.enable
          victim.cancel
          gate.resolve(true)
          expect(capture_error { victim.wait }).to be_a(application)
          delivered.disable
          # The cancellation reached the caller's task inside the held cleanup.
          expect(cancels.map(&:class)).to include(Async::Cancel)
          expect(task.children.to_a.reject(&:finished?) - before).to eq([])
        ensure
          gate&.resolve(true)
          host&.close(grace: 0)
        end
        expect_released(boundaries.take.first)
      end
    ensure
      [trace, cleaned, delivered].each { |point| point&.disable }
    end

    [1, 2].each do |count|
      it "retains #{count} cancellation(s) landing in the close until every task has finished" do
        peer = threaded_recording_peer
        origin = "http://127.0.0.1:#{peer.peer.port}"
        closing = nil
        trace = TracePoint.new(:call) { closing&.resolve(true) }
        trace.enable(target: ConnectRPC::Async::Transport.instance_method(:close_owned))
        with_boundaries do |boundaries|
          reactor do |task|
            closing = Async::Promise.new
            victim = task.async(finished: false) { per_call.call(request("#{origin}/closing")) }
            closing.wait
            count.times { victim.cancel }
            expect(victim.wait).to be_nil
            expect(victim).to be_cancelled
            expect(task.children.to_a.reject(&:finished?)).to eq([])
          end
          expect_released(boundaries.take.first)
          # The peer records the GOAWAY on its own reader, which can trail the call's return.
          eventually("the peer to record the GOAWAY") { peer.peer.goaways.any? }
          expect(peer.peer.goaways.map { |_, _, code| code }).to eq([Protocol::HTTP2::Error::NO_ERROR])
        end
      ensure
        trace&.disable
        peer&.stop
      end
    end
  end
end
