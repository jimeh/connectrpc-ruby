# frozen_string_literal: true

module InterruptErrors
  class Interrupted < RuntimeError; end

  # Rack::Timeout::RequestTimeoutException's shape: not a StandardError.
  class HardInterrupt < Exception; end # rubocop:disable Lint/InheritException
end

RSpec.describe "Interrupts" do
  # Runs `work` in a thread, waits until it is parked in a native wait after
  # `ready`, interrupts it, and reports what landed.
  def interrupt(how, ready:, frame: "Native::")
    landed = Queue.new
    ensured = Queue.new
    thread = Thread.new do
      Thread.current.report_on_exception = false
      yield
      landed << :returned
    rescue InterruptErrors::Interrupted, InterruptErrors::HardInterrupt, ConnectRPC::Error => e
      landed << e
    ensure
      ensured << true
    end
    ready.call
    wait_parked(thread, frame)
    sent = monotonic
    case how
    when :raise then thread.raise(InterruptErrors::Interrupted, "rack-timeout")
    when :raise_exception then thread.raise(InterruptErrors::HardInterrupt, "hard")
    when :kill then thread.kill
    end
    expect(thread.join(5)).not_to be_nil
    elapsed = monotonic - sent
    outcome = landed.empty? ? :killed : landed.pop
    expect(ensured.pop(timeout: 1)).to be(true)
    expect(elapsed).to be < 1
    outcome
  end

  def expect_landed(how, outcome)
    case how
    when :raise then expect([outcome.class, outcome.message]).to eq([InterruptErrors::Interrupted, "rack-timeout"])
    when :raise_exception then expect([outcome.class, outcome.message]).to eq([InterruptErrors::HardInterrupt, "hard"])
    when :kill then expect(outcome).to eq(:killed)
    end
  end

  %i[pooled per_call].each do |mode|
    %i[raise raise_exception kill].each do |how|
      context "#{how} (#{mode})" do
        it "lands while connecting to a full accept queue", :linux_only do
          port, cleanup = full_backlog
          transport = hyper(connections: mode)
          outcome = interrupt(how, ready: -> { wait_until("SYN_SENT") { syn_sent?(port) } }) do
            client("http://127.0.0.1:#{port}", transport: transport).unary(MSG.new, timeout: 30)
          end
          expect_landed(how, outcome)
          expect(client(peer["grpc_h2c"], transport: transport).unary(MSG.new(value: "after"), timeout: 5)
                   .message.value).to eq("after")
        ensure
          cleanup&.call
        end

        # A per-call dial is the call's, so its socket closes when the call
        # ends. A pooled dial is the pool's: it carries on after the caller
        # leaves, still owned by the transport, until connect_timeout or close.
        it "lands during the TLS handshake with a silent server and closes the socket" do
          silent = TCPServer.new("127.0.0.1", 0)
          accepted = Queue.new
          acceptor = Thread.new { loop { accepted << silent.accept } }
          transport = hyper(connections: mode, **tls_options)
          socket = nil
          # The ClientHello arrived, so the client now waits in the handshake.
          hello_sent = lambda do
            socket = accepted.pop(timeout: 5) || raise("no TLS accept")
            socket.wait_readable(5) || raise("no ClientHello")
          end
          outcome = interrupt(how, ready: hello_sent) do
            client("https://127.0.0.1:#{silent.addr[1]}", transport: transport).unary(MSG.new, timeout: 30)
          end
          expect_landed(how, outcome)
          expect(socket.read_nonblock(65_536, exception: false)).to be_a(String)
          if mode == :pooled
            expect(stats(transport)[1..2]).to eq([0, 1])
            # Close ends the dial, whose task may stop before the grace check.
            transport.close(grace: 0)
          end
          expect(socket.wait_readable(5)).not_to be_nil
          expect(socket.read_nonblock(65_536, exception: false)).to be_nil
        ensure
          acceptor&.kill
          silent&.close
        end

        it "lands in a blocked write, resets the stream and keeps the connection" do
          from = peer.mark
          transport = hyper(connections: mode)
          peer_client = client(peer["grpc_h2c"], transport: transport)
          outcome = interrupt(how, ready: -> { peer.wait_event(/grpc cs-noread-start/, from: from) },
                                   frame: "Exchange#write") do
            peer_client.client_stream(metadata: { "x-mode" => "noread" }, timeout: 30) do |writer|
              loop { writer.send_message(MSG.new(payload: "x".b * 65_536)) }
            end
          end
          expect_landed(how, outcome)
          peer.wait_event(/grpc cs-noread-done context canceled/, from: from)
          expect(peer_client.unary(MSG.new(value: "after"), timeout: 5).message.value).to eq("after")
          expect(peer.count(/\Agrpc accept/, from: from)).to eq(mode == :pooled ? 1 : 2)
        end

        it "lands in a blocked read, resets the stream and keeps the connection" do
          from = peer.mark
          transport = hyper(connections: mode)
          peer_client = client(peer["grpc_h2c"], transport: transport)
          outcome = interrupt(how, ready: -> { peer.wait_event(/grpc ss-hold-sent/, from: from) },
                                   frame: "Exchange#read") do
            peer_client.server_stream(MSG.new(value: "hold"), timeout: 30) { |reader| values(reader) }
          end
          expect_landed(how, outcome)
          peer.wait_event(/grpc ss-hold-done context canceled/, from: from)
          expect(peer_client.unary(MSG.new(value: "after"), timeout: 5).message.value).to eq("after")
          expect(peer.count(/\Agrpc accept/, from: from)).to eq(mode == :pooled ? 1 : 2)
        end
      end
    end
  end

  it "lands during name resolution, for every interrupt and connection mode", :dns_shim do
    environment = stall_environment
    output, status = ruby_child(<<~RUBY, env: environment, timeout: 120)
      Interrupted = Class.new(RuntimeError)
      HardInterrupt = Class.new(Exception)
      events = File.open(ENV.fetch("STALL_EVENTS"), "r+")
      results = []
      %i[pooled per_call].product(%i[raise raise_exception kill]).each do |mode, how|
        transport = ConnectRPC::Hyper::Transport.new(connections: mode)
        landed = nil
        thread = Thread.new do
          Thread.current.report_on_exception = false
          client("http://peer.stall:1", transport).unary(MSG.new, timeout: 30)
        rescue Exception => e
          landed = e.class.name
        end
        events.gets
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10
        until thread.status == "sleep" && thread.backtrace.to_a.first.to_s.include?("Native::")
          raise "never parked" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
          Thread.pass
        end
        sent = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        case how
        when :raise then thread.raise(Interrupted, "x")
        when :raise_exception then thread.raise(HardInterrupt, "x")
        else thread.kill
        end
        thread.join(5) || raise("did not exit")
        elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - sent
        File.write(ENV.fetch("STALL_RELEASE"), "x")
        events.gets
        after = client(#{peer['grpc_h2c'].dump}, transport).unary(MSG.new(value: "after"), timeout: 5).message.value
        results << [mode, how, landed || "killed", elapsed < 1, after]
        transport.close
      end
      p results
    RUBY
    expect(status).to be_success
    expect(eval(output.lines.last)).to eq( # rubocop:disable Security/Eval
      %i[pooled per_call].product(%i[raise raise_exception kill]).map do |mode, how|
        [mode, how, { raise: "Interrupted", raise_exception: "HardInterrupt", kill: "killed" }.fetch(how), true,
         "after"]
      end
    )
  end

  it "never waits for parked resolvers: cancellation, another lookup, GC and exit", :dns_shim do
    environment = stall_environment
    output, status = ruby_child(<<~RUBY, env: environment, timeout: 60)
      events = File.open(ENV.fetch("STALL_EVENTS"), "r+")
      transport = ConnectRPC::Hyper::Transport.new
      sources = Array.new(4) { ConnectRPC::CancellationSource.new }
      calls = sources.each_with_index.map do |source, index|
        Thread.new do
          client("http://peer\#{index}.stall:1", transport).unary(MSG.new, timeout: 30, cancellation: source.token)
        rescue ConnectRPC::Error => e
          e.code
        end
      end
      4.times { events.gets }
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      sources.each(&:cancel)
      codes = calls.map(&:value)
      canceled_in = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
      # Another origin resolves through getaddrinfo while four lookups stay parked.
      port = URI(#{peer['grpc_h2c'].dump}).port
      other = client("http://localhost:\#{port}", transport).unary(MSG.new(value: "other"), timeout: 5).message.value
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      transport = nil
      GC.start
      gc = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
      p [codes.uniq, canceled_in < 1, other, gc < 1]
    RUBY
    expect(status).to be_success
    expect(output.lines.last.chomp).to eq('[["canceled"], true, "other", true]')
  end

  describe "a wake without a pending interrupt" do
    # Each message is larger than the peer's static 64 KiB stream window plus
    # the 1 MiB that h2 buffers per stream, so hyper takes the first at once
    # and the second write blocks from its first poll until the peer reads.
    # With smaller messages a write can still be waiting for hyper to buffer
    # it, and with grpc-go's default BDP estimator for a window update: either
    # finishes the write before the wake lands.
    def messages = 3
    def payload = 2 << 20

    # One wait body has run since `entered` began and is still running after
    # a peer round trip: the write is blocked on flow control, with its
    # unblocking function registered, not between two native waits.
    def blocked_write(thread)
      wait_until("a blocked write") do
        entered, running = native.wait_bodies
        next false unless running == 1 && thread.status == "sleep"

        peer.sync
        native.wait_bodies == [entered, 1] && entered
      end
    end

    [
      ["Thread#wakeup", :wakeup.to_proc],
      ["a masked Thread#raise", ->(thread) { thread.raise(InterruptErrors::Interrupted, "masked") }]
    ].each do |name, wake|
      it "resumes a blocked write after #{name} without duplicating a message" do
        from = peer.mark
        tag = "spurious-#{name.gsub(/\W/, '')}-#{rand(1 << 30)}"
        outcome = Queue.new
        writer_thread = Thread.new do
          Thread.current.report_on_exception = false
          client(peer["grpc_static"], transport: hyper).client_stream(metadata: { "x-mode" => "gate:#{tag}" },
                                                                      timeout: 30) do |writer|
            Thread.handle_interrupt(Object => :never) do
              messages.times { |index| writer.send_message(MSG.new(value: index.to_s, payload: "x".b * payload)) }
            end
          end
          outcome << :completed
        rescue InterruptErrors::Interrupted => e
          outcome << e.message
        end
        # The peer reads nothing until released, so the writes block.
        peer.wait_event(/static cs-gate-start #{tag}/, from: from)
        entered = blocked_write(writer_thread)
        resumed = native.resumed_waits
        wake.call(writer_thread)
        wait_until("the blocked write to resume its wait") { native.resumed_waits > resumed }
        # The woken write re-entered its wait and still blocks, all before
        # the peer read anything.
        expect(blocked_write(writer_thread)).to be > entered
        expect(peer.since(from).grep(/static cs-recv/)).to be_empty
        peer.command("release #{tag}")
        result = outcome.pop(timeout: 20)
        peer.wait_event(/static cs-(eof|error)/, from: from)
        received = peer.since(from).grep(/static cs-recv \d+ #{payload}\z/).map { |event| event.split[2] }
        if name == "Thread#wakeup"
          expect(received).to eq(Array.new(messages, &:to_s))
          expect(result).to eq(:completed)
        else
          # The deferred raise aborts the stream once the mask ends, and its
          # RST_STREAM may overtake buffered DATA: the peer sees a prefix.
          expect(received).to eq(Array.new(received.size, &:to_s))
          expect(result).to eq("masked")
        end
      end
    end
  end
end
