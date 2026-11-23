# frozen_string_literal: true

require "spec_helper"
require_relative "support/accept_faults"

RSpec.describe ConnectRPC::Async::Host do
  %i[h2c tls mtls].each do |mode|
    it "recovers one accept failure preserving held, same-connection and fresh generated RPCs over #{mode}" do
      reactor do |task|
        with_ca do |ca_file|
          injected = Async::Queue.new
          release_fault = Async::Queue.new
          entered = Async::Queue.new
          release_rpc = Async::Queue.new
          faults = AcceptFaults.new do |_index, attempt|
            if attempt == 2
              release_fault.dequeue
              injected.enqueue(true)
              raise Errno::EMFILE, "controlled accept fault"
            end
          end
          tls = host_tls(anchors: mode == :mtls ? [tls_material.fetch(:client_ca)] : nil) unless mode == :h2c
          target = server(echo: lambda do |message, _context|
            if message.name == "held"
              entered.enqueue(true)
              release_rpc.dequeue
            end
            message
          end)
          host = described_class.new(server: target, tls: tls, idle_timeout: nil)
          options = mode == :h2c ? {} : { ca_file: ca_file }
          if mode == :mtls
            identity = tls_material.fetch(:identity)
            options.merge!(certificate_chain: [identity.first.to_pem], private_key: identity.last.private_to_pem)
          end
          faults.enable do
            host.start
            origin = "#{mode == :h2c ? 'http' : 'https'}://127.0.0.1:#{host.port}"
            transport = ConnectRPC::Async::Transport.new(origin: origin, **options)
            fresh = ConnectRPC::Async::Transport.new(origin: origin, **options)
            first = client(transport, origin)
            expect(first.echo(AsyncHelpers::MESSAGE.new(name: "baseline")).message.name).to eq("baseline")
            resource = transport.instance_variable_get(:@resources).first
            held = task.async { first.echo(AsyncHelpers::MESSAGE.new(name: "held")) }
            entered.dequeue
            release_fault.enqueue(true)
            injected.dequeue
            release_rpc.enqueue(true)
            expect(held.wait.message.name).to eq("held")
            expect(first.echo(AsyncHelpers::MESSAGE.new(name: "later")).message.name).to eq("later")
            expect(transport.instance_variable_get(:@resources).first).to equal(resource)
            expect(client(fresh, origin).echo(AsyncHelpers::MESSAGE.new(name: "fresh"),
                                              timeout: 1).message.name).to eq("fresh")
          ensure
            release_rpc.enqueue(true)
            transport&.close(grace: 0)
            fresh&.close(grace: 0)
            host.close(grace: 0)
          end
        end
      end
    end
  end

  def controlled_backoff(host)
    entered = Async::Queue.new
    release = Async::Queue.new
    host.define_singleton_method(:accept_backoff) do |delay|
      entered.enqueue(delay)
      release.dequeue
    end
    [entered, release]
  end

  def two_listeners(host)
    bounds = 2.times.map { IO::Endpoint.tcp("127.0.0.1", 0).bound }
    bound = Struct.new(:sockets, :bounds).new(bounds.flat_map(&:sockets), bounds)
    host.define_singleton_method(:bind) { bound }
    bound.sockets
  end

  # Hold cleanup despite repeat cancellation so both runtimes exercise the
  # same interrupted-abort boundary. The test explicitly releases this gate.
  def await_cleanup_release(release)
    release.dequeue
  rescue Async::Cancel
    retry
  end

  def wait_error(host)
    host.wait
  rescue StandardError => e
    e
  end

  def backoff_delays(count) = Array.new(count) { |index| [0.005 * (2**index), 1.0].min }

  def accept_warnings(errno, delays)
    delays.map { |delay| "ConnectRPC accept error: #{errno}; retrying in #{delay}s\n" }.join
  end

  [Errno::EMFILE, Errno::ENFILE, Errno::ENOBUFS, Errno::ENOMEM].each do |errno|
    it "retries 20 consecutive #{errno} accept failures with capped backoff while serving established peers" do
      reactor do |task|
        host = described_class.new(server: server, idle_timeout: nil)
        entered, release = controlled_backoff(host)
        # The first attempt accepts the established peer; the next 20 fail.
        faults = AcceptFaults.new { |_index, attempt| raise errno, "accept #{attempt}" if attempt.between?(2, 21) }
        delays = backoff_delays(20)
        expect(delays.last(12)).to all(eq(1.0))
        expect do
          faults.enable do
            host.start
            origin = "http://127.0.0.1:#{host.port}"
            established = ConnectRPC::Async::Transport.new(origin: origin)
            typed = client(established, origin)
            expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "before")).message.name).to eq("before")
            fresh = ConnectRPC::Async::Transport.new(origin: origin)
            later = task.async { client(fresh, origin).echo(AsyncHelpers::MESSAGE.new(name: "later")) }
            delays.each do |delay|
              expect(entered.dequeue).to eq(delay)
              expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "during")).message.name).to eq("during")
              release.enqueue(true)
            end
            expect(later.wait.message.name).to eq("later")
            expect(faults.attempts[0]).to be >= 22
            expect(faults.accepted[0]).to eq(2)
            expect(faults.tasks).to all(satisfy { |listener| !listener.finished? })
          ensure
            established&.close(grace: 0)
            fresh&.close(grace: 0)
            host.close(grace: 0)
          end
        end.to output(accept_warnings(errno, delays)).to_stderr
        expect(host.wait).to be_nil
      end
    end
  end

  [Errno::ECONNABORTED, Errno::EINTR].each do |errno|
    it "retries sustained #{errno} accept failures without backoff, yielding to established peers and close" do
      reactor do |task|
        host = described_class.new(server: server, idle_timeout: nil)
        entered, = controlled_backoff(host)
        # Without a scheduling boundary per retry, the test task could only
        # resume once the listener stopped failing at this bound.
        limit = 100_000
        faults = AcceptFaults.new do |_index, attempt|
          raise errno, "immediate" if attempt > 1 && attempt < limit
        end
        faults.enable do
          host.start
          origin = "http://127.0.0.1:#{host.port}"
          transport = ConnectRPC::Async::Transport.new(origin: origin)
          typed = client(transport, origin)
          expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "before")).message.name).to eq("before")
          wait_until(task, "sustained #{errno} retries", within: 5) do
            faults.attempts[0] >= 1_000 || !entered.empty?
          end
          expect(entered).to be_empty
          expect(faults.attempts[0]).to be < limit
          expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "during")).message.name).to eq("during")
          transport.close(grace: 0)
          host.close(grace: 0)
          expect(host.wait).to be_nil
          expect(faults.tasks).to all(be_finished)
          expect(faults.attempts[0]).to be < limit
          expect(entered).to be_empty
        ensure
          transport&.close(grace: 0)
          host.close(grace: 0)
        end
      end
    end
  end

  [Errno::EBADF, Errno::EINVAL, Errno::ENOTSOCK, Errno::ENETDOWN, IOError, RuntimeError].each do |error_class|
    it "raises the identical first #{error_class} cause from startup after joining listeners" do
      reactor do
        host = described_class.new(server: server)
        sockets = two_listeners(host)
        original = error_class.new("first accept")
        faults = AcceptFaults.new { raise original }
        faults.enable do
          expect { host.start }.to(raise_error { |error| expect(error).to equal(original) })
          expect(faults.attempts.values).to eq([1])
          expect(sockets).to all(be_closed)
          expect(faults.tasks).to all(be_finished)
          expect(wait_error(host)).to equal(original)
          expect { host.start }.to raise_error(AsyncHelpers::LIFECYCLE_ERROR)
        ensure
          host.close(grace: 0)
        end
      end
    end
  end

  # The session aborts fully, then raises, as a failed retirement does.
  def failing_abort(host, failure)
    allow(host_session(host)).to receive(:abort).and_wrap_original do |abort|
      abort.call
      raise failure
    end
  end

  it "resolves completion when the session abort fails after a terminal cause in startup" do
    reactor do
      host = described_class.new(server: server)
      sockets = two_listeners(host)
      failure = RuntimeError.new("session abort failed")
      failing_abort(host, failure)
      faults = AcceptFaults.new { |index, _attempt| raise IOError, "first accept" if index.zero? }
      faults.enable do
        expect { host.start }.to(raise_error { |error| expect(error).to equal(failure) })
        expect(sockets).to all(be_closed)
        expect(faults.tasks).to all(be_finished)
        expect(host.instance_variable_get(:@completion)).to be_resolved
        expect(wait_error(host)).to equal(failure)
      ensure
        host.close(grace: 0)
      end
    end
  end

  # Fails listener zero after start while a waiter is already blocked, and
  # returns that waiter once the exit task has finished.
  def terminal_after_start(task, host)
    trigger = Async::Queue.new
    faults = AcceptFaults.new do |index, _attempt|
      next unless index.zero?

      trigger.dequeue
      raise IOError, "terminal after start"
    end
    faults.enable do
      host.start
      waiter = task.async { wait_error(host) }
      trigger.enqueue(true)
      wait_until(task, "the exit task to finish") { host.instance_variable_get(:@exit_task)&.finished? }
      yield waiter, faults
    end
  end

  it "wakes a blocked wait when the session abort fails after a terminal cause" do
    reactor do |task|
      host = described_class.new(server: server)
      sockets = two_listeners(host)
      failure = RuntimeError.new("session abort failed")
      failing_abort(host, failure)
      terminal_after_start(task, host) do |waiter, faults|
        expect(host.instance_variable_get(:@completion)).to be_resolved
        expect(sockets).to all(be_closed)
        expect(faults.tasks).to all(be_finished)
        expect(waiter.wait).to equal(failure)
      end
    ensure
      host&.close(grace: 0)
    end
  end

  it "closes the other listeners and wakes a blocked wait when one listener fails to close" do
    reactor do |task|
      host = described_class.new(server: server)
      sockets = two_listeners(host)
      failure = Errno::EBADF.new("listener close failed")
      close = sockets.first.method(:close)
      sockets.first.define_singleton_method(:close) { raise failure }
      terminal_after_start(task, host) do |waiter, faults|
        expect(host.instance_variable_get(:@completion)).to be_resolved
        expect(sockets.last).to be_closed
        expect(faults.tasks).to all(be_finished)
        expect(waiter.wait).to equal(failure)
      end
    ensure
      close&.call
      host&.close(grace: 0)
    end
  end

  it "restarts the resource-error backoff after each successful raw accept" do
    reactor do |task|
      host = described_class.new(server: server, idle_timeout: nil)
      entered, release = controlled_backoff(host)
      # Each false attempt waits for and accepts the next peer.
      plan = [true, true, true, false, true, true, false]
      faults = AcceptFaults.new { raise Errno::EMFILE, "planned" if plan.shift }
      peers = []
      expect do
        faults.enable do
          host.start
          [[0.005, 0.01, 0.02], [0.005, 0.01]].each_with_index do |delays, index|
            peers << IO::Endpoint.tcp("127.0.0.1", host.port).connect
            delays.each do |delay|
              expect(entered.dequeue).to eq(delay)
              release.enqueue(true)
            end
            wait_until(task, "peer #{index} accepted") { faults.accepted[0] == index + 1 }
          end
          expect(plan).to be_empty
          expect(host_session(host).instance_variable_get(:@resources).size).to eq(2)
        ensure
          peers.each(&:close)
          host.close(grace: 0)
        end
      end.to output(accept_warnings(Errno::EMFILE, [0.005, 0.01, 0.02, 0.005, 0.01])).to_stderr
    end
  end

  it "keeps a second real listener serving while the first backs off without stopping the Host" do
    reactor do
      host = described_class.new(server: server)
      sockets = two_listeners(host)
      entered, release = controlled_backoff(host)
      faults = AcceptFaults.new { |index, _attempt| raise Errno::ENFILE, "listener zero" if index.zero? }
      delays = backoff_delays(10)
      expect do
        faults.enable do
          host.start
          origin = "http://127.0.0.1:#{sockets.last.local_address.ip_port}"
          delays.each_with_index do |delay, index|
            # Keep the failing listener in its last backoff until close.
            release.enqueue(true) if index.positive?
            expect(entered.dequeue).to eq(delay)
            transport = ConnectRPC::Async::Transport.new(origin: origin)
            expect(client(transport,
                          origin).echo(AsyncHelpers::MESSAGE.new(name: "healthy")).message.name).to eq("healthy")
            transport.close(grace: 0)
          end
          expect(faults.attempts[0]).to eq(delays.size)
          expect(faults.accepted[1]).to eq(10)
          expect(faults.tasks).to all(satisfy { |listener| !listener.finished? })
          host.close(grace: 0)
          expect(host.wait).to be_nil
          expect(sockets).to all(be_closed)
          expect(faults.tasks).to all(be_finished)
        ensure
          host.close(grace: 0)
        end
      end.to output(accept_warnings(Errno::ENFILE, delays)).to_stderr
    end
  end

  it "retires a stalled admitted RPC and raw peer before concurrent terminal waits observe the cause" do
    reactor do |task|
      entered = Async::Queue.new
      cancelled = Async::Queue.new
      trigger = Async::Queue.new
      target = server(echo: lambda do |message, _context|
        entered.enqueue(true)
        begin
          Async::Queue.new.dequeue
        ensure
          cancelled.enqueue(true)
        end
        message
      end)
      host = described_class.new(server: target, idle_timeout: nil)
      original = Errno::EBADF.new("terminal after admission")
      faults = AcceptFaults.new do |_index, attempt|
        if attempt == 2
          trigger.dequeue
          raise original
        end
      end
      faults.enable do
        host.start
        origin = "http://127.0.0.1:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        pending = task.async(finished: false) do
          client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "stalled"), timeout: nil)
        rescue ConnectRPC::Error => e
          e
        end
        entered.dequeue
        resources = host_session(host).instance_variable_get(:@resources).dup
        waiters = 2.times.map { task.async { wait_error(host) } }
        trigger.enqueue(true)
        expect(waiters.map(&:wait)).to all(equal(original))
        cancelled.dequeue
        expect(resources).to all(be_finished)
        expect(resources.map(&:raw)).to all(be_closed)
        expect(resources.flat_map { |r| [r.task, *r.tasks].compact }).to all(be_finished)
        expect(faults.tasks).to all(be_finished)
        expect(pending.wait).to be_a(ConnectRPC::Error)
        expect(host.close(grace: 5)).to be(false)
        expect(wait_error(host)).to equal(original)
      ensure
        transport&.close(grace: 0)
        host.close(grace: 0)
      end
    end
  end

  it "cancels a real cooperative backoff on close and preserves ordinary concurrent outcomes" do
    reactor do |task|
      host = described_class.new(server: server)
      backoff = Async::Queue.new
      original_backoff = host.method(:accept_backoff)
      host.define_singleton_method(:accept_backoff) do |delay|
        backoff.enqueue(delay)
        # A real sleep longer than the example, so only close can end it.
        original_backoff.call(60)
      end
      faults = AcceptFaults.new { raise Errno::EMFILE, "backoff" }
      expect do
        faults.enable do
          host.start
          expect(backoff.dequeue).to eq(0.005)
          waiters = 2.times.map { task.async { host.wait } }
          closers = 2.times.map { task.async { host.close(grace: 0) } }
          expect(closers.map(&:wait)).to eq([true, true])
          expect(waiters.map(&:wait)).to eq([nil, nil])
          expect(host.wait).to be_nil
          expect(faults.attempts[0]).to eq(1)
          expect(faults.tasks).to all(be_finished)
        ensure
          host.close(grace: 0)
        end
      end.to output(accept_warnings(Errno::EMFILE, [0.005])).to_stderr
    end
  end

  it "stops blocked real accept without making waiter cancellation own the Host" do
    reactor do |task|
      host = described_class.new(server: server)
      faults = AcceptFaults.new { |_index, _attempt| nil }
      faults.enable do
        host.start
        waiter = task.async { host.wait }
        AsyncHelpers::INTERNAL.cancel(waiter)
        expect(waiter).to be_finished
        expect(faults.listeners.first).not_to be_closed
        expect(faults.tasks.first).not_to be_finished
        origin = "http://127.0.0.1:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        expect(client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "live")).message.name).to eq("live")
        expect(host.close(grace: 0.5)).to be(true)
        expect(host.wait).to be_nil
        expect(faults.tasks).to all(be_finished)
      ensure
        transport&.close(grace: 0)
        host.close(grace: 0)
      end
    end
  end

  it "checks wait affinity before its promise, including after settlement" do
    reactor do
      host = described_class.new(server: server)
      expect { host.wait }.to raise_error(AsyncHelpers::LIFECYCLE_ERROR)
      foreign = Thread.new do
        host.wait
      rescue StandardError => e
        e
      end.value
      expect(foreign).to be_a(AsyncHelpers::LIFECYCLE_ERROR)
      other = Thread.new do
        Async do
          host.wait
        rescue StandardError => e
          e
        end.wait
      end.value
      expect(other).to be_a(AsyncHelpers::LIFECYCLE_ERROR)
      host.close
      expect(host.wait).to be_nil
      expect { host.start }.to raise_error(AsyncHelpers::LIFECYCLE_ERROR)
      expect(Thread.new { wait_error(host) }.value).to be_a(AsyncHelpers::LIFECYCLE_ERROR)
    end
  end

  it "preserves unrelated caller exceptions through ensure-close" do
    reactor do
      host = described_class.new(server: server).start
      original = RuntimeError.new("caller")
      expect do
        raise original
      ensure
        host.close
      end.to(raise_error { |error| expect(error).to equal(original) })
      expect(host.wait).to be_nil
    end
  end

  %i[accept backoff].each do |boundary|
    it "resolves ordinary waits and retires resources when the owner is cancelled during #{boundary}" do
      warnings = boundary == :backoff ? accept_warnings(Errno::EMFILE, [0.005]) : ""
      expect { owner_cancellation(boundary) }.to output(warnings).to_stderr
    end
  end

  def owner_cancellation(boundary)
    reactor do |task|
      ready = Async::Queue.new
      progress = Async::Queue.new
      faults = AcceptFaults.new do
        progress.enqueue(true) if boundary == :accept
        raise Errno::EMFILE, "owner backoff" if boundary == :backoff
      end
      owner = task.async(finished: false) do
        host = described_class.new(server: server, idle_timeout: nil)
        if boundary == :backoff
          real_backoff = host.method(:accept_backoff)
          host.define_singleton_method(:accept_backoff) do |_delay|
            progress.enqueue(true)
            # A real sleep longer than the example, so only cancellation ends it.
            real_backoff.call(60)
          end
        end
        faults.enable do
          host.start
          ready.enqueue(host)
          Async::Queue.new.dequeue
        end
      end
      host = ready.dequeue
      progress.dequeue
      waiter = task.async { host.wait }
      AsyncHelpers::INTERNAL.cancel(owner)
      owner.wait
      expect(waiter.wait).to be_nil
      expect(host.wait).to be_nil
      # Ordinary wait observes listener stop; owner.wait only observes its result.
      AsyncHelpers::INTERNAL.join_complete(owner)
      expect(owner).to be_finished
      expect(faults.listeners).to all(be_closed)
      expect(faults.tasks).to all(be_finished)
      expect(host_session(host).instance_variable_get(:@lifetime)).to be_finished
      expect(host_session(host).instance_variable_get(:@state)).to eq(:closed)
    end
  end

  it "anchors terminal cleanup to the construction owner after a short-lived start caller exits" do
    reactor do |task|
      trigger = Async::Queue.new
      original = IOError.new("after start caller")
      host = described_class.new(server: server)
      faults = AcceptFaults.new do
        trigger.dequeue
        raise original
      end
      faults.enable do
        starter = task.async { host.start }
        expect(starter.wait).to equal(host)
        expect(starter).to be_finished
        trigger.enqueue(true)
        expect(wait_error(host)).to equal(original)
        expect(faults.tasks).to all(be_finished)
        expect(host_session(host).instance_variable_get(:@state)).to eq(:closed)
      ensure
        host.close(grace: 0)
      end
    end
  end

  it "preserves the terminal cause when owner cancellation interrupts cooperative cleanup" do
    reactor do |task|
      ready = Async::Queue.new
      entered = Async::Queue.new
      cleaning = Async::Queue.new
      release_cleanup = Async::Queue.new
      trigger = Async::Queue.new
      original = IOError.new("terminal before owner cancellation")
      faults = AcceptFaults.new do |_index, attempt|
        if attempt == 2
          trigger.dequeue
          raise original
        end
      end
      owner = task.async(finished: false) do
        target = server(echo: lambda do |message, _context|
          entered.enqueue(true)
          begin
            Async::Queue.new.dequeue
          ensure
            cleaning.enqueue(Async::Task.current)
            await_cleanup_release(release_cleanup)
          end
          message
        end)
        host = described_class.new(server: target, idle_timeout: nil)
        faults.enable do
          host.start
          ready.enqueue(host)
          Async::Queue.new.dequeue
        end
      end
      host = ready.dequeue
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      pending = task.async(finished: false) do
        client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "held"), timeout: nil)
      rescue ConnectRPC::Error => e
        e
      end
      entered.dequeue
      resources = host_session(host).instance_variable_get(:@resources).dup
      waiter = task.async { wait_error(host) }
      trigger.enqueue(true)
      handler = cleaning.dequeue
      retained = resources.flat_map { |resource| resource.instance_variable_get(:@retirement_members).keys }
      retained.concat(faults.tasks)
      fibers = retained.filter_map(&:fiber)
      expect(handler).not_to be_finished
      expect(resources).to all(satisfy { |resource| resource.send(:raw_failed?) })
      AsyncHelpers::INTERNAL.cancel(owner)
      expect(owner).to be_cancelled
      expect(waiter).not_to be_finished
      expect(host.instance_variable_get(:@completion)).not_to be_resolved
      expect(handler).not_to be_finished
      expect(resources).to all(satisfy { |resource| !resource.send(:retirement_completed?) })
      expect(host_session(host).instance_variable_get(:@state)).to eq(:closing)
      release_cleanup.enqueue(true)
      expect(waiter.wait).to equal(original)
      expect(wait_error(host)).to equal(original)
      expect(handler).to be_finished
      expect(retained).to include(handler)
      expect(retained).to all(be_finished)
      expect(fibers).not_to be_empty
      expect(fibers).to all(satisfy { |fiber| !fiber.alive? })
      expect(resources).to all(satisfy { |resource| resource.send(:retirement_completed?) })
      expect(host_session(host).instance_variable_get(:@state)).to eq(:closed)
      expect(pending.wait).to be_a(ConnectRPC::Error)
    ensure
      release_cleanup&.enqueue(true)
      transport&.close(grace: 0)
      AsyncHelpers::INTERNAL.cancel(owner)
    end
  end

  it "closes every bound listener and collected task after partial startup scheduling failure" do
    reactor do
      host = described_class.new(server: server)
      sockets = two_listeners(host)
      original = RuntimeError.new("second listener scheduling")
      calls = 0
      allow(host_session(host)).to receive(:schedule).and_wrap_original do |schedule, &block|
        calls += 1
        raise original if calls == 2

        schedule.call(&block)
      end
      faults = AcceptFaults.new { |_index, _attempt| nil }
      faults.enable do
        expect { host.start }.to(raise_error { |error| expect(error).to equal(original) })
        expect(sockets).to all(be_closed)
        expect(faults.tasks.length).to eq(1)
        expect(faults.tasks).to all(be_finished)
        expect(host.wait).to be_nil
      end
    end
  end

  it "prefers the recorded first listener cause over a secondary startup scheduling error" do
    reactor do
      host = described_class.new(server: server)
      sockets = two_listeners(host)
      original = IOError.new("first listener cause")
      secondary = RuntimeError.new("schedule return failure")
      allow(host_session(host)).to receive(:schedule).and_wrap_original do |schedule, &block|
        schedule.call(&block)
        raise secondary
      end
      faults = AcceptFaults.new { raise original }
      faults.enable do
        expect { host.start }.to(raise_error { |error| expect(error).to equal(original) })
        expect(sockets).to all(be_closed)
        expect(faults.tasks).to all(be_finished)
        expect(wait_error(host)).to equal(original)
      end
    end
  end

  it "reports bind and initial TLS setup errors synchronously and returns nil from wait" do
    reactor do
      bind_error = IOError.new("bind failed")
      host = described_class.new(server: server)
      host.define_singleton_method(:bind) { raise bind_error }
      expect { host.start }.to(raise_error { |error| expect(error).to equal(bind_error) })
      expect(host.wait).to be_nil
      tls_error = RuntimeError.new("initial provider failed")
      tls_host = described_class.new(server: server, tls_provider: -> { raise tls_error })
      expect { tls_host.start }.to raise_error(ConnectRPC::Async::TLSConfiguration::Error)
      expect(tls_host.wait).to be_nil
    end
  end

  it "does not retry a selected errno raised after raw accept" do
    reactor do |task|
      host = described_class.new(server: server)
      original = Errno::EINTR.new("session admission boundary")
      # Admission is outside the accept rescue. Force its exceptional boundary,
      # while retaining the real accept/session and closing the handed-off raw IO.
      allow(host_session(host)).to receive(:accept).and_wrap_original do |accept, raw|
        accept.call(raw)
        raise original
      end
      faults = AcceptFaults.new { |_index, _attempt| nil }
      faults.enable do
        host.start
        peer = IO::Endpoint.tcp("127.0.0.1", host.port).connect
        expect(wait_error(host)).to equal(original)
        expect(faults.attempts[0]).to eq(1)
        expect(peer.read(1)).to be_nil
        expect(faults.tasks).to all(be_finished)
        task.yield
      ensure
        peer&.close
        host.close(grace: 0)
      end
    end
  end

  it "keeps retrying when the accept warning cannot be written" do
    reactor do
      host = described_class.new(server: server)
      entered, release = controlled_backoff(host)
      faults = AcceptFaults.new { |_index, attempt| raise Errno::EMFILE, "attempt #{attempt}" if attempt <= 2 }
      warnings = []
      allow(host_session(host)).to receive(:warn) do |message|
        warnings << message
        raise Errno::EPIPE, "stderr closed"
      end
      faults.enable do
        host.start
        [0.005, 0.01].each do |delay|
          expect(entered.dequeue).to eq(delay)
          release.enqueue(true)
        end
        origin = "http://127.0.0.1:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        expect(client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "after")).message.name).to eq("after")
        expect(warnings.length).to eq(2)
        expect(faults.tasks).to all(satisfy { |listener| !listener.finished? })
      ensure
        transport&.close(grace: 0)
        host.close(grace: 0)
      end
      expect(host.wait).to be_nil
    end
  end

  it "recovers a first-accept transient failure before a fresh generated call" do
    reactor do
      host = described_class.new(server: server)
      entered, release = controlled_backoff(host)
      faults = AcceptFaults.new { |_index, attempt| raise Errno::EMFILE, "first" if attempt == 1 }
      expect do
        faults.enable do
          expect(host.start).to equal(host)
          expect(entered.dequeue).to eq(0.005)
          release.enqueue(true)
          origin = "http://127.0.0.1:#{host.port}"
          transport = ConnectRPC::Async::Transport.new(origin: origin)
          expect(client(transport,
                        origin).echo(AsyncHelpers::MESSAGE.new(name: "first peer")).message.name).to eq("first peer")
          expect(faults.accepted[0]).to eq(1)
        ensure
          transport&.close(grace: 0)
          host.close(grace: 0)
        end
      end.to output(accept_warnings(Errno::EMFILE, [0.005])).to_stderr
    end
  end

  it "settles ordinary wait at listener stop while concurrent close still awaits admitted work" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      target = server(echo: lambda do |message, _context|
        entered.enqueue(true)
        release.dequeue
        message
      end)
      host = described_class.new(server: target, idle_timeout: nil)
      shutdown_error = RuntimeError.new("listener unwinding during ordinary shutdown")
      faults = AcceptFaults.new(after: -> { raise shutdown_error if host.instance_variable_get(:@closing) }) { nil }
      faults.enable do
        host.start
        origin = "http://127.0.0.1:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        pending = task.async { client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "held")) }
        entered.dequeue
        waiter = task.async { host.wait }
        closer = task.async { host.close(grace: 2) }
        expect(waiter.wait).to be_nil
        expect(faults.tasks).to all(be_finished)
        expect(closer).not_to be_finished
        expect(host_session(host).instance_variable_get(:@state)).to eq(:closing)
        second = task.async { host.close(grace: 0) }
        expect(second).not_to be_finished
        release.enqueue(true)
        expect(pending.wait.message.name).to eq("held")
        expect([closer.wait, second.wait]).to eq([true, true])
        expect(host.wait).to be_nil
      ensure
        release.enqueue(true)
        transport&.close(grace: 0)
        host.close(grace: 0)
      end
    end
  end

  it "starts terminal abort before a concurrent close can grant a drain window" do
    reactor do |task|
      entered = Async::Queue.new
      cleaning = Async::Queue.new
      release = Async::Queue.new
      trigger = Async::Queue.new
      original = IOError.new("failure wins close race")
      target = server(echo: lambda do |message, _context|
        entered.enqueue(true)
        begin
          Async::Queue.new.dequeue
        ensure
          cleaning.enqueue(true)
          await_cleanup_release(release)
        end
        message
      end)
      host = described_class.new(server: target, idle_timeout: nil)
      faults = AcceptFaults.new do |_index, attempt|
        if attempt == 2
          trigger.dequeue
          raise original
        end
      end
      faults.enable do
        host.start
        origin = "http://127.0.0.1:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        pending = task.async(finished: false) do
          client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "held"), timeout: nil)
        rescue ConnectRPC::Error => e
          e
        end
        entered.dequeue
        waiter = task.async { wait_error(host) }
        trigger.enqueue(true)
        cleaning.dequeue
        resources = host_session(host).instance_variable_get(:@resources).dup
        retained = resources.flat_map { |resource| resource.instance_variable_get(:@retirement_members).keys }
        retained.concat(faults.tasks)
        fibers = retained.filter_map(&:fiber)
        closer = task.async { host.close(grace: 5) }
        expect(host_session(host).instance_variable_get(:@aborting)).to be(true)
        expect(waiter).not_to be_finished
        expect(closer).not_to be_finished
        release.enqueue(true)
        expect(waiter.wait).to equal(original)
        expect(retained).to all(be_finished)
        expect(fibers).not_to be_empty
        expect(fibers).to all(satisfy { |fiber| !fiber.alive? })
        expect(resources).to all(satisfy { |resource| resource.send(:retirement_completed?) })
        expect(resources.map(&:raw)).to all(be_closed)
        expect(host_session(host).instance_variable_get(:@state)).to eq(:closed)
        expect(closer.wait).to be(false)
        expect(pending.wait).to be_a(ConnectRPC::Error)
        caller_error = RuntimeError.new("caller ensure after terminal failure")
        expect do
          raise caller_error
        ensure
          host.close
        end.to(raise_error { |error| expect(error).to equal(caller_error) })
      ensure
        release.enqueue(true)
        transport&.close(grace: 0)
        host.close(grace: 0)
      end
    end
  end
end
