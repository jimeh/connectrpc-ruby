# frozen_string_literal: true

require_relative "support/retirement_helpers"

RSpec.describe "Shared resource retirement ownership" do
  include RetirementHelpers

  %i[h2c tls mtls].each do |mode|
    it "retains the retiring #{mode} resource, accepted handle and identity through cleanup" do
      reactor do |task|
        with_retirement_connection(task, mode: mode, max_connections: 1) do |state|
          resource, gate, session = state.values_at(:resource, :gate, :session)
          identities = Async::Queue.new
          state[:on_cleanup] = -> { identities.enqueue(state.fetch(:host).peer_certificate) }
          aborter = task.async(finished: false) { resource.abort }
          expect(gate.entered.dequeue).to equal(state.fetch(:handler))
          identity = identities.dequeue
          expect(identity&.serial&.to_i).to eq(mode == :mtls ? 201 : nil)
          expect(session.instance_variable_get(:@identities)).to have_key(state.fetch(:handler))
          waiter = task.async(finished: false) { state.fetch(:accepted).wait_closed }
          expect(resource).to be_finished
          expect(retirement_complete?(resource)).to be(false)
          expect(aborter).not_to be_finished
          expect(waiter).not_to be_finished

          # The retiring resource keeps its slot, so a new peer waits in the backlog.
          listener = state.fetch(:host).instance_variable_get(:@bound).sockets.first
          excess = IO::Endpoint.tcp("127.0.0.1", URI(state.fetch(:origin)).port).connect
          wait_until(task, "the excess peer queued or reset") do
            # A zero-timeout probe; Async runs IO.select on a thread.
            IO.select([listener], nil, nil, 0) || # rubocop:disable Lint/IncompatibleIoSelectWithFiberScheduler
              excess.read_nonblock(1, exception: false).nil?
          end
          expect(excess.read_nonblock(1, exception: false)).to eq(:wait_readable)
          expect(session.instance_variable_get(:@resources)).to eq([resource])
          expect { resource.connection.synchronize { raise "late writer entered" } }.to raise_error(IOError)
          expect(resource.connection.writers).to be_empty
          gate.release
          join_retirement_tasks(task, aborter, waiter, state.fetch(:handler))
          expect(retirement_complete?(resource)).to be(true)
          expect(session.instance_variable_get(:@identities)).to be_empty
          # Release the slot the excess peer takes once retirement completes.
          excess.close
          fresh = ConnectRPC::Async::Transport.new(origin: state.fetch(:origin), **state.fetch(:client_options))
          expect(client(fresh, state.fetch(:origin)).echo(AsyncHelpers::MESSAGE.new(name: "fresh")).message.name)
            .to eq("fresh")
          expect(session.instance_variable_get(:@resources)).not_to include(resource)
          expect(state.fetch(:host).close(grace: 0)).to be(false)
        ensure
          AsyncHelpers::INTERNAL.raw_close(excess)
          fresh&.close(grace: 0)
        end
      end
    end
  end

  it "retains only each external resource joiner's first interruption through repeated interruption" do
    reactor do |task|
      with_retirement_connection(task) do |state|
        resource, gate = state.values_at(:resource, :gate)
        aborter = task.async(finished: false) { resource.abort }
        gate.entered.dequeue
        first = Async::Cancel.new("first external interruption")
        second = RuntimeError.new("second external interruption")
        interrupted = task.async(finished: false) do
          resource.abort
        rescue Exception => e # rubocop:disable Lint/RescueException -- assert the exact escaping interruption
          e
        end
        competitor = task.async(finished: false) { resource.abort }
        Fiber.scheduler.raise(interrupted.fiber, first)
        Fiber.scheduler.raise(interrupted.fiber, second)
        expect(interrupted).not_to be_finished
        expect(competitor).not_to be_finished
        expect(retirement_complete?(resource)).to be(false)
        gate.release
        join_retirement_tasks(task, aborter, interrupted, competitor)
        expect(interrupted.result).to equal(first)
        expect(competitor.result).to be_nil
        expect(retirement_complete?(resource)).to be(true)
      end
    end
  end

  %i[resource_abort session_abort host_close].each do |operation|
    it "joins an initiating handler's full unwind after owned #{operation}" do
      reactor do |task|
        with_retirement_connection(task) do |state|
          resource, gate = state.values_at(:resource, :gate)
          state.fetch(:commands).enqueue(lambda do |_|
            case operation
            when :resource_abort then resource.abort
            when :session_abort then state.fetch(:session).abort
            when :host_close then state.fetch(:host).close(grace: 0)
            end
          end)
          gate.entered.dequeue
          external = task.async(finished: false) { resource.abort }
          expect(external).not_to be_finished
          expect(state.fetch(:handler)).not_to be_finished
          expect(retirement_complete?(resource)).to be(false)
          gate.release
          join_retirement_tasks(task, external, state.fetch(:handler))
          expect(retirement_complete?(resource)).to be(true)
        end
      end
    end
  end

  it "allows a cancelled handler to reenter session abort while session cleanup joins it" do
    reactor do |task|
      with_retirement_connection(task) do |state|
        gate = state.fetch(:gate)
        state[:on_cleanup] = -> { state.fetch(:session).abort }
        aborter = task.async(finished: false) { state.fetch(:session).abort }
        expect(gate.entered.dequeue).to equal(state.fetch(:handler))
        closer = task.async(finished: false) { state.fetch(:host).close(grace: 0) }
        expect(aborter).not_to be_finished
        expect(closer).not_to be_finished
        expect(state.fetch(:session).instance_variable_get(:@state)).to eq(:closing)
        gate.release
        join_retirement_tasks(task, aborter, closer, state.fetch(:handler))
        expect(closer.result).to be(false)
        expect(state.fetch(:host).close(grace: 0)).to be(false)
      end
    end
  end

  it "keeps the abort outcome when a previously started close finishes draining later" do
    reactor do |task|
      with_retirement_connection(task) do |state|
        closer = task.async(finished: false) { state.fetch(:host).close(grace: 2) }
        aborter = task.async(finished: false) { state.fetch(:session).abort }
        state.fetch(:gate).entered.dequeue
        expect(closer).not_to be_finished
        state.fetch(:gate).release
        join_retirement_tasks(task, closer, aborter)
        expect(closer.result).to be(false)
        expect(state.fetch(:host).close(grace: 0)).to be(false)
      end
    end
  end

  it "joins captured descendants after reparenting and after their root promise resolves" do
    reactor do |task|
      with_retirement_connection(task) do |state|
        resource = state.fetch(:resource)
        child_entered = Async::Queue.new
        child_gate = RetirementHelpers::CleanupGate.new
        lifetime = state.fetch(:session).instance_variable_get(:@lifetime)
        root = Async::Task.new(lifetime, finished: false) do |current|
          current.async(finished: false) do |child|
            child_entered.enqueue(child)
            Async::Condition.new.wait
          ensure
            # Move out of the ordinary root tree while retaining owned identity.
            child.parent = lifetime
            resource.abort
            child_gate.hold($ERROR_INFO)
          end
          Async::Condition.new.wait
        end
        resource.track(root)
        root.run
        child = child_entered.dequeue
        aborter = task.async(finished: false) { resource.abort }
        child_gate.entered.dequeue
        state.fetch(:gate).entered.dequeue
        expect(root).to be_finished
        expect(child.parent).to equal(lifetime)
        expect(child).not_to be_finished
        state.fetch(:gate).release
        task.yield
        expect(aborter).not_to be_finished
        expect(retirement_complete?(resource)).to be(false)
        child_gate.release
        join_retirement_tasks(task, aborter, child, root)
        expect(retirement_complete?(resource)).to be(true)
      ensure
        child_gate&.release
      end
    end
  end

  it "joins ordinary descendants created during handler cleanup without cancelling them twice" do
    reactor do |task|
      with_retirement_connection(task) do |state|
        child_gate = RetirementHelpers::CleanupGate.new
        child = nil
        state[:on_cleanup] = lambda do
          child = Async::Task.current.async(finished: false) { child_gate.hold }
        end
        aborter = task.async(finished: false) { state.fetch(:resource).abort }
        child_gate.entered.dequeue
        state.fetch(:gate).entered.dequeue
        state.fetch(:gate).release
        state.fetch(:handler).wait
        expect(state.fetch(:handler)).not_to be_finished
        expect(aborter).not_to be_finished
        expect(child_gate.interruptions).not_to be_empty
        child_gate.release
        join_retirement_tasks(task, aborter, child, state.fetch(:handler))
      ensure
        child_gate&.release
      end
    end
  end

  it "keeps a removed writer owned during reentry and refuses a new writer" do
    reactor do |task|
      with_retirement_connection(task) do |state|
        resource = state.fetch(:resource)
        writer_gate = RetirementHelpers::CleanupGate.new
        writer = task.async(finished: false) do
          resource.connection.synchronize do
            # Nested entry remains legal for the currently held writer.
            resource.connection.synchronize { resource.abort }
          end
        ensure
          resource.abort
          writer_gate.hold($ERROR_INFO)
        end
        writer_gate.entered.dequeue
        state.fetch(:gate).entered.dequeue
        expect(resource.connection.writers).not_to have_key(writer)
        expect { resource.connection.synchronize { raise "late writer entered" } }.to raise_error(IOError)
        external = task.async(finished: false) { resource.abort }
        state.fetch(:gate).release
        task.yield
        expect(external).not_to be_finished
        writer_gate.release
        join_retirement_tasks(task, writer, external)
      ensure
        writer_gate&.release
      end
    end
  end

  it "does not absorb an arbitrary transient application child into retirement" do
    reactor do |task|
      with_retirement_connection(task) do |state|
        started = Async::Queue.new
        release = Async::Queue.new
        state.fetch(:commands).enqueue(lambda do |_|
          Async::Task.current.async(transient: true) do |child|
            started.enqueue(child)
            release.dequeue
          end
          Async::Condition.new.wait
        end)
        child = started.dequeue
        aborter = task.async(finished: false) { state.fetch(:resource).abort }
        state.fetch(:gate).entered.dequeue
        state.fetch(:gate).release
        join_retirement_tasks(task, aborter, state.fetch(:handler))
        expect(retirement_complete?(state.fetch(:resource))).to be(true)
        # Retirement has completed through real owned joins while the
        # deliberately transient application task still waits for its owner.
        expect(child).not_to be_finished
        expect(child).not_to be_cancelled
        release.enqueue(true)
        join_retirement_tasks(task, child)
      ensure
        release&.enqueue(true)
      end
    end
  end
end

RSpec.describe "Retirement lifecycle coordinators" do
  include RetirementHelpers

  %i[abort close].each do |operation|
    it "raises an external session #{operation} interruption at once and leaves retirement running" do
      reactor do |task|
        with_retirement_connection(task) do |state|
          first = RuntimeError.new("first session interruption")
          joiner = task.async(finished: false) do
            operation == :abort ? state.fetch(:session).abort : state.fetch(:host).close(grace: 0)
          rescue Exception => e # rubocop:disable Lint/RescueException -- assert the exact escaping interruption
            e
          end
          state.fetch(:gate).entered.dequeue
          competitor = task.async(finished: false) { state.fetch(:host).close(grace: 0) }
          Fiber.scheduler.raise(joiner.fiber, first)
          expect(joiner).to be_finished
          expect(joiner.result).to equal(first)
          expect(competitor).not_to be_finished
          expect(retirement_complete?(state.fetch(:resource))).to be(false)
          expect(state.fetch(:session).instance_variable_get(:@state)).to eq(:closing)
          state.fetch(:gate).release
          join_retirement_tasks(task, competitor)
          expect(competitor.result).to be(false)
          expect(retirement_complete?(state.fetch(:resource))).to be(true)
        end
      end
    end
  end

  it "keeps the lifetime guard alive through repeated owner cancellation" do
    reactor do |task|
      ready = Async::Queue.new
      entered = Async::Queue.new
      gate = RetirementHelpers::CleanupGate.new
      owner = task.async(finished: false) do |current|
        host = ConnectRPC::Async::Host.new(server: server(echo: lambda { |message, _|
          begin
            entered.enqueue(Async::Task.current)
            Async::Condition.new.wait
            message
          ensure
            gate.hold($ERROR_INFO)
          end
        }), idle_timeout: nil).start
        origin = "http://127.0.0.1:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        current.async(finished: false) do
          client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "held"), timeout: nil)
        rescue ConnectRPC::Error
          nil
        end
        handler = entered.dequeue
        session = host.instance_variable_get(:@session)
        ready.enqueue([host, session, handler, session.instance_variable_get(:@resources).first])
        # No explicit close ensure: cancellation must reach the lifetime guard.
        Async::Condition.new.wait
      end
      host, session, handler, resource = ready.dequeue
      guard = session.instance_variable_get(:@lifetime)
      owned = [resource.task, handler, resource.connection.reader_task, *resource.tasks,
               *resource.connection.writers.keys].compact.uniq
      guard_entered = Async::Queue.new
      trace = TracePoint.new(:call) do |event|
        if event.self.equal?(session) && event.method_id == :abort_owned && Async::Task.current.equal?(guard)
          guard_entered.enqueue(true)
        end
      end
      trace.enable
      owner.cancel
      task.with_timeout(2) { guard_entered.dequeue }
      gate.entered.dequeue
      owner.cancel
      expect(guard).not_to be_finished
      expect(owner).not_to be_finished
      expect(session.instance_variable_get(:@state)).to eq(:closing)
      expect(retirement_complete?(resource)).to be(false)
      gate.release
      task.with_timeout(2) do
        [owner, guard, *owned].each(&:wait)
        wait_until(task, "owner, lifetime guard and retained resource tasks finish") do
          [owner, guard, *owned].all?(&:finished?)
        end
      end
      expect(retirement_complete?(resource)).to be(true)
      expect(session.instance_variable_get(:@state)).to eq(:closed)
    ensure
      trace&.disable
      gate&.release
      host&.close(grace: 0)
    end
  end

  it "lets serve interrupt an idle watcher without interrupting its cleanup owner" do
    reactor do |task|
      host = ConnectRPC::Async::Host.new(server: server, idle_timeout: 0.02).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "warm"))
      session = host.instance_variable_get(:@session)
      resource = session.instance_variable_get(:@resources).first
      cleanup_gate = RetirementHelpers::CleanupGate.new
      descendant = resource.task.async(finished: false) do
        Async::Condition.new.wait
      ensure
        cleanup_gate.hold($ERROR_INFO)
      end
      # The actual network idle timer is the trigger. The queue observes its
      # cancellation reaching a real serve descendant; no timing assumption.
      cleanup_gate.entered.dequeue
      cleanup = resource.instance_variable_get(:@cleanup)
      expect(cleanup).to be_transient
      expect(cleanup.parent).to equal(session.instance_variable_get(:@lifetime))
      expect(cleanup).not_to be_finished
      expect(retirement_complete?(resource)).to be(false)
      cleanup_gate.release
      join_retirement_tasks(task, cleanup, descendant, resource.task)
      expect(retirement_complete?(resource)).to be(true)
    ensure
      cleanup_gate&.release
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  it "invalidates all accepted raw peers before listener cleanup yields" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      port = bound.sockets.first.local_address.ip_port
      boundary = Async::Queue.new
      release = Async::Queue.new
      session = ConnectRPC::Async::ServerSession.new(application: ConnectRPC::Async::Application.new(server: server),
                                                     stop_accepting: lambda {
                                                       boundary.enqueue(true)
                                                       release.dequeue
                                                       bound.close
                                                     }).start
      peers = 2.times.map { IO::Endpoint.tcp("127.0.0.1", port).connect }
      handles = 2.times.map do
        raw, = bound.sockets.first.accept
        session.accept(raw)
      end
      resources = session.instance_variable_get(:@resources).dup
      aborter = task.async(finished: false) { session.abort }
      boundary.dequeue
      # While listener cleanup is held, each peer reads end of stream; the
      # server descriptors stay open until retirement.
      wait_until(task, "both peers read end of stream") do
        peers.all? { |peer| peer.read_nonblock(1, exception: false).nil? }
      end
      expect(resources.map(&:raw)).to all(satisfy { |raw| !raw.closed? })
      expect(aborter).not_to be_finished
      expect(session.instance_variable_get(:@state)).to eq(:closing)
      release.enqueue(true)
      join_retirement_tasks(task, aborter)
      expect(resources).to all(satisfy { |resource| retirement_complete?(resource) })
      expect(resources.map(&:raw)).to all(be_closed)
      # Accepted but unscheduled work cannot start setup after completion.
      handles.each(&:serve)
      expect(resources.map(&:task)).to eq([nil, nil])
      expect(session.close(grace: 0)).to be(false)
    ensure
      release&.enqueue(true)
      peers&.each { |raw| AsyncHelpers::INTERNAL.raw_close(raw) }
      session&.abort
      bound&.close
    end
  end

  def finalization(session)
    tls = session.instance_variable_get(:@tls)
    [session.instance_variable_get(:@state), tls.status[:state], tls.instance_variable_get(:@timer)&.finished?,
     session.instance_variable_get(:@lifetime).finished?]
  end

  # Session retirement fails every peer, then retires every resource whatever
  # fails, is interrupted or prunes the resource list. It then closes the
  # session and its TLS configuration, reports each failure and raises only
  # the first interruption.
  {
    listener: "listener cleanup fails",
    resource_raises: "listener cleanup fails and the first resource abort raises",
    pruned: "listener cleanup fails and a listener prunes a retired resource",
    resource_interrupted: "listener cleanup fails and the first resource abort is interrupted",
    http1_interrupted: "listener cleanup fails and an HTTP/1 resource abort is interrupted"
  }.each do |mode, description|
    it "closes accepted raw peers that never served when #{description}" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        port = bound.sockets.first.local_address.ip_port
        failure = RuntimeError.new("listener cleanup failed")
        interruption = Async::Cancel.new("interrupted")
        events = []
        session = ConnectRPC::Async::ServerSession.new(application: ConnectRPC::Async::Application.new(server: server),
                                                       tls_provider: -> { host_tls.merge(version: "one") },
                                                       stop_accepting: lambda {
                                                         events << :listener
                                                         raise failure
                                                       }).start
        peers = 2.times.map do
          peer = IO::Endpoint.tcp("127.0.0.1", port).connect
          raw, = bound.sockets.first.accept
          session.accept(raw)
          peer
        end
        resources = session.instance_variable_get(:@resources).dup
        case mode
        when :resource_raises, :resource_interrupted
          error = mode == :resource_raises ? RuntimeError.new("first resource abort failed") : interruption
          allow(resources.first).to receive(:abort).and_wrap_original do |abort|
            abort.call
            raise error
          end
        when :pruned
          # A listener waiting for capacity prunes the retired resource.
          allow(resources.first).to receive(:abort).and_wrap_original do |abort|
            abort.call
            session.send(:capacity?)
          end
        when :http1_interrupted
          # An HTTP/1 resource fails its raw socket with every other peer,
          # before listener cleanup can yield. Its own retirement closes it.
          http1 = Object.new
          http1.define_singleton_method(:fail_raw) { events << :http1_failed }
          http1.define_singleton_method(:abort) { raise interruption }
          http1.define_singleton_method(:owns_task?) { |_task| false }
          http1.define_singleton_method(:retirement_completed?) { true }
          session.instance_variable_get(:@resources).unshift(http1)
        end
        # A failure never reaches the abort caller. An interruption ends the
        # session cleanup cancelled, which abort does not raise either.
        warnings = mode == :resource_raises ? [RuntimeError, RuntimeError] : [RuntimeError]
        expect { expect(session.abort).to equal(session) }.to output(teardown_warnings(*warnings)).to_stderr
        expect(events).to eq(mode == :http1_interrupted ? %i[http1_failed listener] : [:listener])
        expect(resources).to all(satisfy { |resource| retirement_complete?(resource) })
        expect(resources.map(&:raw)).to all(be_closed)
        wait_until(task, "both peers read end of stream") do
          peers.all? { |peer| peer.read_nonblock(1, exception: false).nil? }
        end
        # Whatever failed, the session, its TLS refresh timer and its lifetime
        # guard have finished, so the guard has nothing left to report.
        expect(finalization(session)).to eq([:closed, :closed, true, true])
        expect(session.close(grace: 0)).to be(false)
      ensure
        peers&.each { |raw| AsyncHelpers::INTERNAL.raw_close(raw) }
        bound&.close
      end
    end
  end

  # The failed listener cleanup skips neither TLS close nor the drain, whose
  # result, true with no connection to drain, is the close's. The close and
  # the session cleanup each report their own call's failure.
  it "closes the session and returns the drain result when graceful close's listener cleanup fails" do
    reactor do
      session = ConnectRPC::Async::ServerSession.new(application: ConnectRPC::Async::Application.new(server: server),
                                                     tls_provider: -> { host_tls.merge(version: "one") },
                                                     stop_accepting: -> { raise "listener cleanup failed" }).start
      expect { expect(session.close(grace: 0)).to be(true) }
        .to output(teardown_warnings(RuntimeError, RuntimeError)).to_stderr
      expect(finalization(session)).to eq([:closed, :closed, true, true])
      expect(session.close(grace: 0)).to be(true)
    end
  end

  it "finishes TLS shutdown when the session cleanup is interrupted while a provider callback unwinds" do
    reactor do |task|
      gate = RetirementHelpers::CleanupGate.new
      checks = 0
      session = ConnectRPC::Async::ServerSession.new(
        application: ConnectRPC::Async::Application.new(server: server), on_tls_error: ->(_) {},
        tls_provider: lambda {
          checks += 1
          next host_tls.merge(version: "one") if checks == 1

          begin
            Async::Condition.new.wait
          ensure
            gate.hold
          end
        }
      ).start
      tls = session.instance_variable_get(:@tls)
      refresh = task.async(finished: false) { session.refresh_tls }
      wait_until(task, "the provider refresh started") { tls.instance_variable_get(:@callback) }
      callback = tls.instance_variable_get(:@callback)
      aborter = task.async(finished: false) { session.abort }
      cleanup = gate.entered.dequeue && session.instance_variable_get(:@cleanup)
      expect(cleanup).not_to be_finished
      # The cleanup is joining the provider callback's held unwind.
      Fiber.scheduler.raise(cleanup.fiber, Async::Cancel.new("cleanup interrupted"))
      expect(cleanup).not_to be_finished
      expect(tls.status[:state]).to eq(:closing)
      gate.release
      join_retirement_tasks(task, aborter, cleanup, callback)
      expect(aborter.result).to equal(session)
      expect(cleanup).to be_cancelled
      expect(finalization(session)).to eq([:closed, :closed, true, true])
      expect { refresh.wait }.to raise_error(ConnectRPC::Async::TLSConfiguration::Error)
    ensure
      gate&.release
    end
  end

  # An abort caller that was not itself interrupted returns once the session
  # closed, even when the cleanup task itself ended cancelled: raising that
  # cancellation into the caller would unwind it as if it were cancelled.
  it "closes the session without raising into an abort caller when its cleanup is cancelled" do
    reactor do |task|
      with_retirement_connection(task) do |state|
        session, gate, resource = state.values_at(:session, :gate, :resource)
        aborter = task.async(finished: false) { session.abort }
        expect(gate.entered.dequeue).to equal(state.fetch(:handler))
        cleanup = session.instance_variable_get(:@cleanup)
        cleanup.cancel
        expect(cleanup).not_to be_finished
        expect(session.instance_variable_get(:@state)).to eq(:closing)
        gate.release
        join_retirement_tasks(task, aborter, cleanup)
        expect(aborter.result).to equal(session)
        expect(cleanup).to be_cancelled
        expect(retirement_complete?(resource)).to be(true)
        expect(session.instance_variable_get(:@state)).to eq(:closed)
        wait_until(task, "lifetime guard finished") { session.instance_variable_get(:@lifetime).finished? }
      end
    end
  end

  it "closes the session without raising into an abort caller when the session owner is cancelled" do
    reactor do |task|
      with_retirement_connection(task) do |state|
        session, gate, resource = state.values_at(:session, :gate, :resource)
        guard = session.instance_variable_get(:@lifetime)
        aborter = task.async(finished: false) { session.abort }
        expect(gate.entered.dequeue).to equal(state.fetch(:handler))
        # The owner's cancellation reaches the lifetime guard, which joins the
        # running cleanup rather than starting another.
        guard.cancel
        expect(guard).not_to be_finished
        expect(session.instance_variable_get(:@state)).to eq(:closing)
        gate.release
        join_retirement_tasks(task, aborter, guard)
        expect(aborter.result).to equal(session)
        expect(guard).to be_cancelled
        expect(retirement_complete?(resource)).to be(true)
        expect(session.instance_variable_get(:@state)).to eq(:closed)
      end
    end
  end

  # A client transport's forced teardown fails every connection first and
  # retires each one whatever another's abort raises or is interrupted by. It
  # reports a failure and returns false, and raises an interruption once,
  # after closing.
  %i[raises interrupted].each do |mode|
    it "retires every client connection when the first forced connection abort #{mode}" do
      reactor do |task|
        entered = Async::Queue.new
        host = ConnectRPC::Async::Host.new(server: server(echo: lambda { |message, _|
          entered.enqueue(true)
          Async::Condition.new.wait
          message
        }), max_concurrent_streams: 1, idle_timeout: nil).start
        origin = "http://127.0.0.1:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin, connection_limit: 2)
        calls = 2.times.map do
          task.async(finished: false) do
            client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "held"), timeout: nil)
          rescue ConnectRPC::Error => e
            e
          end
        end
        2.times { entered.dequeue }
        resources = transport.instance_variable_get(:@resources).dup
        expect(resources.size).to eq(2)
        error = mode == :raises ? RuntimeError.new("connection abort failed") : Async::Cancel.new("abort interrupted")
        # Like a joined retirement's stored failure, every teardown join sees
        # it. The connection's own reader, which also aborts it once its raw
        # socket fails, does not.
        sibling_failed = []
        allow(resources.first).to receive(:abort).and_wrap_original do |abort, *arguments|
          teardown = caller_locations.any? { |location| location.label.match?(/Transport#(close|abort)_owned\z/) }
          sibling_failed << resources.last.send(:raw_failed?) if teardown
          abort.call(*arguments)
          raise error if teardown
        end
        if mode == :raises
          expect { expect(transport.close(grace: 0)).to be(false) }.to output(teardown_warnings(RuntimeError)).to_stderr
        else
          # The closer itself ends cancelled, so close returns no outcome.
          expect(transport.close(grace: 0)).to be_nil
        end
        # The second connection had already failed before the first abort could yield.
        expect(sibling_failed).to eq([true])
        expect(resources).to all(satisfy { |resource| retirement_complete?(resource) })
        expect(resources.map(&:raw)).to all(be_closed)
        expect(transport.instance_variable_get(:@state)).to eq(:closed)
        expect(transport.instance_variable_get(:@lifetime)).to be_finished
        expect(transport.close(grace: 0)).to be(false)
        join_retirement_tasks(task, *calls)
        expect(calls.map(&:result)).to all(be_a(ConnectRPC::Error))
      ensure
        host&.close(grace: 0)
      end
    end
  end
end
