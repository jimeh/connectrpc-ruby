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

          excess = IO::Endpoint.tcp("127.0.0.1", URI(state.fetch(:origin)).port).connect
          expect(excess.read(1)).to be_nil
          expect(session.instance_variable_get(:@resources)).to eq([resource])
          expect { resource.connection.synchronize { raise "late writer entered" } }.to raise_error(IOError)
          expect(resource.connection.writers).to be_empty
          gate.release
          join_retirement_tasks(task, aborter, waiter, state.fetch(:handler))
          expect(retirement_complete?(resource)).to be(true)
          expect(session.instance_variable_get(:@identities)).to be_empty
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
    it "retains the first external session #{operation} interruption until publication" do
      reactor do |task|
        with_retirement_connection(task) do |state|
          first = RuntimeError.new("first session interruption")
          second = Async::Cancel.new("second session interruption")
          joiner = task.async(finished: false) do
            operation == :abort ? state.fetch(:session).abort : state.fetch(:host).close(grace: 0)
          rescue Exception => e # rubocop:disable Lint/RescueException -- assert the exact escaping interruption
            e
          end
          state.fetch(:gate).entered.dequeue
          competitor = task.async(finished: false) { state.fetch(:host).close(grace: 0) }
          Fiber.scheduler.raise(joiner.fiber, first)
          Fiber.scheduler.raise(joiner.fiber, second)
          expect(joiner).not_to be_finished
          expect(competitor).not_to be_finished
          expect(state.fetch(:session).instance_variable_get(:@state)).to eq(:closing)
          state.fetch(:gate).release
          join_retirement_tasks(task, joiner, competitor)
          expect(joiner.result).to equal(first)
          expect(competitor.result).to be(false)
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
                                                       boundary.enqueue(session.instance_variable_get(:@resources)
                                                         .map { |resource| resource.raw.closed? })
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
      expect(boundary.dequeue).to eq([true, true])
      expect(aborter).not_to be_finished
      expect(session.instance_variable_get(:@state)).to eq(:closing)
      release.enqueue(true)
      join_retirement_tasks(task, aborter)
      expect(resources).to all(satisfy { |resource| retirement_complete?(resource) })
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
end
