# frozen_string_literal: true

require "spec_helper"
require "json"
require_relative "support/accept_faults"

RSpec.describe ConnectRPC::Async::Host, "exit ownership" do
  def exit_owned_tasks(resources, *extra)
    resources.flat_map do |resource|
      members = resource.instance_variable_get(:@retirement_members) || {}
      [resource.task, *resource.tasks, resource.connection&.reader_task,
       *(resource.connection&.writers&.keys || []), *members.keys]
    end.concat(extra.flatten).compact.uniq
  end

  def hold_start_return(session, scheduled, release, errors = [])
    original = session.method(:schedule)
    first = true
    session.define_singleton_method(:schedule) do |&work|
      listener = original.call(&work)
      if first
        first = false
        scheduled.enqueue(listener)
        begin
          release.dequeue
        rescue Async::Cancel => e
          errors << e
          raise
        end
      end
      listener
    end
  end

  it "preserves an admitted held RPC and the original pending graceful close when startup resumes" do
    reactor do |task|
      scheduled = Async::Queue.new
      release_start = Async::Queue.new
      entered = Async::Queue.new
      release_rpc = Async::Queue.new
      draining = Async::Queue.new
      target = server(echo: lambda do |message, _context|
        entered.enqueue(Async::Task.current)
        release_rpc.dequeue
        message
      end)
      host = described_class.new(server: target, idle_timeout: nil)
      session = host_session(host)
      hold_start_return(session, scheduled, release_start)
      expiries = []
      original = session.method(:drain_resources)
      session.define_singleton_method(:drain_resources) do |peers, expires|
        expiries << expires
        draining.enqueue(expires)
        original.call(peers, expires)
      end
      starter = task.async(finished: false) do
        host.start
      rescue StandardError => e
        e
      end
      listener = scheduled.dequeue
      sockets = host.instance_variable_get(:@bound).sockets.dup
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      call = task.async(finished: false) do
        client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "held-admitted"), timeout: nil)
      rescue ConnectRPC::Error => e
        e
      end
      handler = entered.dequeue
      resources = session.instance_variable_get(:@resources).dup
      expect(resources.size).to eq(1)
      expect(resources.first.ready).to be(true)
      closer = task.async(finished: false) { host.close(grace: 5) }
      expiry = draining.dequeue
      original_closer = session.instance_variable_get(:@closer)
      expect(original_closer).not_to be_nil
      expect(session.instance_variable_get(:@state)).to eq(:closing)
      expect(session.instance_variable_get(:@close_decided)).to be_nil
      expect(resources.first.draining).to be(true)
      expect(resources.first.raw).not_to be_closed
      expect(handler).not_to be_finished
      expect(closer).not_to be_finished
      release_start.enqueue(true)
      expect(starter.wait).to be_a(AsyncHelpers::LIFECYCLE_ERROR)
      AsyncHelpers::INTERNAL.join_complete(starter)
      # Product cleanup must finish without taking over the independent drain.
      expect(handler).not_to be_finished
      expect(call).not_to be_finished
      expect(closer).not_to be_finished
      expect(resources.first.raw).not_to be_closed
      expect(session.instance_variable_get(:@closer)).to equal(original_closer)
      expect(expiries).to eq([expiry])
      expect(session.instance_variable_get(:@close_decided)).to be_nil
      expect(host.wait).to be_nil
      exit_owner = host.instance_variable_get(:@exit_task)
      expect(exit_owner).to be_finished
      expect(listener).to be_finished
      expect(sockets).to all(be_closed)
      puts "EXIT_BOUNDARY #{JSON.generate(ruby: RUBY_VERSION, mode: :prior_close, starter: starter.status,
                                          exit_owner: exit_owner.status, handler: handler.status,
                                          rpc: call.status, closer: original_closer.object_id, expiry: expiry,
                                          decided: session.instance_variable_get(:@close_decided),
                                          raw_closed: resources.first.raw.closed?)}"
      release_rpc.enqueue(true)
      result = call.wait
      expect(result).to be_a(ConnectRPC::Response)
      expect(result.message).to be_a(AsyncHelpers::MESSAGE)
      expect(result.message.name).to eq("held-admitted")
      expect(closer.wait).to be(true)
      AsyncHelpers::INTERNAL.join_complete(closer)
      expect(session.instance_variable_get(:@closer)).to equal(original_closer)
      expect(expiries).to eq([expiry])
      expect(resources).to all(satisfy { |resource| resource.send(:retirement_completed?) })
      expect(session.instance_variable_get(:@state)).to eq(:closed)
      puts "EXIT_BOUNDARY prior_close final_typed_response=#{result.message.name} final_drain=#{closer.result}"
    ensure
      release_start&.enqueue(true)
      release_rpc&.enqueue(true)
      transport&.close(grace: 0)
      host&.close(grace: 0)
      # Cancel all callers before joining any caller that may depend on another.
      [starter, call, closer].compact.each { |owned| AsyncHelpers::INTERNAL.cancel(owned) }
      [starter, call, closer].compact.each { |owned| AsyncHelpers::INTERNAL.join_complete(owned) } # rubocop:disable Style/CombinableLoops
    end
  end

  %i[starter third owner].each do |mode|
    it "completes actual session retirement and ordinary wait after #{mode} interruption at listener join" do
      reactor do |task|
        scheduled = Async::Queue.new
        release_start = Async::Queue.new
        unwinding = Async::Queue.new
        release_unwind = Async::Queue.new
        joining = Async::Queue.new
        first_errors = []
        if mode == :owner
          ready = Async::Queue.new
          owner = task.async(finished: false) do
            ready.enqueue(described_class.new(server: server, idle_timeout: nil))
            Async::Queue.new.dequeue
          end
          host = ready.dequeue
        else
          host = described_class.new(server: server, idle_timeout: nil)
        end
        session = host_session(host)
        hold_start_return(session, scheduled, release_start, first_errors)
        unwind_seen = join_seen = false
        listener = starter = transport = nil
        faults = AcceptFaults.new(after: lambda {
          if host.instance_variable_get(:@closing) && !unwind_seen
            unwind_seen = true
            unwinding.enqueue(Async::Task.current)
            begin
              release_unwind.dequeue
            rescue Async::Cancel
              retry
            end
            raise IOError, "listener error after startup teardown intent" if mode == :third
          end
        }) { nil }
        completion = AsyncHelpers::INTERNAL.const_get(:CompletionJoin)
        trace = TracePoint.new(:call) do |event|
          if event.self.equal?(completion) && event.method_id == :call && !join_seen &&
             event.binding.local_variable_get(:task).equal?(listener)
            join_seen = true
            joining.enqueue(Async::Task.current)
          end
        end
        trace.enable
        faults.enable do
          starter = task.async(finished: false) do
            host.start
          rescue Exception => e # rubocop:disable Lint/RescueException -- inspect the identical escaping cancellation
            e
          end
          listener = scheduled.dequeue
          origin = "http://127.0.0.1:#{host.port}"
          transport = ConnectRPC::Async::Transport.new(origin: origin)
          response = client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "accepted-before-cancel"))
          expect(response.message.name).to eq("accepted-before-cancel")
          expect(faults.accepted[0]).to eq(1)
          resources = session.instance_variable_get(:@resources).dup
          expect(resources.size).to eq(1)
          retained = exit_owned_tasks(resources, starter, listener, session.instance_variable_get(:@lifetime))
          fibers = retained.filter_map(&:fiber)
          AsyncHelpers::INTERNAL.cancel(starter)
          expect(unwinding.dequeue).to equal(listener)
          expect(joining.dequeue).not_to be_nil
          expect(starter).not_to be_finished
          expect(listener).not_to be_finished
          expect(first_errors.size).to eq(1)
          second = RuntimeError.new("second starter cleanup interruption")
          Fiber.scheduler.raise(starter.fiber, second)
          expect(starter).not_to be_finished
          expect(listener).not_to be_finished
          exit_owner = host.instance_variable_get(:@exit_task)
          owner_fiber = exit_owner&.fiber
          if mode != :starter
            expect(exit_owner).not_to be_nil
            expect(exit_owner.parent).to equal(mode == :owner ? owner : task)
            expect(exit_owner).not_to be_finished
            expect(exit_owner).to be_transient
            Fiber.scheduler.raise(starter.fiber, RuntimeError.new("third starter interruption"))
            AsyncHelpers::INTERNAL.cancel(owner) if mode == :owner
            expect(starter).not_to be_finished
            expect(exit_owner).not_to be_finished
          end
          release_unwind.enqueue(true)
          expect(starter.wait).to equal(first_errors.first)
          AsyncHelpers::INTERNAL.join_complete(starter)
          # Assert before fixture close or any peer join can supply cleanup credit.
          expect(session.instance_variable_get(:@state)).to eq(:closed)
          expect(retained).to all(be_finished)
          expect(fibers).not_to be_empty
          expect(fibers).to all(satisfy { |fiber| !fiber.alive? })
          expect(resources.map(&:raw)).to all(be_closed)
          expect(resources).to all(satisfy { |resource| resource.send(:retirement_completed?) })
          expect(faults.listeners).to all(be_closed)
          exit_owner = host.instance_variable_get(:@exit_task)
          expect(exit_owner).to be_finished
          expect(exit_owner.fiber).to be_nil
          expect(owner_fiber).not_to be_nil
          expect(owner_fiber).not_to be_alive
          expect(task.with_timeout(0.2) { host.wait }).to be_nil
          expect(host.instance_variable_get(:@starting)).to be(false)
          retired = resources.all? { |resource| resource.send(:retirement_completed?) }
          puts "EXIT_BOUNDARY #{JSON.generate(ruby: RUBY_VERSION, mode: mode,
                                              first_error: first_errors.first.object_id,
                                              result: starter.result.object_id,
                                              owner: exit_owner.object_id, owner_finished: exit_owner.finished?,
                                              owner_fiber_alive: owner_fiber.alive?,
                                              tasks: retained.map { |owned| [owned.object_id, owned.finished?] },
                                              fibers: fibers.map { |fiber| [fiber.object_id, fiber.alive?] },
                                              sockets_closed: faults.listeners.all?(&:closed?),
                                              peers_closed: resources.all? { |resource| resource.raw.closed? },
                                              retired: retired)}"
        end
      ensure
        release_start&.enqueue(true)
        release_unwind&.enqueue(true)
        trace&.disable
        transport&.close(grace: 0)
        host&.close(grace: 0)
        AsyncHelpers::INTERNAL.cancel(starter)
        AsyncHelpers::INTERNAL.join_complete(starter) if starter
        AsyncHelpers::INTERNAL.cancel(owner)
        AsyncHelpers::INTERNAL.join_complete(owner) if owner
      end
    end
  end

  it "keeps terminal wait pending until the stored exit owner's actual finalization" do
    reactor do |task|
      trigger = Async::Queue.new
      resolved = Async::Queue.new
      release = Async::Queue.new
      original = IOError.new("terminal before ordinary close")
      host = described_class.new(server: server)
      promise = host.instance_variable_get(:@completion)
      resolve = promise.method(:resolve)
      promise.define_singleton_method(:resolve) do |value|
        resolve.call(value)
        next unless value.equal?(original)

        resolved.enqueue(Async::Task.current)
        release.dequeue
      end
      waiter = nil
      faults = AcceptFaults.new do
        trigger.dequeue
        raise original
      end
      faults.enable do
        host.start
        waiter = task.async(finished: false) do
          host.wait
        rescue StandardError => e
          e
        end
        trigger.enqueue(true)
        exit_owner = resolved.dequeue
        fiber = exit_owner.fiber
        expect(host.instance_variable_get(:@exit_task)).to equal(exit_owner)
        expect(promise).to be_resolved
        expect(host_session(host).instance_variable_get(:@state)).to eq(:closed)
        expect(faults.tasks).to all(be_finished)
        expect(faults.listeners).to all(be_closed)
        expect(exit_owner).not_to be_finished
        expect(waiter).not_to be_finished
        expect(host.close(grace: 5)).to be(false)
        expect(waiter).not_to be_finished
        release.enqueue(true)
        expect(waiter.wait).to equal(original)
        expect(exit_owner).to be_finished
        expect(fiber).not_to be_alive
        expect { host.wait }.to(raise_error { |error| expect(error).to equal(original) })
      end
    ensure
      release&.enqueue(true)
      trigger&.enqueue(true)
      AsyncHelpers::INTERNAL.cancel(waiter)
      AsyncHelpers::INTERNAL.join_complete(waiter) if waiter
      host&.close(grace: 0)
    end
  end

  it "clears startup state and joins forced retirement after an initial provider error" do
    reactor do
      original = RuntimeError.new("initial provider setup")
      host = described_class.new(server: server, tls_provider: -> { raise original })
      session = host_session(host)
      observed = []
      allow(session).to receive(:start).and_wrap_original do |start|
        start.call
      rescue StandardError => e
        observed << e
        raise
      end
      lifetime = session.instance_variable_get(:@lifetime)
      fiber = lifetime.fiber
      expect { host.start }.to(raise_error { |error| expect(error).to equal(observed.fetch(0)) })
      expect(observed.fetch(0).class.name).to eq("ConnectRPC::Async::TLSConfiguration::Error")
      expect(host.instance_variable_get(:@starting)).to be(false)
      expect(host.instance_variable_get(:@exit_task)).to be_finished
      expect(lifetime).to be_finished
      expect(fiber).not_to be_alive
      expect(host.wait).to be_nil
      expect { host.start }.to raise_error(AsyncHelpers::LIFECYCLE_ERROR, "Server session cannot start after close")
    ensure
      host&.close(grace: 0)
    end
  end

  it "rejects foreign start without retiring a healthy owner's listeners or admitted RPC connection" do
    reactor do
      host = described_class.new(server: server).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      typed = client(transport, origin)
      expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "before")).message.name).to eq("before")
      resource = host_session(host).instance_variable_get(:@resources).first
      foreign = Thread.new do
        Async do
          host.start
        rescue StandardError => e
          e
        end.wait
      end.value
      expect(foreign).to be_a(AsyncHelpers::LIFECYCLE_ERROR)
      expect(host.instance_variable_get(:@exit_task)).to be_nil
      expect(host_session(host).instance_variable_get(:@state)).to eq(:running)
      expect(host.instance_variable_get(:@starting)).to be(false)
      expect(resource.raw).not_to be_closed
      expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "after")).message.name).to eq("after")
      expect(host_session(host).instance_variable_get(:@resources)).to eq([resource])
    ensure
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end
end
