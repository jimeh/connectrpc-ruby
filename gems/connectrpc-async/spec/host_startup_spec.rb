# frozen_string_literal: true

require "spec_helper"
require_relative "support/accept_faults"

RSpec.describe ConnectRPC::Async::Host, "startup ownership" do
  def bounded_startup_wait(task, host)
    task.with_timeout(0.2) { host.wait }
  rescue Async::TimeoutError
    :unresolved
  end

  %i[starter owner].each do |cancelled|
    it "settles ordinary wait after #{cancelled} cancellation during yielding real bind" do
      reactor do |task|
        entered = Async::Queue.new
        release = Async::Queue.new
        observed = []
        bound = nil
        sockets = []
        host = nil
        owner = task.async(finished: false) do |application|
          host = described_class.new(server: server)
          real_bind = host.method(:bind)
          host.define_singleton_method(:bind) do
            bound = real_bind.call
            sockets = bound.sockets.dup
            entered.enqueue(true)
            begin
              release.dequeue
              bound
            rescue Async::Cancel => e
              observed << e
              # The wrapper has not handed these sockets to Host yet.
              bound.close
              raise
            end
          end
          starter = application.async(finished: false) do
            host.start
          rescue Async::Cancel => e
            observed << e
            raise
          end
          entered.enqueue(starter)
          Async::Queue.new.dequeue
        end
        entered.dequeue
        starter = entered.dequeue
        retained = [owner, starter, host_session(host).instance_variable_get(:@lifetime)]
        fibers = retained.filter_map(&:fiber)
        expect(sockets).not_to be_empty
        expect(sockets).to all(satisfy { |socket| !socket.closed? })
        AsyncHelpers::INTERNAL.cancel(cancelled == :owner ? owner : starter)
        AsyncHelpers::INTERNAL.join_complete(cancelled == :owner ? owner : starter)
        expect(observed.length).to eq(2)
        expect(observed.last).to equal(observed.first)
        host.close(grace: 0)
        expect(sockets).to all(be_closed)
        AsyncHelpers::INTERNAL.cancel(owner)
        AsyncHelpers::INTERNAL.join_complete(owner)
        expect(retained).to all(be_finished)
        expect(fibers).to all(satisfy { |fiber| !fiber.alive? })
        expect(bounded_startup_wait(task, host)).to be_nil
      ensure
        release&.enqueue(true)
        AsyncHelpers::INTERNAL.cancel(owner)
        AsyncHelpers::INTERNAL.join_complete(owner) if owner
        host&.close(grace: 0)
        bound&.close
      end
    end
  end

  it "retires real listeners acquired after concurrent close completed during bind" do
    reactor do |task|
      host = described_class.new(server: server)
      entered = Async::Queue.new
      release = Async::Queue.new
      bound = nil
      sockets = []
      real_bind = host.method(:bind)
      host.define_singleton_method(:bind) do
        entered.enqueue(true)
        release.dequeue
        bound = real_bind.call
        sockets = bound.sockets.dup
        bound
      end
      starter = task.async do
        host.start
      rescue StandardError => e
        e
      end
      entered.dequeue
      expect(host.close(grace: 0)).to be(true)
      release.enqueue(true)
      outcome = starter.wait
      AsyncHelpers::INTERNAL.join_complete(starter)
      expect(sockets).not_to be_empty
      expect(sockets).to all(be_closed)
      expect(outcome).to be_a(AsyncHelpers::LIFECYCLE_ERROR)
      expect(host.wait).to be_nil
      expect(host.instance_variable_get(:@listeners)).to all(be_finished)
    ensure
      release&.enqueue(true)
      AsyncHelpers::INTERNAL.cancel(starter)
      AsyncHelpers::INTERNAL.join_complete(starter) if starter
      host&.close(grace: 0)
      bound&.close
    end
  end

  %i[starter owner].each do |cancelled|
    it "registers eager accept before yielding and retires its task after #{cancelled} interruption" do
      reactor do |task|
        entered = Async::Queue.new
        release = Async::Queue.new
        registered = nil
        host = nil
        starter = nil
        faults = AcceptFaults.new do
          registered = host.instance_variable_get(:@listeners).include?(Async::Task.current)
        end
        owner = task.async(finished: false) do |application|
          host = described_class.new(server: server)
          allow(host_session(host)).to receive(:schedule).and_wrap_original do |schedule, &block|
            listener = schedule.call(&block)
            entered.enqueue(listener)
            release.dequeue
            listener
          end
          faults.enable do
            starter = application.async { host.start }
            Async::Queue.new.dequeue
          end
        end
        listener = entered.dequeue
        retained = [owner, starter, listener, host_session(host).instance_variable_get(:@lifetime)]
        fibers = retained.filter_map(&:fiber)
        AsyncHelpers::INTERNAL.cancel(cancelled == :owner ? owner : starter)
        AsyncHelpers::INTERNAL.join_complete(cancelled == :owner ? owner : starter)
        host.close(grace: 0)
        expect(registered).to be(true)
        expect(listener).to be_finished
        expect(faults.listeners).to all(be_closed)
        expect(bounded_startup_wait(task, host)).to be_nil
        AsyncHelpers::INTERNAL.cancel(owner)
        AsyncHelpers::INTERNAL.join_complete(owner)
        expect(retained).to all(be_finished)
        expect(fibers).to all(satisfy { |fiber| !fiber.alive? })
      ensure
        release&.enqueue(true)
        AsyncHelpers::INTERNAL.cancel(owner)
        AsyncHelpers::INTERNAL.join_complete(owner) if owner
        host&.close(grace: 0)
      end
    end
  end

  it "retires collected listeners when close completes before scheduling returns" do
    reactor do |task|
      host = described_class.new(server: server)
      entered = Async::Queue.new
      release = Async::Queue.new
      allow(host_session(host)).to receive(:schedule).and_wrap_original do |schedule, &block|
        listener = schedule.call(&block)
        entered.enqueue(listener)
        release.dequeue
        listener
      end
      faults = AcceptFaults.new { nil }
      faults.enable do
        starter = task.async do
          host.start
        rescue StandardError => e
          e
        end
        listener = entered.dequeue
        fiber = listener.fiber
        expect(host.close(grace: 0)).to be(true)
        expect(listener).to be_finished
        expect(fiber).not_to be_alive
        expect(faults.listeners).to all(be_closed)
        release.enqueue(true)
        expect(starter.wait).to be_a(AsyncHelpers::LIFECYCLE_ERROR)
        AsyncHelpers::INTERNAL.join_complete(starter)
        expect(bounded_startup_wait(task, host)).to be_nil
      ensure
        release.enqueue(true)
        AsyncHelpers::INTERNAL.cancel(starter)
        AsyncHelpers::INTERNAL.join_complete(starter) if starter
        host.close(grace: 0)
      end
    end
  end

  it "preserves the first terminal cause when startup collection is cancelled" do
    reactor do |task|
      host = described_class.new(server: server)
      entered = Async::Queue.new
      release = Async::Queue.new
      original = IOError.new("terminal before startup cancellation")
      allow(host_session(host)).to receive(:schedule).and_wrap_original do |schedule, &block|
        listener = schedule.call(&block)
        entered.enqueue(listener)
        release.dequeue
        listener
      end
      faults = AcceptFaults.new { raise original }
      faults.enable do
        caught = nil
        starter = task.async do
          host.start
        rescue StandardError => e
          caught = e
        end
        listener = entered.dequeue
        AsyncHelpers::INTERNAL.cancel(starter)
        AsyncHelpers::INTERNAL.join_complete(starter)
        expect(caught).to equal(original)
        expect { host.wait }.to(raise_error { |error| expect(error).to equal(original) })
        expect(listener).to be_finished
        expect(faults.listeners).to all(be_closed)
        expect(host_session(host).instance_variable_get(:@state)).to eq(:closed)
      ensure
        release.enqueue(true)
        AsyncHelpers::INTERNAL.cancel(starter)
        AsyncHelpers::INTERNAL.join_complete(starter) if starter
        host.close(grace: 0)
      end
    end
  end
  it "aborts initial provider startup after starter cancellation without replacing its cause" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      observed = []
      host = described_class.new(server: server, tls_provider: lambda {
        entered.enqueue(Async::Task.current)
        release.dequeue
      })
      session = host_session(host)
      allow(session).to receive(:start).and_wrap_original do |start|
        start.call
      rescue Async::Cancel => e
        observed << e
        raise
      end
      starter = task.async do
        host.start
      rescue Async::Cancel => e
        observed << e
        raise
      end
      provider = entered.dequeue
      retained = [starter, provider, session.instance_variable_get(:@lifetime)]
      fibers = retained.filter_map(&:fiber)
      AsyncHelpers::INTERNAL.cancel(starter)
      AsyncHelpers::INTERNAL.join_complete(starter)
      expect(observed.length).to eq(2)
      expect(observed.last).to equal(observed.first)
      expect(session.instance_variable_get(:@state)).to eq(:closed)
      expect(retained).to all(be_finished)
      expect(fibers).to all(satisfy { |fiber| !fiber.alive? })
      expect(host.wait).to be_nil
    ensure
      release&.enqueue(true)
      AsyncHelpers::INTERNAL.cancel(starter)
      AsyncHelpers::INTERNAL.join_complete(starter) if starter
      host&.close(grace: 0)
    end
  end

  def settled_start(task, starter, host)
    task.with_timeout(2) { starter.wait }
  rescue Async::TimeoutError
    session = host_session(host)
    raise "Outer start did not settle; exit_task=#{host.instance_variable_get(:@exit_task)&.status.inspect} " \
          "session=#{session.instance_variable_get(:@state).inspect} " \
          "cleanup=#{session.instance_variable_get(:@cleanup)&.status.inspect}"
  end

  %i[provider reporter].each do |callback|
    it "rejects recursive start from the initial TLS #{callback} before any startup cleanup" do
      reactor do |task|
        host = nil
        rejected = []
        events = []
        recurse = lambda do
          host.start
        rescue AsyncHelpers::LIFECYCLE_ERROR => e
          rejected << { error: e, task: Async::Task.current, exit_task: host.instance_variable_get(:@exit_task),
                        starting: host.instance_variable_get(:@starting),
                        state: host_session(host).instance_variable_get(:@state) }
        end
        provider = lambda do
          raise "initial provider setup" unless callback == :provider

          recurse.call
          host_tls.merge(version: "one")
        end
        host = described_class.new(server: server, tls_provider: provider, on_tls_error: lambda { |event|
          events << event[:category]
          recurse.call if callback == :reporter
        })
        session = host_session(host)
        starter = task.async(finished: false) do
          host.start
        rescue StandardError => e
          e
        end
        outcome = settled_start(task, starter, host)
        expect(rejected.size).to eq(1)
        expect(rejected.first[:error].message).to eq("Recursive TLS provider lifecycle call")
        expect(rejected.first.slice(:exit_task, :starting, :state)).to eq(exit_task: nil, starting: true, state: :new)
        expect(rejected.first[:task]).to be_finished
        if callback == :provider
          expect(outcome).to equal(host)
          expect(events).to be_empty
          expect(host.instance_variable_get(:@exit_task)).to be_nil
          expect(session.instance_variable_get(:@state)).to eq(:running)
          expect(host.tls_generation).to eq(1)
          with_ca do |ca|
            origin = "https://127.0.0.1:#{host.port}"
            transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca)
            expect(client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "served")).message.name)
              .to eq("served")
          ensure
            transport&.close(grace: 0)
          end
        else
          expect(outcome).to be_a(ConnectRPC::Async::TLSConfiguration::Error)
          expect(outcome.category).to eq(:source)
          expect(events).to eq([:source])
          expect(host.instance_variable_get(:@starting)).to be(false)
          expect(host.instance_variable_get(:@exit_task)).to be_finished
          expect(session.instance_variable_get(:@state)).to eq(:closed)
          expect(session.instance_variable_get(:@lifetime)).to be_finished
          expect(host.wait).to be_nil
        end
      ensure
        if starter&.finished?
          host&.close(grace: 0)
        elsif host
          # A startup join cycle has no timer, and reactor teardown skips transient
          # tasks. Cancelling its shared cleanup lets this failure report.
          AsyncHelpers::INTERNAL.cancel(host_session(host).instance_variable_get(:@cleanup))
        end
      end
    end
  end

  %i[ephemeral fixed].each do |port_mode|
    it "rejects an overlapping start during a yielding real #{port_mode}-port bind without disturbing the first" do
      reactor do |task|
        port = 0
        if port_mode == :fixed
          probe = Addrinfo.tcp("127.0.0.1", 0).bind
          port = probe.local_address.ip_port
          probe.close
        end
        host = described_class.new(server: server, port: port)
        entered = Async::Queue.new
        release = Async::Queue.new
        endpoints = []
        real_bind = host.method(:bind)
        host.define_singleton_method(:bind) do
          bound = real_bind.call
          endpoints << bound
          if endpoints.size == 1
            entered.enqueue(true)
            release.dequeue
          end
          bound
        end
        first = task.async(finished: false) do
          host.start
        rescue StandardError => e
          e
        end
        entered.dequeue
        second = begin
          host.start
        rescue StandardError => e
          e
        end
        during = { binds: endpoints.size, starting: host.instance_variable_get(:@starting),
                   exit_task: host.instance_variable_get(:@exit_task),
                   session: host_session(host).instance_variable_get(:@state) }
        release.enqueue(true)
        outcome = task.with_timeout(2) { first.wait }
        served = nil
        if outcome.equal?(host)
          origin = "http://127.0.0.1:#{host.port}"
          transport = ConnectRPC::Async::Transport.new(origin: origin)
          served = client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "overlap")).message.name
          transport.close(grace: 0)
        end
        closed = host.close(grace: 0)
        observed = {
          second: [second.class, second.respond_to?(:message) ? second.message : second], during: during,
          first: outcome.equal?(host) ? :host : [outcome.class, outcome.message], served: served,
          port: port.zero? ? :ephemeral : host.port == port,
          listeners_finished: host.instance_variable_get(:@listeners).all?(&:finished?),
          sockets_closed: endpoints.flat_map(&:sockets).all?(&:closed?),
          closed: closed, session: host_session(host).instance_variable_get(:@state)
        }
        expect(observed).to eq(
          second: [AsyncHelpers::LIFECYCLE_ERROR, "Host startup is already in progress"],
          during: { binds: 1, starting: true, exit_task: nil, session: :running },
          first: :host, served: "overlap", port: port.zero? ? :ephemeral : true,
          listeners_finished: true, sockets_closed: true, closed: true, session: :closed
        )
      ensure
        release&.enqueue(true)
        AsyncHelpers::INTERNAL.cancel(first)
        AsyncHelpers::INTERNAL.join_complete(first) if first
        host&.close(grace: 0)
        endpoints&.each(&:close)
      end
    end
  end

  it "keeps start idempotent when a running provider refresh calls it" do
    reactor do
      host = nil
      inner = []
      recurse = false
      host = described_class.new(server: server, tls_provider: lambda {
        inner << host.start if recurse
        host_tls.merge(version: "one")
      }).start
      recurse = true
      expect(host.refresh_tls).to eq(1)
      expect(inner.size).to eq(1)
      expect(inner.first).to equal(host)
      expect(host.instance_variable_get(:@exit_task)).to be_nil
      expect(host.tls_status[:state]).to eq(:running)
    ensure
      host&.close(grace: 0)
    end
  end
end
