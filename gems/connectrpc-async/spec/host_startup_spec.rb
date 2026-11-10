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
end
