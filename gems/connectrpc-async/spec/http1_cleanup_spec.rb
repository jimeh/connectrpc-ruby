# frozen_string_literal: true

require_relative "support/http1_client_helpers"

RSpec.describe ConnectRPC::Async::HTTP1Transport, "cleanup ownership" do
  include HTTP1ClientHelpers

  def cleanup_lifecycle(&)
    observer = Object.new
    observer.define_singleton_method(:attach) { nil }
    observer.define_singleton_method(:detach, &)
    subscriber = Object.new
    subscriber.define_singleton_method(:start) { |_| observer }
    ConnectRPC::Instrumentation.new(subscribers: [subscriber])
                               .start(owner: self, role: :client, method: nil, carrier: {})
  end

  %i[complete caller_cancel caller_fatal concurrent_abort owner_abort owner_cancel].each do |stop|
    it "holds exclusive ownership through yielding lifecycle cleanup on #{stop}" do
      with_http1_peer(response: http1_wire, connection_limit: 1) do |transport, origin, accepted, _requests, task|
        if stop == :owner_cancel
          created = Async::Promise.new
          owner = task.async do
            created.resolve(described_class.new(origin: origin, connection_limit: 1))
            Async::Condition.new.wait
          end
          transport = created.wait
        end
        entered = Async::Promise.new
        release = Async::Promise.new
        unwinding = Async::Promise.new
        unwind_release = Async::Promise.new
        operation = nil
        exchange = nil
        detached = false
        lifecycle = cleanup_lifecycle do |_token|
          next unless operation && operation[:cleaned]

          entered.resolve(Async::Task.current)
          begin
            release.wait
            detached = true
          ensure
            unless detached
              unwinding.resolve(nil)
              unwind_release.wait
            end
          end
        end
        outgoing = stream_request(origin)
        outgoing.lifecycle = lifecycle
        cancellation = IOError.new("application cancellation cause")
        fatal = Interrupt.new("application fatal exit")
        observed_fatal = nil
        observed_cancel = nil
        caller = task.async(finished: false) do
          transport.open_stream(outgoing) do |active|
            exchange = active
            active.close_send
            expect(active.read(2)).to eq("ok")
            expect(active.read(1)).to be_nil
            operation = active.instance_variable_get(:@operation)
            active.finish(timeout: 1)
          end
        rescue Async::Cancel => e
          observed_cancel = e
          raise
        rescue fatal.class => e
          observed_fatal = e
        rescue ConnectRPC::Error => e
          expect(e.code).to eq("unavailable")
          raise if %i[complete caller_cancel caller_fatal].include?(stop)
        end
        cleanup_child = entered.wait
        resource = operation.fetch(:resource)
        expect(transport.instance_variable_get(:@idle)).to be_empty
        expect(transport.instance_variable_get(:@leases)).to include(resource)
        if stop == :complete
          release.resolve(nil)
        else
          stopper = task.async(finished: false) do
            case stop
            when :caller_cancel then caller.cancel(cause: cancellation)
            when :caller_fatal then Fiber.scheduler.raise(caller.fiber, fatal)
            when :concurrent_abort then exchange.abort
            when :owner_abort then transport.close(grace: 0)
            when :owner_cancel
              owner.cancel
              wait_until(task, "owner cancellation joined cleanup") do
                transport.instance_variable_get(:@state) == :closed
              end
            end
          end
          unwinding.wait
          expect(resource.raw).to be_closed
          expect(transport.instance_variable_get(:@idle)).to be_empty
          expect(transport.instance_variable_get(:@leases)).to include(resource)
          expect(stopper).not_to be_finished
          unwind_release.resolve(nil)
          stopper.wait
        end
        AsyncHelpers::INTERNAL.join(caller)
        expect(cleanup_child).to be_finished
        expect(operation.fetch(:tasks).keys).to all(be_finished)
        expect(operation.fetch(:writer)).to be_finished
        if stop == :complete
          expect(detached).to be(true)
          expect(transport.call(request(origin)).body).to eq("ok")
          expect(accepted.size).to eq(1)
        else
          expect(detached).to be(false)
          expect(resource.raw).to be_closed
          expect(transport.instance_variable_get(:@leases)).to be_empty
          if stop == :caller_cancel
            expect(caller.status).to eq(:cancelled)
            expect(observed_cancel).to be_a(Async::Cancel)
            expect(observed_cancel.cause).to equal(cancellation)
          end
          expect(observed_fatal).to equal(fatal) if stop == :caller_fatal
          if %i[owner_abort owner_cancel].include?(stop)
            expect(transport.instance_variable_get(:@state)).to eq(:closed)
            expect(transport.instance_variable_get(:@lifetime)).to be_finished
          else
            expect(transport.call(request(origin)).body).to eq("ok")
            expect(accepted.size).to eq(2)
          end
        end
      ensure
        release&.resolve(nil)
        unwind_release&.resolve(nil)
        AsyncHelpers::INTERNAL.join(stopper)
        AsyncHelpers::INTERNAL.join(caller)
        AsyncHelpers::INTERNAL.join(owner)
      end
    end
  end

  it "retires and joins cleanup ownership when a fatal cleanup child stops the reactor" do
    fatal = Interrupt.new("fatal observer detach")
    operation = nil
    transport = nil
    expect do
      with_http1_peer(response: http1_wire, connection_limit: 1) do |active, origin|
        transport = active
        outgoing = stream_request(origin)
        outgoing.lifecycle = cleanup_lifecycle { |_| raise fatal if operation && operation[:cleaned] }
        transport.open_stream(outgoing) do |exchange|
          exchange.close_send
          expect(exchange.read(2)).to eq("ok")
          expect(exchange.read(1)).to be_nil
          operation = exchange.instance_variable_get(:@operation)
          exchange.finish
        end
      end
    end.to raise_error(Interrupt) { |error| expect(error).to equal(fatal) }
    expect(operation.fetch(:resource).raw).to be_closed
    expect(operation.fetch(:cleanup_task)).to be_finished
    expect(operation.fetch(:tasks).keys).to all(be_finished)
    expect(operation[:cleanup_complete]).to be(true)
    expect(transport.instance_variable_get(:@idle)).to be_empty
    expect(transport.instance_variable_get(:@leases)).to be_empty
  end

  it "closes admission before owner-exit teardown yields and joins sibling operations before closed" do
    entered = Async::Promise.new
    release = Async::Promise.new
    peer = lambda do |native, *_|
      native.read_request
      entered.resolve(nil)
      release.wait
    end
    with_raw_http1(peer: peer) do |_unused, origin, _accepted, task|
      owned = nil
      created = Async::Promise.new
      owner = task.async do
        owned = described_class.new(origin: origin)
        created.resolve(nil)
        Async::Condition.new.wait
      end
      created.wait
      holder = task.async do
        expect { owned.open_stream(stream_request(origin), &:response_head) }
          .to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("unavailable") }
      end
      entered.wait
      teardown = Async::Promise.new
      proceed = Async::Promise.new
      allow(owned).to receive(:force_operations).and_wrap_original do |original|
        teardown.resolve(nil)
        proceed.wait
        original.call
      end
      owner.cancel
      teardown.wait
      expect(owned.instance_variable_get(:@state)).to eq(:closing)
      expect { owned.call(request(origin)) }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("unavailable") }
      proceed.resolve(nil)
      holder.wait
      wait_until(task, "owner lifetime joined and closed") do
        owned.instance_variable_get(:@state) == :closed && owned.instance_variable_get(:@lifetime).finished?
      end
      AsyncHelpers::INTERNAL.join(owner)
      expect(owned.close).to be(false)
      expect(owned.instance_variable_get(:@calls)).to be_empty
      expect(owned.instance_variable_get(:@leases)).to be_empty
      expect(owned.instance_variable_get(:@idle)).to be_empty
      expect(owned.instance_variable_get(:@resources).all? { |r| r.raw.closed? && r.setup.finished? }).to be(true)
      expect(owned.instance_variable_get(:@lifetime).children.to_a).to all(be_finished)
    ensure
      proceed&.resolve(nil)
      release.resolve(nil)
    end
  end
end
