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

  %i[second_cancel second_fatal cancel_after_budget timeout_after_budget cancel_after_join].each do |stop|
    it "completes disposition and preserves the original interruption on #{stop}" do
      with_http1_peer(response: http1_wire, connection_limit: 1) do |transport, origin, accepted, _requests, task|
        entered = Async::Promise.new
        unwinding = Async::Promise.new
        reentered = Async::Promise.new
        release = Async::Promise.new
        joined = Async::Promise.new
        join_release = Async::Promise.new
        operation = nil
        exchange = nil
        interruptions = []
        observed = nil
        cancellation_count = 0
        cause = IOError.new("first application cancel")
        fatal = Interrupt.new("second owner interruption")
        lifecycle = cleanup_lifecycle do |_token|
          next unless operation && operation[:cleaned]

          entered.resolve(Async::Task.current)
          begin
            Async::Promise.new.wait
          ensure
            unwinding.resolve(nil)
            exchange.abort
            reentered.resolve(nil)
            release.wait
          end
        end
        outgoing = stream_request(origin)
        outgoing.lifecycle = lifecycle
        caller = nil
        trace = TracePoint.new(:raise) do |event|
          error = event.raised_exception
          if Async::Task.current?.equal?(caller) && (error.is_a?(Async::Cancel) || error.equal?(fatal))
            interruptions << error
          end
        end
        trace.enable
        caller = task.async(finished: false) do
          transport.open_stream(outgoing) do |active|
            exchange = active
            active.close_send
            expect(active.read(2)).to eq("ok")
            expect(active.read(1)).to be_nil
            operation = active.instance_variable_get(:@operation)
            active.finish(timeout: stop.to_s.include?("budget") || stop == :cancel_after_join ? 0.03 : 1)
          end
        rescue Async::Cancel, Interrupt, Async::TimeoutError => e
          observed = e
          raise if e.is_a?(Async::Cancel)
        end
        child = entered.wait
        resource = operation.fetch(:resource)
        allow(child).to receive(:cancel).and_wrap_original do |original, *args, **options|
          cancellation_count += 1
          original.call(*args, **options)
        end
        if stop == :cancel_after_join
          # Hold the owner's wait after actual child completion. A finished?
          # check cannot classify an interruption delivered at this boundary.
          allow(child).to receive(:wait).and_wrap_original do |original, *args|
            result = original.call(*args)
            if operation[:retiring] && Async::Task.current.equal?(caller)
              joined.resolve(nil)
              join_release.wait
            end
            result
          end
        end
        caller.cancel(cause: cause) unless stop.to_s.include?("budget") || stop == :cancel_after_join
        unwinding.wait
        reentered.wait
        wait_until(task, "owner entered final cleanup joins") do
          caller.backtrace&.any? { |frame| frame.include?("join_cleanup_task") }
        end
        aborter = task.async(finished: false) { exchange.abort }
        expect(aborter).not_to be_finished
        expect(child).not_to be_finished
        expect(cancellation_count).to eq(1)
        expect(resource.raw).to be_closed
        expect(operation.fetch(:cleanup_done)).not_to be_resolved
        expect(transport.instance_variable_get(:@leases)).to eq([resource])
        expect(transport.instance_variable_get(:@idle)).to be_empty
        if stop == :cancel_after_join
          release.resolve(nil)
          joined.wait
          expect(child).to be_finished
        end
        case stop
        when :second_fatal then Fiber.scheduler.raise(caller.fiber, fatal)
        when :timeout_after_budget
          timeout = Async::TimeoutError.new("owner timeout during final join")
          Fiber.scheduler.raise(caller.fiber, timeout)
        else caller.cancel(cause: IOError.new("later application cancel"))
        end
        expect(caller).not_to be_finished
        expect(transport.instance_variable_get(:@leases)).to eq([resource])
        expect(operation.fetch(:cleanup_done)).not_to be_resolved
        expect(cancellation_count).to eq(1)
        release.resolve(nil)
        join_release.resolve(nil)
        AsyncHelpers::INTERNAL.join(caller)
        aborter.wait
        expect(operation[:cleanup_complete]).to be(true)
        expect(operation.fetch(:cleanup_done)).to be_resolved
        expect(operation.fetch(:tasks).keys).to all(be_finished)
        expect(transport.instance_variable_get(:@leases)).to be_empty
        expect(transport.instance_variable_get(:@calls)).to be_empty
        if stop == :timeout_after_budget
          expect(observed).to equal(timeout)
        else
          expect(observed).to equal(interruptions.first)
          expect(observed).to be_a(Async::Cancel)
          expect(observed.cause).to equal(cause) if %i[second_cancel second_fatal].include?(stop)
          expect(caller.status).to eq(:cancelled)
        end
        expect(transport.call(request(origin)).body).to eq("ok")
        expect(accepted.size).to eq(2)
      ensure
        trace&.disable
        release&.resolve(nil)
        join_release&.resolve(nil)
        AsyncHelpers::INTERNAL.join(caller)
        AsyncHelpers::INTERNAL.join(aborter)
      end
    end
  end

  %i[dispatch setup writer reader].each do |stage|
    it "prevents late work after retirement while #{stage} activation is suspended" do
      with_http1_peer(response: http1_wire, connection_limit: 1) do |transport, origin, accepted, _requests, task|
        entered = Async::Promise.new
        unwinding = Async::Promise.new
        release = Async::Promise.new
        operation = nil
        selected = false
        cancels = 0
        observer = Object.new
        observer.define_singleton_method(:attach) do
          operation = transport.instance_variable_get(:@calls).keys.first
          current = Async::Task.current
          role = if operation[:writer].equal?(current)
                   :writer
                 elsif operation[:resource]&.setup.equal?(current)
                   :setup
                 elsif operation[:connection]
                   :reader
                 else
                   :dispatch
                 end
          next if selected || role != stage

          selected = true
          entered.resolve(current)
          begin
            Async::Promise.new.wait
          rescue Async::Cancel
            # A yielding application attach can finish normally after cancel.
            # The transport must recheck terminal entry before starting I/O.
            unwinding.resolve(nil)
            release.wait
          end
          nil
        end
        observer.define_singleton_method(:detach) { |_| nil }
        subscriber = Object.new
        subscriber.define_singleton_method(:start) { |_| observer }
        outgoing = stream_request(origin)
        outgoing.lifecycle = ConnectRPC::Instrumentation.new(subscribers: [subscriber])
                                                        .start(owner: self, role: :client, method: nil, carrier: {})
        caller = task.async(finished: false) do
          transport.open_stream(outgoing) do |exchange|
            exchange.close_send
            exchange.response_head
          end
        end
        held = entered.wait
        allow(held).to receive(:cancel).and_wrap_original do |original, *args, **options|
          cancels += 1
          original.call(*args, **options)
        end
        caller.cancel
        unwinding.wait
        resource = operation[:resource]
        workers = operation.fetch(:tasks).keys - [operation[:cleanup_task]]
        closer = task.async(finished: false) { transport.close(grace: 0) }
        expect(closer).not_to be_finished
        caller.cancel
        expect(cancels).to eq(1)
        expect(held).not_to be_finished
        expect(operation[:cleanup_complete]).not_to be(true)
        expect(transport.instance_variable_get(:@leases)).to include(resource) if resource
        expect(resource.raw).to be_closed if resource&.raw
        expect(operation[:writer]).to be_nil if %i[dispatch setup].include?(stage)
        release.resolve(nil)
        AsyncHelpers::INTERNAL.join(caller)
        expect(closer.wait).to be(false)
        expect(operation[:cleanup_complete]).to be(true)
        expect(operation.fetch(:tasks).keys - [operation[:cleanup_task]]).to match_array(workers)
        expect(operation.fetch(:tasks).keys).to all(be_finished)
        expect(transport.instance_variable_get(:@leases)).to be_empty
        expect(transport.instance_variable_get(:@calls)).to be_empty
        expect(transport.instance_variable_get(:@lifetime)).to be_finished
        expect(accepted.size).to eq(%i[dispatch setup].include?(stage) ? 0 : 1)
      ensure
        release&.resolve(nil)
        AsyncHelpers::INTERNAL.join(caller)
        AsyncHelpers::INTERNAL.join(closer)
      end
    end
  end

  %i[setup writer reader].each do |stage|
    it "lets #{stage} detach reenter abort while external abort and forced close wait" do
      with_http1_peer(response: http1_wire, connection_limit: 1) do |transport, origin, _accepted, _requests, task|
        entered = Async::Promise.new
        reentered = Async::Promise.new
        release = Async::Promise.new
        operation = nil
        exchange = nil
        selected = false
        cancels = 0
        outgoing = stream_request(origin)
        outgoing.lifecycle = cleanup_lifecycle do |_token|
          operation = transport.instance_variable_get(:@calls).keys.first
          current = Async::Task.current
          role = if operation[:resource]&.setup.equal?(current)
                   :setup
                 elsif operation[:writer].equal?(current)
                   :writer
                 elsif exchange && !operation[:cleanup_task].equal?(current)
                   :reader
                 end
          next if selected || role != stage

          selected = true
          entered.resolve(current)
          begin
            Async::Promise.new.wait
          ensure
            # Setup precedes public exchange exposure; this is the same abort
            # entry used by HTTP1Exchange once the writer exposes the exchange.
            transport.send(:abort, operation)
            reentered.resolve(nil)
            release.wait
          end
        end
        caller = task.async(finished: false) do
          transport.open_stream(outgoing) do |active|
            exchange = active
            active.close_send
            active.response_head
          end
        end
        held = entered.wait
        allow(held).to receive(:cancel).and_wrap_original do |original, *args, **options|
          cancels += 1
          original.call(*args, **options)
        end
        caller.cancel
        reentered.wait
        aborter = task.async(finished: false) { transport.send(:abort, operation) }
        closer = task.async(finished: false) { transport.close(grace: 0) }
        caller.cancel
        expect(cancels).to eq(1)
        expect(held).not_to be_finished
        expect(aborter).not_to be_finished
        expect(closer).not_to be_finished
        expect(operation.fetch(:resource).raw).to be_closed
        expect(operation.fetch(:cleanup_done)).not_to be_resolved
        expect(transport.instance_variable_get(:@leases)).to eq([operation.fetch(:resource)])
        release.resolve(nil)
        AsyncHelpers::INTERNAL.join(caller)
        aborter.wait
        expect(closer.wait).to be(false)
        expect(operation[:cleanup_complete]).to be(true)
        expect(operation.fetch(:tasks).keys).to all(be_finished)
        expect(transport.instance_variable_get(:@leases)).to be_empty
        expect(transport.instance_variable_get(:@calls)).to be_empty
        expect(transport.instance_variable_get(:@lifetime)).to be_finished
      ensure
        release&.resolve(nil)
        AsyncHelpers::INTERNAL.join(caller)
        AsyncHelpers::INTERNAL.join(aborter)
        AsyncHelpers::INTERNAL.join(closer)
      end
    end
  end

  it "allows an owned setup callback to request retirement before an external cleanup owner exists" do
    with_http1_peer(response: http1_wire, connection_limit: 1) do |transport, origin, accepted|
      operation = nil
      retired = false
      cleaned_at_retirement = nil
      outgoing = stream_request(origin)
      outgoing.lifecycle = cleanup_lifecycle do |_token|
        operation = transport.instance_variable_get(:@calls).keys.first
        next if retired || !operation[:resource]&.setup.equal?(Async::Task.current)

        cleaned_at_retirement = operation[:cleaned]
        retired = true
        transport.send(:abort, operation)
      end
      expect { transport.open_stream(outgoing) { raise "Retired setup was handed off" } }
        .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("unavailable") }
      expect(retired).to be(true)
      expect(cleaned_at_retirement).not_to be(true)
      expect(operation[:writer]).to be_nil
      expect(operation[:cleanup_complete]).to be(true)
      expect(operation.fetch(:tasks).keys).to all(be_finished)
      expect(transport.instance_variable_get(:@leases)).to be_empty
      expect(transport.call(request(origin)).body).to eq("ok")
      expect(accepted.size).to eq(2)
    end
  end

  it "finishes every operation and closes after repeated lifetime-owner cancellation during forced joins" do
    with_http1_peer(response: http1_wire, connection_limit: 2) do |_unused, origin, _accepted, _requests, task|
      created = Async::Promise.new
      owner = task.async do
        created.resolve(described_class.new(origin: origin, connection_limit: 2))
        Async::Condition.new.wait
      end
      transport = created.wait
      entered = Async::Queue.new
      release = Async::Promise.new
      operations = []
      callers = 2.times.map do
        outgoing = stream_request(origin)
        operation = nil
        outgoing.lifecycle = cleanup_lifecycle do |_token|
          next unless operation && operation[:cleaned]

          entered.enqueue(Async::Task.current)
          begin
            release.wait
          ensure
            release.wait
          end
        end
        task.async(finished: false) do
          transport.open_stream(outgoing) do |exchange|
            exchange.close_send
            exchange.read(2)
            exchange.read(1)
            operation = exchange.instance_variable_get(:@operation)
            operations << operation
            # Keep application ownership outside cleanup until owner exit.
            Async::Promise.new.wait
          end
        end
      end
      wait_until(task, "two fully read operations") { operations.size == 2 }
      owner.cancel
      entered.dequeue
      lifetime = transport.instance_variable_get(:@lifetime)
      # The lifetime itself is now the first operation's cleanup owner.
      expect(operations.first[:cleanup_owner]).to equal(lifetime)
      lifetime.cancel
      lifetime.cancel
      expect(lifetime).not_to be_finished
      expect(transport.instance_variable_get(:@leases).size).to eq(2)
      release.resolve(nil)
      wait_until(task, "all forced joins and closed state") do
        transport.instance_variable_get(:@state) == :closed && lifetime.finished?
      end
      expect(operations).to all(include(cleanup_complete: true))
      expect(operations.flat_map { |operation| operation.fetch(:tasks).keys }).to all(be_finished)
      expect(transport.instance_variable_get(:@leases)).to be_empty
      expect(transport.close).to be(false)
      callers.each(&:cancel)
      callers.each { |caller| AsyncHelpers::INTERNAL.join(caller) }
      expect(transport.instance_variable_get(:@calls)).to be_empty
    ensure
      release&.resolve(nil)
      callers&.each(&:cancel)
      callers&.each { |caller| AsyncHelpers::INTERNAL.join(caller) }
      AsyncHelpers::INTERNAL.join(owner)
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
