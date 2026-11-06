# frozen_string_literal: true

require_relative "support/http1_client_helpers"

RSpec.describe ConnectRPC::Async::HTTP1Transport, "retirement boundaries" do
  include HTTP1ClientHelpers

  %i[acquire setup].each do |stage|
    it "rejects #{stage} after a yielding cancellation query outlives retirement" do
      options = { response: http1_wire, tls: stage == :setup, connection_limit: 1 }
      with_http1_peer(**options) do |transport, origin, accepted, requests, task|
        entered = Async::Promise.new
        unwinding = Async::Promise.new
        release = Async::Promise.new
        token = ConnectRPC::CancellationSource.new.token
        source = Object.new
        selected = false
        operation = nil
        source.define_singleton_method(:reason) do
          current = transport.instance_variable_get(:@calls).keys.first
          target = if stage == :acquire
                     caller_locations.any? { |frame| frame.base_label == "acquire" }
                   else
                     current && current[:resource]&.setup.equal?(Async::Task.current)
                   end
          if !selected && target
            selected = true
            operation = current
            entered.resolve(Async::Task.current)
            begin
              Async::Promise.new.wait
            rescue Async::Cancel
              unwinding.resolve(nil)
              release.wait
            end
          end
          nil
        end
        source.define_singleton_method(:cancelled?) { false }
        source.define_singleton_method(:check!) { nil }
        source.define_singleton_method(:on_cancel) { |&callback| token.on_cancel(&callback) }
        caller = task.async(finished: false) do
          transport.open_stream(stream_request(origin, cancellation: source)) { raise "Retired call handed off" }
        end
        held = entered.wait
        resource = operation[:resource]
        expect(resource&.raw).to be_nil
        caller.cancel
        unwinding.wait
        if stage == :setup
          wait_until(task, "owner reached final join after actual cleanup expiry") do
            caller.backtrace&.any? { |frame| frame.include?("join_cleanup_task") }
          end
        end
        expect(operation[:cleaned]).to be(true)
        expect(operation.fetch(:cleanup_done)).not_to be_resolved
        release.resolve(nil)
        AsyncHelpers::INTERNAL.join(caller)
        expect(caller).to be_finished
        expect(held).to be_finished
        expect(operation.fetch(:tasks).keys).to all(be_finished)
        expect(operation).to include(cleanup_complete: true)
        expect(operation.fetch(:cleanup_done)).to be_resolved
        expect(operation[:resource]).to equal(resource)
        expect(resource&.raw).to be_nil
        expect(transport.instance_variable_get(:@leases)).to be_empty
        expect(transport.instance_variable_get(:@calls)).to be_empty
        expect(transport.instance_variable_get(:@idle)).to be_empty
        expect(accepted).to be_empty
        expect(requests).to be_empty
        expect(transport.call(request(origin, deadline: ConnectRPC::Deadline.new(1))).body).to eq("ok")
        expect(accepted.size).to eq(1)
        actual = transport.instance_variable_get(:@resources).dup
        expect(transport.close).to be(true)
        expect(actual.flat_map { |owned| [owned.raw, owned.socket] }.compact).to all(be_closed)
      ensure
        release&.resolve(nil)
        AsyncHelpers::INTERNAL.join(caller)
        AsyncHelpers::INTERNAL.raw_close(operation&.dig(:resource)&.raw)
        operation&.dig(:resource)&.close
      end
    end
  end

  %i[check_request on_cancel].each do |boundary|
    it "rejects an operation launch after #{boundary} yields across external retirement" do
      with_http1_peer(response: http1_wire, connection_limit: 1) do |transport, origin, accepted, requests, task|
        entered = Async::Promise.new
        release = Async::Promise.new
        token = ConnectRPC::CancellationSource.new.token
        source = Object.new
        operation = nil
        selected = false
        source.define_singleton_method(:reason) do
          frames = caller_locations.map(&:base_label)
          if !selected && frames.include?("operate") && frames.include?("on_cancel") == (boundary == :on_cancel)
            selected = true
            operation = transport.instance_variable_get(:@calls).keys.first
            entered.resolve(nil)
            release.wait
          end
          nil
        end
        source.define_singleton_method(:cancelled?) { false }
        source.define_singleton_method(:check!) { nil }
        source.define_singleton_method(:on_cancel) { |&callback| token.on_cancel(&callback) }
        late_activations = []
        observer = Object.new
        observer.define_singleton_method(:attach) do
          late_activations << Async::Task.current if operation && operation[:cleanup_complete]
          nil
        end
        observer.define_singleton_method(:detach) { |_| nil }
        subscriber = Object.new
        subscriber.define_singleton_method(:start) { |_| observer }
        outgoing = stream_request(origin, cancellation: source)
        outgoing.lifecycle = ConnectRPC::Instrumentation.new(subscribers: [subscriber])
                                                        .start(owner: self, role: :client, method: nil, carrier: {})
        caller = task.async(finished: false) do
          transport.open_stream(outgoing) { raise "Retired launch handed off" }
        rescue ConnectRPC::Error => e
          expect(e.code).to eq("unavailable")
        end
        entered.wait
        transport.send(:abort, operation)
        workers = operation.fetch(:tasks).keys
        expect(operation[:cleanup_complete]).to be(true)
        release.resolve(nil)
        caller.wait
        expect(caller).to be_finished
        expect(late_activations).to be_empty
        expect(operation.fetch(:tasks).keys).to eq(workers)
        expect(operation.fetch(:tasks).keys).to all(be_finished)
        expect(operation.fetch(:events)).to be_empty
        expect(accepted).to be_empty
        expect(requests).to be_empty
        expect(transport.instance_variable_get(:@calls)).to be_empty
        expect(transport.instance_variable_get(:@leases)).to be_empty
        expect(transport.call(request(origin)).body).to eq("ok")
      ensure
        release&.resolve(nil)
        AsyncHelpers::INTERNAL.join(caller)
      end
    end
  end

  %i[native gated_active gated_idle].each do |schedule|
    it "disposes active and idle TCP resources through #{schedule} forced raw close interruption" do
      hang = Async::Promise.new
      peer = lambda do |native, socket, _raw, index|
        if index == 1
          native.read_request
          hang.wait
        else
          loop do
            incoming = native.read_request
            break unless incoming

            incoming.last&.each { |_| nil }
            socket.write(http1_wire)
            native.send_end_stream!
          end
        end
      end
      with_raw_http1(peer: peer) do |_unused, origin, accepted, task|
        created = Async::Promise.new
        owner = task.async do
          created.resolve(described_class.new(origin: origin, connection_limit: 2))
          Async::Condition.new.wait
        end
        transport = created.wait
        lifetime = transport.instance_variable_get(:@lifetime)
        caller = task.async(finished: false) do
          transport.open_stream(stream_request(origin)) do |exchange|
            exchange.close_send
            exchange.response_head
          end
        rescue ConnectRPC::Error, Async::Cancel
          nil
        end
        operation = nil
        wait_until(task, "reader blocked on active TCP socket") do
          operation = transport.instance_variable_get(:@calls).keys.first
          operation && operation.fetch(:tasks).keys.any? do |owned|
            owned.alive? && owned.backtrace&.any? { |frame| frame.include?("read_head") }
          end
        end
        active = operation.fetch(:resource)
        active_fd = active.raw.fileno
        expect(transport.call(request(origin)).body).to eq("ok")
        idle = transport.instance_variable_get(:@idle).first
        expect(transport.instance_variable_get(:@leases)).to eq([active, idle])
        expect(accepted.size).to eq(2)
        closing = Async::Promise.new
        release = Async::Promise.new
        inside = false
        boundary_resource = schedule == :gated_idle ? idle : active
        allow(boundary_resource.raw).to receive(:close).and_wrap_original do |original|
          inside = true
          closing.resolve(Async::Task.current)
          begin
            release.wait if schedule != :native
            original.call
          ensure
            inside = false
          end
        end
        yielded = nil
        interrupter = task.async(finished: false) do
          forcing = closing.wait
          yielded = inside && forcing.equal?(lifetime)
          if yielded
            lifetime.cancel
            if schedule != :native
              expect(transport.instance_variable_get(:@state)).to eq(:closing)
              expect(lifetime).not_to be_finished
              expect(boundary_resource).not_to be_finished
              expect(transport.instance_variable_get(:@leases)).to include(boundary_resource)
              lifetime.cancel
              expect(lifetime).not_to be_finished
            end
          end
          release.resolve(nil)
        end
        owner.cancel
        wait_until(task, "forced lifetime finished after disposition") do
          transport.instance_variable_get(:@state) == :closed && lifetime.finished?
        end
        AsyncHelpers::INTERNAL.join(caller)
        interrupter.wait
        expect(yielded).to eq(schedule != :native || RUBY_VERSION.start_with?("4."))
        expect(operation.fetch(:cleanup_owner)).to be_finished
        expect(operation).to include(cleanup_complete: true)
        expect(operation.fetch(:tasks).keys).to all(be_finished)
        expect([caller, owner, lifetime, interrupter]).to all(be_finished)
        expect([active, idle]).to all(be_finished)
        expect([active.raw, idle.raw, active.socket, idle.socket]).to all(be_closed)
        # Ruby 4 marks IO closed before waiting. Check actual descriptor disposal,
        # before any new call could reuse its number, as well as Ruby's flag.
        expect { IO.for_fd(active_fd, autoclose: false) }.to raise_error(Errno::EBADF)
        expect(transport.instance_variable_get(:@idle)).to be_empty
        expect(transport.instance_variable_get(:@leases)).to be_empty
        expect(transport.instance_variable_get(:@calls)).to be_empty
        expect(transport.close).to be(false)
        healthy = described_class.new(origin: origin)
        expect(healthy.call(request(origin)).body).to eq("ok")
        healthy.close
      ensure
        release&.resolve(nil)
        hang.resolve(nil)
        AsyncHelpers::INTERNAL.join(interrupter)
        AsyncHelpers::INTERNAL.join(caller)
        AsyncHelpers::INTERNAL.join(owner)
        transport&.close(grace: 0)
        AsyncHelpers::INTERNAL.raw_close(idle&.raw)
      end
    end
  end
  %i[owner_begin owner_ensure external_waiter].each do |boundary|
    it "retains interruptions and ownership through #{boundary} raw disposal" do
      with_http1_peer(response: http1_wire, connection_limit: 1) do |transport, origin, accepted, _requests, task|
        prepared = Async::Promise.new
        command = Async::Promise.new
        detached = Async::Promise.new
        detach_release = Async::Promise.new
        closing = Async::Promise.new
        release = Async::Promise.new
        observer = Object.new
        operation = nil
        observer.define_singleton_method(:attach) { nil }
        observer.define_singleton_method(:detach) do |_token|
          next unless boundary != :owner_begin && operation && operation[:cleanup_task].equal?(Async::Task.current)

          detached.resolve(nil)
          begin
            detach_release.wait
          ensure
            detach_release.wait
          end
        end
        subscriber = Object.new
        subscriber.define_singleton_method(:start) { |_| observer }
        outgoing = stream_request(origin)
        outgoing.lifecycle = ConnectRPC::Instrumentation.new(subscribers: [subscriber])
                                                        .start(owner: self, role: :client, method: nil, carrier: {})
        cause = IOError.new("first raw boundary interruption")
        interruptions = []
        observed = nil
        target = nil
        trace = TracePoint.new(:raise) do |event|
          error = event.raised_exception
          interruptions << error if Async::Task.current?.equal?(target) && error.is_a?(Async::Cancel)
        end
        trace.enable
        caller = task.async(finished: false) do
          transport.open_stream(outgoing) do |exchange|
            exchange.close_send
            expect(exchange.read(2)).to eq("ok")
            expect(exchange.read(1)).to be_nil
            operation = exchange.instance_variable_get(:@operation)
            prepared.resolve(exchange)
            command.wait
            if boundary == :owner_begin
              exchange.abort
            else
              exchange.finish(timeout: boundary == :owner_ensure ? 0.03 : 1)
            end
          end
        rescue Async::Cancel => e
          observed = e
          raise
        rescue ConnectRPC::Error => e
          raise unless boundary == :external_waiter && e.code == "unavailable"
        end
        exchange = prepared.wait
        resource = operation.fetch(:resource)
        descriptor = resource.raw.fileno
        allow(resource.raw).to receive(:close).and_wrap_original do |original|
          closing.resolve(Async::Task.current)
          release.wait
          original.call
        end
        command.resolve(nil)
        if boundary == :external_waiter
          detached.wait
          aborter = task.async(finished: false) do
            exchange.abort
          rescue Async::Cancel => e
            observed = e
            raise
          end
        end
        target = closing.wait
        expect(target).to equal(boundary == :external_waiter ? aborter : caller)
        expect(operation[:cleaned]).to be(true)
        target.cancel(cause: cause)
        target.cancel(cause: IOError.new("later raw boundary interruption"))
        expect(target).not_to be_finished
        expect(resource).not_to be_finished
        expect(operation.fetch(:cleanup_done)).not_to be_resolved
        expect(transport.instance_variable_get(:@leases)).to eq([resource])
        expect(transport.instance_variable_get(:@idle)).to be_empty
        release.resolve(nil)
        if boundary == :external_waiter
          wait_until(task, "external waiter reached cleanup completion wait") do
            aborter.backtrace&.any? { |frame| frame.include?("cleanup") } && resource.raw.closed?
          end
          expect(aborter).not_to be_finished
          expect(operation.fetch(:cleanup_done)).not_to be_resolved
        end
        detach_release.resolve(nil)
        AsyncHelpers::INTERNAL.join(caller)
        AsyncHelpers::INTERNAL.join(aborter)
        expect(observed).to equal(interruptions.first)
        expect(observed.cause).to equal(cause)
        expect(target.status).to eq(:cancelled)
        expect(operation[:cleanup_complete]).to be(true)
        expect(operation.fetch(:cleanup_done)).to be_resolved
        expect(operation.fetch(:tasks).keys).to all(be_finished)
        expect([caller, aborter].compact).to all(be_finished)
        expect(resource).to be_finished
        expect([resource.raw, resource.socket]).to all(be_closed)
        expect { IO.for_fd(descriptor, autoclose: false) }.to raise_error(Errno::EBADF)
        expect(transport.instance_variable_get(:@calls)).to be_empty
        expect(transport.instance_variable_get(:@leases)).to be_empty
        expect(transport.instance_variable_get(:@idle)).to be_empty
        expect(transport.call(request(origin)).body).to eq("ok")
        expect(accepted.size).to eq(2)
      ensure
        trace&.disable
        command&.resolve(nil)
        release&.resolve(nil)
        detach_release&.resolve(nil)
        AsyncHelpers::INTERNAL.join(caller)
        AsyncHelpers::INTERNAL.join(aborter)
      end
    end
  end
end
