# frozen_string_literal: true

RSpec.describe "Shared HTTP/2 connection attempts" do
  def path_peer(task, listener, **)
    ConnectionPeer::RecordingServer.new(task, listener, **) do |incoming|
      incoming.send_response(Protocol::HTTP::Response[200, {}, [incoming.path]])
    end
  end

  %i[deadline cancellation].each do |exit|
    it "completes a second waiter on the TLS attempt that a #{exit} initiator left" do
      with_ca do |ca_file|
        reactor do |task|
          bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
          listener = bound.sockets.first
          gate = Async::Queue.new
          tls = AsyncHelpers::INTERNAL::TLS.server(**host_tls)
          peer = path_peer(task, listener, tls: tls, gate: gate)
          origin = "https://127.0.0.1:#{listener.local_address.ip_port}"
          events = []
          transport = recording_transport(origin, events, ca_file: ca_file)
          source = ConnectRPC::CancellationSource.new
          # The deadline exit is real elapsed time, the boundary under test. The
          # join synchronization below proves both callers wait before it expires.
          initiator = request("#{origin}/initiator",
                              deadline: exit == :deadline ? ConnectRPC::Deadline.new(1) : nil)
          initiator.cancellation = source.token if exit == :cancellation
          first = task.async(finished: false) { transport.call(initiator) }
          wait_until(task, "attempt accepted") { peer.accepted == 1 }
          second = task.async { transport.call(request("#{origin}/second")) }
          wait_until(task, "both callers joined") { transport.instance_variable_get(:@acquiring).size == 2 }
          source.cancel
          expect { first.wait }.to raise_error(ConnectRPC::Error) { |error|
            expect(error.code).to eq(exit == :deadline ? "deadline_exceeded" : "canceled")
          }
          resource = transport.instance_variable_get(:@resources).first
          expect(resource.raw).not_to be_closed
          expect(transport.connection_state).to eq(:connecting)
          gate.enqueue(true)
          expect(second.wait.body).to eq("/second")
          expect(peer.accepted).to eq(1)
          expect(peer.requests.dequeue.path).to eq("/second")
          expect(peer.headers_frames.size).to eq(1)
          expect(attempt_outcomes(events)).to eq([:ready])
        ensure
          gate&.enqueue(true)
          transport&.close(grace: 0)
          peer&.stop
          bound&.close
        end
      end
    end
  end

  %i[call grpc].each do |kind|
    it "never lets #{kind} cleanup stop the shared attempt when cancelled right after readiness" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        listener = bound.sockets.first
        peer = path_peer(task, listener)
        origin = "http://127.0.0.1:#{listener.local_address.ip_port}"
        source = ConnectRPC::CancellationSource.new
        supervisor = nil
        transport = nil
        observer = lambda do |event|
          next unless event.type == :attempt_finished

          supervisor = connection_manager(transport).instance_variable_get(:@attempt)&.task || Async::Task.current
          source.cancel
        end
        transport = ConnectRPC::Async::Transport.new(origin: origin, connection_observer: observer)
        expect do
          if kind == :call
            outgoing = request("#{origin}/cancelled")
            outgoing.cancellation = source.token
            transport.call(outgoing)
          else
            transport.open_grpc_stream(ConnectRPC::StreamRequest.new(
                                         url: URI("#{origin}/cancelled"), http_method: "POST",
                                         headers: { "content-type" => ["application/grpc"] }, cancellation: source.token
                                       )) { raise "must not yield" }
          end
        end.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("canceled") }
        wait_until(task, "attempt supervisor finished") { supervisor&.finished? }
        expect(supervisor).not_to be_cancelled
        resource = transport.instance_variable_get(:@resources).first
        expect(resource.task).to be_finished
        expect(resource.task).not_to be_cancelled
        expect(resource.raw).not_to be_closed
        expect(transport.connection_state).to eq(:ready)
        expect(connection_manager(transport).connections.values).to eq([0])
        expect(transport.call(request("#{origin}/next")).body).to eq("/next")
        expect(peer.accepted).to eq(1)
        expect(peer.requests.dequeue.path).to eq("/next")
      ensure
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end
  end

  it "uses an attempt that succeeds before its initiator waits" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      listener = bound.sockets.first
      peer = path_peer(task, listener)
      origin = "http://127.0.0.1:#{listener.local_address.ip_port}"
      events = []
      transport = recording_transport(origin, events)
      spare = transport.send(:build_resource)
      transport.send(:setup_connection, spare)
      transport.instance_variable_get(:@resources).delete(spare)
      # Adopt an established connection without yielding, so the eager attempt
      # finishes inside its own first run, before the initiator waits.
      connection_manager(transport).instance_variable_set(:@setup, lambda do |resource|
        resource.raw = spare.raw
        resource.socket = spare.socket
        resource.connection = spare.connection
        spare.connection.resource = resource
        resource.ready = true
      end)
      expect(transport.call(request("#{origin}/immediate")).body).to eq("/immediate")
      expect(attempt_outcomes(events)).to eq([:ready])
      # The initiator's published pending call ends with its acquisition.
      expect(events.last.type).to eq(:counts_changed)
      expect([events.last.active, events.last.draining, events.last.pending_calls]).to eq([1, 0, 0])
      expect(peer.accepted).to eq(1)
    ensure
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  [nil, ConnectRPC::Async::Backoff.new].each do |backoff|
    it "fails callers of an attempt that fails before they wait with backoff #{backoff ? 'enabled' : 'disabled'}" do
      reactor do
        events = []
        policy = ConnectRPC::Async::ConnectionPolicy.new(backoff: backoff)
        transport = recording_transport("http://127.0.0.1:1", events, connection_policy: policy)
        failure = Errno::ECONNREFUSED.new("immediate")
        connection_manager(transport).instance_variable_set(:@setup, ->(_) { raise failure })
        install_clock(transport) if backoff
        failures = []
        instrumentation = ConnectRPC::Instrumentation.new(subscribers: [Object.new.tap do |subscriber|
          observer = Object.new
          observer.define_singleton_method(:processing_finished) { |outcome| failures << outcome.error }
          subscriber.define_singleton_method(:start) { |_| observer }
        end])
        typed = client(transport, "http://127.0.0.1:1", instrumentation: instrumentation)
        expect { typed.echo(AsyncHelpers::MESSAGE.new) }.to unavailable_error
        expect(failures).to eq([failure])
        expect(attempt_outcomes(events)).to eq([:refused])
        expect(transport.connection_state).to eq(backoff ? :transient_failure : :idle)
      ensure
        transport&.close(grace: 0)
      end
    end
  end
end
