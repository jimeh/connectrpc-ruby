# frozen_string_literal: true

RSpec.describe "Explicit HTTP/2 unary cancellation" do
  %i[h2c tls mtls].product([false, true]).each do |mode, foreign|
    it "cancels #{mode} from #{foreign ? 'a foreign thread' : 'the owner reactor'} and preserves a sibling and pool" do
      with_ca do |ca_file|
        reactor do |task|
          entered = Async::Queue.new
          release = Async::Queue.new
          contexts = {}
          host = ConnectRPC::Async::Host.new(server: server(echo: lambda { |message, context|
            contexts[message.name] = context
            entered.enqueue(message.name)
            release.dequeue
            context.check!
            message
          }), tls: if mode == :h2c
                     nil
                   else
                     host_tls(anchors: mode == :mtls ? [tls_material.fetch(:client_ca)] : nil)
                   end).start
          origin = "#{mode == :h2c ? 'http' : 'https'}://127.0.0.1:#{host.port}"
          options = mode == :h2c ? {} : { ca_file: ca_file }
          options.merge!(host_tls(tls_material.fetch(:identity)).except(:client_ca)) if mode == :mtls
          transport = ConnectRPC::Async::Transport.new(origin: origin, **options)
          typed = client(transport, origin)
          source = ConnectRPC::CancellationSource.new
          sibling = task.async { typed.echo(AsyncHelpers::MESSAGE.new(name: "sibling"), timeout: nil) }
          expect(entered.dequeue).to eq("sibling")
          call = task.async(finished: false) do
            typed.echo(AsyncHelpers::MESSAGE.new(name: "cancel"), timeout: foreign ? nil : 30,
                                                                  cancellation: source.token)
          end
          expect(entered.dequeue).to eq("cancel")
          resource = transport.instance_variable_get(:@resources).first
          foreign ? Thread.new { source.cancel }.value : source.cancel
          expect { call.wait }.to raise_error(ConnectRPC::Error) { |e| expect([e.code, e.cause]).to eq(["canceled", nil]) }
          wait_until(task, "actual peer CANCEL reached Context") { contexts.fetch("cancel").cancelled? }
          expect(contexts.fetch("cancel").cancellation_reason).to eq(:canceled)
          expect(contexts.fetch("sibling")).not_to be_cancelled
          release.enqueue(true, true)
          expect(sibling.wait.message.name).to eq("sibling")
          next_call = task.async(finished: false) { typed.echo(AsyncHelpers::MESSAGE.new(name: "next"), cancellation: source.token) }
          expect { next_call.wait }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("canceled") }
          expect(resource.raw).not_to be_closed
          expect(lease_usage(transport)).to eq(0)
          expect(transport.instance_variable_get(:@calls)).to be_empty
          expect(source.instance_variable_get(:@signal).instance_variable_get(:@registrations)).to be_empty
        ensure
          release&.enqueue(true, true)
          transport&.close(grace: 0)
          host&.close(grace: 0)
        end
      end
    end
  end
end

RSpec.describe "HTTP/2 cancellation before dispatch and during cleanup" do
  it "cancels a connection-guard waiter without late dispatch and reuses the connection" do
    reactor do |task|
      observed = []
      host = ConnectRPC::Async::Host.new(server: server(echo: lambda { |message, _|
        observed << message.name
        message
      })).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      typed = client(transport, origin)
      typed.echo(AsyncHelpers::MESSAGE.new(name: "warm"))
      connection = transport.instance_variable_get(:@resources).first.connection
      entered = Async::Queue.new
      release = Async::Queue.new
      holder = task.async do
        connection.synchronize do
          entered.enqueue(true)
          release.dequeue
        end
      end
      entered.dequeue
      source = ConnectRPC::CancellationSource.new
      pending = task.async(finished: false) do
        typed.echo(AsyncHelpers::MESSAGE.new(name: "cancel"), timeout: nil, cancellation: source.token)
      end
      wait_until(task, "call waiting on the connection guard") { guard_waiters(connection) == 1 }
      Thread.new { source.cancel }.value
      expect { pending.wait }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("canceled") }
      expect(holder).not_to be_finished
      release.enqueue(true)
      holder.wait
      expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "after")).message.name).to eq("after")
      expect(observed).to eq(%w[warm after])
      expect(transport.instance_variable_get(:@resources).size).to eq(1)
    ensure
      release&.enqueue(true)
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  %i[close connect_timeout].each do |retirement|
    it "wakes a TLS setup waiter from a foreign thread and retires the shared attempt at #{retirement}" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        listener = bound.sockets.first
        entered = Async::Queue.new
        events = Async::Queue.new
        peer = task.async do
          raw, = listener.accept
          expect(raw.readpartial(1024)).not_to be_empty
          entered.enqueue(true)
          expect(raw.read).to eq("")
        ensure
          raw&.close
        end
        origin = "https://127.0.0.1:#{listener.local_address.ip_port}"
        transport = ConnectRPC::Async::Transport.new(
          origin: origin, connect_timeout: retirement == :connect_timeout ? 0.3 : 10,
          connection_observer: ->(event) { events.enqueue(event) if event.type == :attempt_finished }
        )
        source = ConnectRPC::CancellationSource.new
        pending = task.async(finished: false) do
          client(transport, origin).echo(AsyncHelpers::MESSAGE.new, timeout: nil, cancellation: source.token)
        end
        entered.dequeue
        Thread.new { source.cancel }.value
        expect { pending.wait }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("canceled") }
        expect(transport.instance_variable_get(:@calls)).to be_empty
        expect(transport.instance_variable_get(:@acquiring)).to be_empty
        # The leaving caller never owned the shared attempt; it keeps its socket.
        resource = transport.instance_variable_get(:@resources).first
        expect(resource.raw).not_to be_closed
        expect(resource.task).not_to be_finished
        expect(transport.connection_state).to eq(:connecting)
        if retirement == :close
          # Close cancels the attempt; with no admitted call it is graceful.
          expect(transport.close(grace: 1)).to be(true)
          expect(events).to be_empty
        else
          # The real connect_timeout is the external boundary under test.
          expect(events.dequeue.outcome).to eq(:timeout)
          expect(transport.connection_state).to eq(:transient_failure)
        end
        peer.wait
        expect(transport.instance_variable_get(:@resources).map(&:raw)).to all(be_closed)
        expect(transport.instance_variable_get(:@resources).map(&:task)).to all(be_finished)
      ensure
        transport&.close(grace: 0)
        peer&.cancel
        bound&.close
      end
    end
  end

  it "finishes an already-started partial frame within the single cleanup budget and keeps its sibling alive" do
    reactor do |task|
      entered = Async::Queue.new
      release_handler = Async::Queue.new
      host = ConnectRPC::Async::Host.new(server: server(echo: lambda { |message, _|
        if message.name == "sibling"
          entered.enqueue(true)
          release_handler.dequeue
        end
        message
      })).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      typed = client(transport, origin)
      sibling = task.async { typed.echo(AsyncHelpers::MESSAGE.new(name: "sibling"), timeout: nil) }
      entered.dequeue
      resource = transport.instance_variable_get(:@resources).first
      gate = FrameGate.new(resource.socket)
      resource.connection.stream.instance_variable_set(:@io, gate)
      gate.armed = true
      source = ConnectRPC::CancellationSource.new
      partial = task.async(finished: false) do
        typed.echo(AsyncHelpers::MESSAGE.new(name: "partial"), timeout: nil, cancellation: source.token)
      end
      gate.started.dequeue
      source.cancel
      wait_until(task, "partial frame cancellation deferred") do
        transport.instance_variable_get(:@calls).keys.any? { |operation| operation[:task]&.cancel_deferred? }
      end
      gate.release.enqueue(true)
      expect { partial.wait }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("canceled") }
      release_handler.enqueue(true)
      expect(sibling.wait.message.name).to eq("sibling")
      expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "after")).message.name).to eq("after")
      expect(resource.raw).not_to be_closed
      expect(transport.instance_variable_get(:@resources)).to eq([resource])
      expect(lease_usage(transport)).to eq(0)
    ensure
      gate&.release&.enqueue(true)
      release_handler&.enqueue(true)
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end
end

RSpec.describe "Promise wake and arbitration" do
  it "accepts a directly observed source even when the response event wins while source notification is delayed" do
    reactor do
      host = ConnectRPC::Async::Host.new(server: server).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      source = ConnectRPC::CancellationSource.new
      entered = Queue.new
      release = Queue.new
      source.token.on_cancel do
        entered << true
        release.pop
      end
      worker = nil
      allow(transport).to receive(:exchange).and_wrap_original do |exchange, operation|
        result = exchange.call(operation)
        worker = Thread.new { source.cancel }
        expect(entered.pop(timeout: 1)).to be(true)
        result
      end
      expect do
        client(transport, origin).echo(AsyncHelpers::MESSAGE.new, timeout: nil, cancellation: source.token)
      end.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("canceled") }
      release << true
      expect(worker.join(1)).to equal(worker)
      expect(transport.instance_variable_get(:@calls)).to be_empty
      expect(lease_usage(transport)).to eq(0)
      expect(source.instance_variable_get(:@signal).instance_variable_get(:@registrations)).to be_empty
    ensure
      release&.push(true)
      worker&.join(1)
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end
end

RSpec.describe "Explicit HTTP/2 pool-wait cancellation" do
  it "retires a pool waiter without late dispatch or disturbing the admitted call" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      observed = []
      host = ConnectRPC::Async::Host.new(max_requests: 1, server: server(echo: lambda { |message, _|
        observed << message.name
        if message.name == "hold"
          entered.enqueue(true)
          release.dequeue
        end
        message
      })).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin, connection_limit: 1)
      typed = client(transport, origin)
      first = task.async { typed.echo(AsyncHelpers::MESSAGE.new(name: "hold"), timeout: nil) }
      entered.dequeue
      source = ConnectRPC::CancellationSource.new
      second = task.async(finished: false) do
        typed.echo(AsyncHelpers::MESSAGE.new(name: "cancel"), timeout: nil, cancellation: source.token)
      end
      wait_until(task, "pool acquisition waiter") { transport.instance_variable_get(:@acquiring).size == 1 }
      source.cancel
      expect { second.wait }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("canceled") }
      expect(first).not_to be_finished
      release.enqueue(true)
      expect(first.wait.message.name).to eq("hold")
      expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "after")).message.name).to eq("after")
      expect(observed).to eq(%w[hold after])
      expect(transport.instance_variable_get(:@acquiring)).to be_empty
    ensure
      release&.enqueue(true)
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end
end

RSpec.describe "Direct HTTP/2 cancellation and deadline composition" do
  it "keeps a real deadline authoritative during I/O with a plain live token" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      host = ConnectRPC::Async::Host.new(server: server(echo: lambda { |message, _|
        entered.enqueue(true)
        release.dequeue
        message
      })).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      source = ConnectRPC::CancellationSource.new
      outgoing = request("#{origin}#{AsyncHelpers::ECHO::ECHO.path}",
                         headers: { "content-type" => ["application/proto"] }, deadline: ConnectRPC::Deadline.new(0.2))
      outgoing.cancellation = source.token
      pending = task.async(finished: false) { transport.call(outgoing) }
      entered.dequeue
      # The real socket deadline is the external behavior under test.
      expect { pending.wait }.to raise_error(ConnectRPC::Error) do |error|
        expect([error.code, error.cause]).to eq(["deadline_exceeded", nil])
      end
      expect(outgoing.cancellation).to equal(source.token)
      expect(source.instance_variable_get(:@signal).instance_variable_get(:@registrations)).to be_empty
      expect(transport.instance_variable_get(:@calls)).to be_empty
    ensure
      release&.enqueue(true)
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end
end

RSpec.describe "HTTP/2 unary upload cancellation" do
  # The pinned async-http server grants stream credit only as its handler
  # reads, so a handler that never reads stalls the upload at the window.
  %i[token deadline stop].each do |reason|
    it "resets a credit-blocked upload with CANCEL on #{reason}" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        listener = bound.sockets.first
        held = Async::Queue.new
        peer = ConnectionPeer::RecordingServer.new(task, listener) do |request|
          held.enqueue(request)
          Async::Promise.new.wait
        end
        received = Hash.new(0)
        resets = {}
        peer.define_singleton_method(:record) do |index, frame|
          received[frame.stream_id] += frame.length if frame.is_a?(Protocol::HTTP2::DataFrame)
          resets[frame.stream_id] = frame.unpack if frame.is_a?(Protocol::HTTP2::ResetStreamFrame)
          super(index, frame)
        end
        origin = "http://127.0.0.1:#{listener.local_address.ip_port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        source = ConnectRPC::CancellationSource.new
        call = task.async(finished: false) do
          client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "x" * 200_000),
                                         timeout: reason == :deadline ? 1 : nil, cancellation: source.token)
        end
        id = held.dequeue.stream.id
        operation = transport.instance_variable_get(:@calls).keys.first
        stream = operation.fetch(:response).stream
        wait_until(task, "upload blocked on peer credit") do
          stream.remote_window.available <= 0 && received[id] == 65_535
        end
        expect(call).not_to be_finished
        expect(operation.fetch(:request).deadline).not_to be_expired if reason == :deadline
        expect(peer.data_frames).to all(satisfy { |_, _, flags| flags.nobits?(Protocol::HTTP2::END_STREAM) })
        case reason
        when :token then source.cancel
        when :stop then call.stop
        end
        if reason == :stop
          call.wait
        else
          expect { call.wait }.to raise_error(ConnectRPC::Error) do |error|
            expect(error.code).to eq(reason == :token ? "canceled" : "deadline_exceeded")
          end
        end
        wait_until(task, "peer received the upload reset") { resets.key?(id) }
        expect(resets).to eq(id => Protocol::HTTP2::Error::CANCEL)
        expect(peer.data_frames).to all(satisfy { |_, _, flags| flags.nobits?(Protocol::HTTP2::END_STREAM) })
        expect(transport.instance_variable_get(:@calls)).to be_empty
        expect(lease_usage(transport)).to eq(0)
      ensure
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end
  end
end
