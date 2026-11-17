# frozen_string_literal: true

RSpec.describe "Native Async and Falcon hosts" do
  def native(target, backend: :async, **, &)
    if backend == :async
      host = ConnectRPC::Async::Host.new(server: target, **).start
      yield host, "http://127.0.0.1:#{host.port}"
    else
      falcon(server: target, &)
    end
  ensure
    host&.close(grace: 0)
  end

  def typed_client(origin, instrumentation)
    transport = ConnectRPC::Async::Transport.new(origin: origin)
    [client(transport, origin, instrumentation: instrumentation), transport]
  end

  %i[async falcon].each do |backend|
    it "keeps interleaved #{backend} RPCs in separate transactions continued over h2c" do
      sentry(isolation: :fiber)
      instrumentation = bus
      reactor do |task|
        entered = Async::Queue.new
        release = Async::Queue.new
        handler = lambda do |message, _|
          Sentry.set_tags(rpc_name: message.name)
          entered.enqueue(message.name)
          release.dequeue
          Sentry.with_child_span(op: "db.query", description: "query #{message.name}") { nil }
          message
        end
        native(echo_server(instrumentation: instrumentation, echo: handler), backend: backend) do |_host, origin|
          typed, transport = typed_client(origin, instrumentation)
          callers = {}
          workers = %w[a b].map do |name|
            task.async do
              callers[name] = start_caller(name)
              result = typed.echo(MESSAGE.new(name: name))
              expect(scope_span).to equal(callers[name])
              callers[name].finish
              result.message.name
            end
          end
          expect(2.times.map { entered.dequeue }.sort).to eq(%w[a b])
          2.times { release.enqueue(true) }
          expect(workers.map(&:wait).sort).to eq(%w[a b])
          wait_until(task, "both owned server transactions") { rpc_transactions("rpc.server").size == 2 }
          %w[a b].each do |name|
            caller = transactions.find { |event| event.transaction == name }
            client = caller.spans.find { |span| span[:op] == "rpc.client" }
            expect(client).to include(parent_span_id: callers[name].span_id, status: "ok")
            server = rpc_transactions("rpc.server").find { |event| event.tags[:rpc_name] == name }
            expect(trace_of(server)).to include(trace_id: callers[name].trace_id, parent_span_id: client[:span_id],
                                                status: "ok")
            expect(trace_of(server)[:data]).to include("connectrpc.delivery" => "completed")
            expect(server.spans.map { |span| span[:description] }).to eq(["query #{name}"])
          end
          expect(scope_span).to be_nil
        ensure
          2.times { release.enqueue(true) }
          transport&.close(grace: 0)
        end
      end
    end
  end

  it "fails closed under a fiber scheduler with thread isolation, warns once and still captures bugs" do
    instrumentation = bus
    reactor do
      seen = []
      handler = lambda do |message, context|
        seen << [context.request_metadata["sentry-trace"], scope_span]
        raise "native bug" if message.name == "bug"

        message
      end
      native(echo_server(instrumentation: instrumentation, echo: handler)) do |_host, origin|
        typed, transport = typed_client(origin, instrumentation)
        caller = start_caller
        Sentry.get_current_scope.set_tags(thread_scope: "not copied")
        typed.echo(MESSAGE.new(name: "ok"))
        expect { typed.echo(MESSAGE.new(name: "bug")) }.to raise_error(ConnectRPC::Error)
        expect(scope_span).to equal(caller)
        expect(Sentry.get_current_scope.tags).to eq(thread_scope: "not copied")
        caller.finish
        expect(seen.map(&:first)).to eq([nil, nil])
        expect(transactions.size).to eq(1)
        expect(transactions.first.spans).to be_empty
        expect(sentry_log.scan("hub_isolation_level = :fiber").size).to eq(1)
        expect(error_events.size).to eq(1)
        expect(error_events.first.tags).to eq("rpc.system.name" => "connectrpc",
                                              "rpc.method" => "example.v1.EchoService/Echo")
        expect(trace_of(error_events.first)[:trace_id]).not_to eq(caller.trace_id)
      ensure
        transport&.close(grace: 0)
      end
    end
  end

  it "finishes an admission-rejected RPC without attaching and without capture" do
    sentry(isolation: :fiber)
    instrumentation = bus
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      handler = lambda do |message, _|
        if message.name == "admitted"
          entered.enqueue(true)
          release.dequeue
        end
        message
      end
      native(echo_server(instrumentation: instrumentation, echo: handler), max_requests: 1) do |_host, origin|
        typed, transport = typed_client(origin, nil)
        # The host refuses the stream with REFUSED_STREAM; without the default
        # resend, one refused stream is one admission rejection.
        other_transport = ConnectRPC::Async::Transport.new(
          origin: origin, connection_policy: ConnectRPC::Async::ConnectionPolicy.new(retry_refused: false)
        )
        other = client(other_transport, origin)
        other.echo(MESSAGE.new(name: "warmup"))
        wait_until(task, "warmup transaction") { rpc_transactions.size == 1 }
        active = task.async { typed.echo(MESSAGE.new(name: "admitted"), timeout: nil) }
        entered.dequeue
        expect { other.echo(MESSAGE.new(name: "rejected"), timeout: nil) }
          .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("unavailable") }
        wait_until(task, "rejection transaction") { rpc_transactions.size == 2 }
        rejected = rpc_transactions.last
        expect(trace_of(rejected)).to include(status: "unavailable")
        expect(trace_of(rejected)[:data]).to include("connectrpc.processing.state" => "not_started",
                                                     "connectrpc.decision" => "admission_rejected")
        # A refusal is a stream reset, not an HTTP response.
        expect(trace_of(rejected)[:data]).not_to have_key("http.response.status_code")
        expect(trace_of(rejected)[:data]).not_to have_key("rpc.response.status_code")
        expect(rejected.tags).to be_empty
        release.enqueue(true)
        expect(active.wait.message.name).to eq("admitted")
        expect(error_events).to be_empty
      ensure
        release.enqueue(true)
        transport&.close(grace: 0)
        other_transport&.close(grace: 0)
      end
    end
  end

  it "marks a successful RPC whose response delivery aborted as unknown_error" do
    sentry(isolation: :fiber)
    instrumentation = bus
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      handler = lambda do |message, _|
        if message.name == "sibling"
          entered.enqueue(true)
          release.dequeue
        end
        message
      end
      native(echo_server(instrumentation: instrumentation, echo: handler)) do |host, origin|
        typed, transport = typed_client(origin, nil)
        sibling = task.async { typed.echo(MESSAGE.new(name: "sibling"), timeout: nil) }
        entered.dequeue
        resource = host_session(host).instance_variable_get(:@resources).first
        gate = FrameGate.new(resource.socket, frame_type: 1)
        resource.connection.stream.instance_variable_set(:@io, gate)
        gate.armed = true
        call = task.async { typed.echo(MESSAGE.new(name: "reset"), timeout: nil) }
        gate.started.dequeue
        stream = resource.connection.streams.values.max_by(&:id)
        call.cancel
        call.wait
        wait_until(task, "reset while response headers are flushing") { stream.closed? }
        gate.release.enqueue(true)
        release.enqueue(true)
        expect(sibling.wait.message.name).to eq("sibling")
        wait_until(task, "both server transactions") { rpc_transactions.size == 2 }
        traces = rpc_transactions.map { |event| trace_of(event) }
        aborted = traces.find { |trace| trace[:data]["connectrpc.delivery"] == "aborted" }
        expect(aborted).to include(status: "unknown_error")
        expect(aborted[:data]).to include("rpc.response.status_code" => "OK")
        completed = traces.find { |trace| trace[:data]["connectrpc.delivery"] == "completed" }
        expect(completed).to include(status: "ok")
      ensure
        gate&.release&.enqueue(true)
        release.enqueue(true)
        transport&.close(grace: 0)
      end
    end
  end

  it "keeps handler scope changes on the owned transaction and its error event" do
    sentry(isolation: :fiber)
    instrumentation = bus
    reactor do |task|
      handler = lambda do |*|
        Sentry.set_user(id: "user-1")
        Sentry.set_context("account", plan: "pro")
        raise "native bug"
      end
      native(echo_server(instrumentation: instrumentation, fail: handler)) do |_host, origin|
        typed, transport = typed_client(origin, nil)
        expect { typed.fail(MESSAGE.new) }.to raise_error(ConnectRPC::Error)
        wait_until(task, "server transaction") { rpc_transactions.size == 1 }
        [rpc_transactions.first, *error_events].each do |event|
          expect(event.user).to eq(id: "user-1")
          expect(event.contexts).to include("account" => { plan: "pro" })
        end
        expect(error_events.size).to eq(1)
      ensure
        transport&.close(grace: 0)
      end
    end
  end

  it "captures a native handler bug once on its owned transaction" do
    sentry(isolation: :fiber)
    instrumentation = bus
    reactor do |task|
      native(echo_server(instrumentation: instrumentation, fail: ->(*) { raise "native bug" })) do |_host, origin|
        typed, transport = typed_client(origin, nil)
        expect { typed.fail(MESSAGE.new) }.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("internal") }
        wait_until(task, "server transaction") { rpc_transactions.size == 1 }
        owned = trace_of(rpc_transactions.first)
        expect(owned).to include(status: "internal_error")
        expect(error_events.size).to eq(1)
        expect(trace_of(error_events.first)).to include(span_id: owned[:span_id], trace_id: owned[:trace_id])
      ensure
        transport&.close(grace: 0)
      end
    end
  end
end
