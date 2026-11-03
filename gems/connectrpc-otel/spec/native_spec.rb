# frozen_string_literal: true

RSpec.describe "Native host tracing" do
  let(:telemetry) { tracing }
  let(:provider) { telemetry[0] }
  let(:exporter) { telemetry[1] }
  let(:bus) { telemetry[2] }

  %i[async falcon].each do |backend|
    it "keeps overlapping #{backend} HTTP/2 calls isolated across operation, setup and handler tasks" do
      reactor do |task|
        entered = Async::Queue.new
        release = Async::Queue.new
        observed = {}
        target = server(instrumentation: bus, echo: lambda { |message, _context|
          observed[message.name] = current_span.context
          entered.enqueue(message.name)
          release.dequeue
          expect(current_span.context.span_id).to eq(observed.fetch(message.name).span_id)
          message
        })
        runner = lambda do |host, origin|
          transport = ConnectRPC::Async::Transport.new(origin: origin)
          typed = client(transport, origin, instrumentation: bus)
          task_contexts = []
          probe = TracePoint.new(:call) do |event|
            if event.defined_class == ConnectRPC::Async::Transport && %i[exchange
                                                                         setup_connection].include?(event.method_id)
              task_contexts << [event.method_id, current_span.context]
            end
          end
          parents = 2.times.map { |index| provider.tracer("test").start_span("parent#{index}") }
          calls = probe.enable do
            workers = parents.each_with_index.map do |parent, index|
              task.async do
                OpenTelemetry::Trace.with_span(parent) do
                  result = typed.echo(AsyncHelpers::MESSAGE.new(name: index.to_s))
                  expect(current_span).to equal(parent)
                  result
                end
              end
            end
            expect(2.times.map { entered.dequeue }.sort).to eq(%w[0 1])
            expect(rpc_spans(exporter)).to be_empty
            2.times { release.enqueue(true) }
            expect(workers.map { |worker| worker.wait.message.name }.sort).to eq(%w[0 1])
            workers
          end
          wait_until(task, "both native server span completions") { rpc_spans(exporter).size == 4 }
          spans = rpc_spans(exporter)
          expect(task_contexts.map(&:first)).to include(:exchange, :setup_connection)
          expect(task_contexts.map { |entry| entry.last.valid? }).to all(be(true))
          parents.each_with_index do |parent, index|
            pair = spans.select { |span| span.trace_id == parent.context.trace_id }
            expect(pair.size).to eq(2)
            caller = pair.find { |span| span.kind == :client }
            receiver = pair.find { |span| span.kind == :server }
            expect(caller.parent_span_id).to eq(parent.context.span_id)
            expect(receiver.parent_span_id).to eq(caller.span_id)
            expect(receiver.span_id).to eq(observed.fetch(index.to_s).span_id)
            expect(receiver.attributes["connectrpc.delivery"]).to eq("completed")
          end
          client_resources = transport.instance_variable_get(:@resources)
          client_resources.each do |resource|
            stack = resource.connection.reader_task.fiber.opentelemetry_context
            expect(stack.nil? || stack.empty?).to be(true)
          end
          expect(current_span.context).not_to be_valid
          expect(calls).to all(be_finished)
          expect(host.close(grace: 1)).to be(true)
        ensure
          probe&.disable
          parents&.each(&:finish)
          2.times { release.enqueue(true) }
          transport&.close(grace: 0)
        end
        if backend == :async
          host = ConnectRPC::Async::Host.new(server: target).start
          runner.call(host, "http://127.0.0.1:#{host.port}")
        else
          falcon(server: target, &runner)
        end
      ensure
        host&.close(grace: 0)
      end
    end
  end

  it "finishes Falcon HTTP/1 only after the native writer and restores the handler context" do
    reactor do |task|
      handler_id = nil
      target = server(instrumentation: bus, echo: lambda { |message, _|
        handler_id = current_span.context.span_id
        expect(rpc_spans(exporter)).to be_empty
        message
      })
      falcon(server: target) do |_host, origin|
        message = AsyncHelpers::MESSAGE.new(name: "http1")
        parent = provider.tracer("test").start_span("parent")
        carrier = {}
        propagator = OpenTelemetry::Trace::Propagation::TraceContext.text_map_propagator
        propagator.inject(carrier, context: OpenTelemetry::Trace.context_with_span(parent))
        headers = carrier.merge("content-type" => "application/proto")
        status, _headers, body = http1(origin, AsyncHelpers::ECHO::ECHO.path, method: "POST",
                                                                              body: message.to_proto, headers: headers)
        expect(status).to eq(200)
        expect(AsyncHelpers::MESSAGE.decode(body)).to eq(message)
        wait_until(task, "HTTP/1 server completion") { rpc_spans(exporter).size == 1 }
        span = rpc_spans(exporter).first
        expect(span.span_id).to eq(handler_id)
        expect(span.parent_span_id).to eq(parent.context.span_id)
        expect(span.attributes["connectrpc.delivery"]).to eq("completed")
        expect(current_span.context).not_to be_valid
      ensure
        parent&.finish
      end
    end
  end

  it "records Falcon listener rejection independently of core processing" do
    reactor do |task|
      falcon(server: server(instrumentation: bus)) do |_host, origin|
        status, = http1(origin, AsyncHelpers::ECHO::ECHO.path, headers: { "upgrade" => "websocket" })
        expect(status).to eq(501)
        wait_until(task, "host rejection completion") { rpc_spans(exporter).size == 1 }
        span = rpc_spans(exporter).first
        expect(span.attributes).to include("connectrpc.processing.state" => "not_started",
                                           "connectrpc.decision" => "host_rejected",
                                           "connectrpc.delivery" => "completed")
        expect(span.attributes).not_to have_key("rpc.response.status_code")
      end
    end
  end

  it "keeps a normal-return header reset aborted and lets a sibling finish on the same connection" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      target = server(instrumentation: bus, echo: lambda { |message, _|
        if message.name == "sibling"
          entered.enqueue(true)
          release.dequeue
        end
        message
      })
      host = ConnectRPC::Async::Host.new(server: target).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      typed = client(transport, origin, instrumentation: bus)
      sibling = task.async { typed.echo(AsyncHelpers::MESSAGE.new(name: "sibling"), timeout: nil) }
      entered.dequeue
      resource = host_session(host).instance_variable_get(:@resources).first
      gate = FrameGate.new(resource.socket, frame_type: 1)
      resource.connection.stream.instance_variable_set(:@io, gate)
      gate.armed = true
      call = task.async { typed.echo(AsyncHelpers::MESSAGE.new(name: "reset"), timeout: nil) }
      gate.started.dequeue
      stream = resource.connection.streams.values.max_by(&:id)
      call.cancel
      call.wait
      wait_until(task, "reset while response headers are flushing") { stream.closed? }
      gate.release.enqueue(true)
      release.enqueue(true)
      expect(sibling.wait.message.name).to eq("sibling")
      wait_until(task, "both host completions") { rpc_spans(exporter).count { |span| span.kind == :server } == 2 }
      servers = rpc_spans(exporter).select { |span| span.kind == :server }
      expect(servers.map { |span| span.attributes["connectrpc.delivery"] }.sort).to eq(%w[aborted completed])
      aborted = servers.find { |span| span.attributes["connectrpc.delivery"] == "aborted" }
      expect(aborted.attributes).to include("rpc.response.status_code" => "OK", "error.type" => "delivery_aborted")
      expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "after")).message.name).to eq("after")
    ensure
      gate&.release&.enqueue(true)
      release&.enqueue(true)
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end
  it "exports a real deadline after handler progress, with aborted delivery and restored contexts" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      target = server(instrumentation: bus, echo: lambda { |message, context|
        entered.enqueue(context)
        release.dequeue
        context.check!
        message
      })
      host = ConnectRPC::Async::Host.new(server: target).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      typed = client(transport, origin, instrumentation: bus)
      server_context = nil
      # Server wire milliseconds round up and start later. Establish actual
      # server expiry/acceptance before the client's cleanup sends peer CANCEL.
      allow(transport).to receive(:cleanup).and_wrap_original do |cleanup, operation|
        wait_until(task, "server deadline before reset") { server_context&.deadline&.expired? }
        expect(server_context.cancellation_reason).to eq(:deadline_exceeded)
        cleanup.call(operation)
      end
      call = task.async do
        typed.echo(AsyncHelpers::MESSAGE.new(name: "deadline"), timeout: 0.5)
      rescue ConnectRPC::Error => e
        e
      end
      server_context = entered.dequeue
      expect(call.wait.code).to eq("deadline_exceeded")
      # Wire milliseconds round up, and the server starts its own budget later.
      # Wait for that actual external deadline before releasing cooperative work.
      wait_until(task, "server deadline expiry") do
        server_context.deadline.expired?
      end
      release.enqueue(true)
      wait_until(task, "deadline server completion") { rpc_spans(exporter).size == 2 }
      spans = rpc_spans(exporter)
      expect(spans.map do |span|
        span.attributes["rpc.response.status_code"]
      end).to eq(%w[deadline_exceeded deadline_exceeded])
      expect(spans.map { |span| span.status.code }).to all(eq(OpenTelemetry::Trace::Status::ERROR))
      expect(spans.find { |span| span.kind == :server }.attributes["connectrpc.delivery"]).to eq("aborted")
      expect(current_span.context).not_to be_valid
    ensure
      release&.enqueue(true)
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  it "propagates a generated request over verified TLS with a native server span" do
    reactor do |task|
      with_ca do |ca_file|
        host = ConnectRPC::Async::Host.new(server: server(instrumentation: bus, encoding: :gzip), tls: host_tls).start
        origin = "https://localhost:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca_file)
        typed = client(transport, origin, codec: :json, encoding: :gzip, instrumentation: bus)
        expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "tls")).message.name).to eq("tls")
        wait_until(task, "TLS span completion") { rpc_spans(exporter).size == 2 }
        caller = rpc_spans(exporter).find { |span| span.kind == :client }
        receiver = rpc_spans(exporter).find { |span| span.kind == :server }
        expect(receiver.parent_span_id).to eq(caller.span_id)
        expect(receiver.attributes["connectrpc.delivery"]).to eq("completed")
      ensure
        transport&.close(grace: 0)
        host&.close(grace: 0)
      end
    end
  end
end
