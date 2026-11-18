# frozen_string_literal: true

RSpec.describe "Span lifetime, promotion and finish scope" do
  def stream_method
    @stream_method ||= ConnectRPC::ServerStreamMethod.new(service: "trace.Streams", name: "Watch",
                                                          request_type: MESSAGE, response_type: MESSAGE)
  end

  def stream_server(instrumentation, &handler)
    ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: "trace.Streams", rpcs: [stream_method])],
                           handlers: { stream_method.path => handler }, instrumentation: instrumentation,
                           binding: ConnectRPC::Bindings::GoogleProtobuf.new)
  end

  def occurrences(span_id) = recorded_span_ids.count(span_id)

  it "promotes a client span whose caller finished inside the streaming block, exactly once" do
    sentry(isolation: :fiber)
    instrumentation = bus
    reactor do
      target = stream_server(instrumentation) do |message, writer, _context|
        3.times { |index| writer.send_message(MESSAGE.new(name: "#{message.name}#{index}")) }
      end
      host = ConnectRPC::Async::Host.new(server: target).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      core = ConnectRPC::Client.new(base_url: origin, transport: transport, instrumentation: instrumentation,
                                    binding: ConnectRPC::Bindings::GoogleProtobuf.new)
      caller = start_caller
      received = []
      core.server_stream(stream_method, MESSAGE.new(name: "item")) do |reader|
        received << reader.receive.name
        caller.finish
        reader.each { |message| received << message.name }
      end
      expect(received).to eq(%w[item0 item1 item2])
      wait_until(Async::Task.current, "server and promoted client transactions") { rpc_transactions.size == 2 }
      caller_event = transactions.find { |event| event.transaction == "caller" }
      expect(caller_event.spans.map { |span| span[:op] }).not_to include("rpc.client")
      client = rpc_transactions("rpc.client").first
      server = rpc_transactions("rpc.server").first
      expect(trace_of(client)).to include(trace_id: caller.trace_id, parent_span_id: caller.span_id, status: "ok")
      expect(client.transaction).to eq("trace.Streams/Watch")
      expect(trace_of(client)[:data]).to include("connectrpc.delivery" => "completed", "server.address" => "127.0.0.1")
      expect(trace_of(server)).to include(trace_id: caller.trace_id, parent_span_id: trace_of(client)[:span_id])
      expect(occurrences(trace_of(client)[:span_id])).to eq(1)
    ensure
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  it "promotes a nested server span whose host transaction closed during processing" do
    caller = start_caller("host")
    handler = lambda do |message, _|
      Sentry.with_child_span(op: "db.query", description: "before close") { nil }
      caller.finish
      message
    end
    typed = in_process(echo_server(instrumentation: bus, echo: handler))
    expect(typed.echo(MESSAGE.new(name: "closing")).message.name).to eq("closing")
    host = transactions.find { |event| event.transaction == "host" }
    expect(host.spans.map { |span| span[:op] }).to eq(["db.query"])
    promoted = rpc_transactions("rpc.server").first
    expect(promoted.transaction).to eq("example.v1.EchoService/Echo")
    expect(trace_of(promoted)).to include(trace_id: caller.trace_id, parent_span_id: caller.span_id, status: "ok")
    expect(trace_of(promoted)[:data]).to include("rpc.response.status_code" => "OK")
    expect(occurrences(trace_of(promoted)[:span_id])).to eq(1)
    expect(scope_span).to equal(caller)
  end

  it "promotes a child the saturated span recorder rejected at start instead of losing it" do
    caller = start_caller
    999.times { |index| caller.start_child(op: "filler", description: index.to_s).finish }
    typed = in_process(echo_server(instrumentation: bus))
    expect(typed.echo(MESSAGE.new(name: "full")).message.name).to eq("full")
    expect(rpc_transactions).to eq(rpc_transactions("rpc.server"))
    caller.finish
    host = transactions.find { |event| event.transaction == "caller" }
    expect(host.spans.size).to eq(999)
    expect(host.spans.map { |span| span[:op] }.uniq).to eq(["filler"])
    promoted = rpc_transactions("rpc.server").first
    expect(trace_of(promoted)).to include(trace_id: caller.trace_id, parent_span_id: caller.span_id)
    expect(occurrences(trace_of(promoted)[:span_id])).to eq(1)
  end

  it "finishes an owned transaction with the RPC scope, not the finishing context's scope" do
    handler = lambda do |message, _|
      Sentry.set_tags(handler: "kept")
      Sentry.set_user(id: "rpc-user")
      Sentry.get_current_scope.add_event_processor do |event, _hint|
        event.tags[:rpc_processor] = "ran"
        event
      end
      message
    end
    app = ConnectRPC::Rack.new(server: echo_server(instrumentation: bus, echo: handler))
    env = Rack::MockRequest.env_for(ECHO::ECHO.path, method: "POST", "CONTENT_TYPE" => "application/proto",
                                                     input: MESSAGE.new(name: "scoped").to_proto)
    _status, _headers, body = app.call(env)
    expect(Sentry.get_current_scope.tags).to be_empty
    Sentry.with_scope do |foreign|
      foreign.set_rack_env("PATH_INFO" => "/foreign", "REQUEST_METHOD" => "GET")
      foreign.set_tags(foreign: "leaked")
      foreign.set_level(:fatal)
      foreign.add_event_processor do |event, _hint|
        event.tags[:foreign_processor] = "ran"
        event
      end
      body.close
      expect(Sentry.get_current_scope).to equal(foreign)
      expect(foreign.tags).to eq(foreign: "leaked")
    end
    owned = transactions.first
    expect(owned.tags).to include(handler: "kept", rpc_processor: "ran")
    expect(owned.tags).not_to include(:foreign, :foreign_processor)
    expect(owned.user).to eq(id: "rpc-user")
    expect(owned.level).not_to eq(:fatal)
    expect(owned.to_h[:request]).to be_nil
    expect(Sentry.get_current_scope.tags).to be_empty
    expect(Sentry.get_current_scope.user).to be_empty
  end

  it "releases an owned transaction's profiler registration when another fiber's hub finishes it" do
    sentry(isolation: :fiber)
    app = ConnectRPC::Rack.new(server: echo_server(instrumentation: bus))
    env = Rack::MockRequest.env_for(ECHO::ECHO.path, method: "POST", "CONTENT_TYPE" => "application/proto",
                                                     input: MESSAGE.new(name: "profiled").to_proto)
    starting = Sentry.get_current_hub
    _status, _headers, body = app.call(env)
    expect(starting.profiler_running?).to be(true)
    finishing = Fiber.new do
      body.close
      Sentry.get_current_hub
    end.resume
    expect(finishing).not_to equal(starting)
    expect(starting.profiler_running?).to be(false)
    expect(rpc_transactions("rpc.server").size).to eq(1)
  end

  # Starts a nested server child under a caller span on a child fiber's hub, so it activates elsewhere.
  def nested_elsewhere(instrumentation)
    caller = handle = nil
    Fiber.new do
      caller = start_caller("host")
      Sentry.set_tags(request: "start")
      handle = instrumentation.start(owner: self, role: :server, method: ECHO::ECHO, carrier: {})
    end.resume
    [caller, handle]
  end

  { "saves and restores the span on" => true, "pushes" => false }.each do |path, span|
    it "captures and promotes with the handler's scope when a nested child #{path} another context's scope" do
      sentry(isolation: :fiber)
      caller, handle = nested_elsewhere(bus)
      local = start_caller("local") if span
      bug = RuntimeError.new("moved bug")
      handle.activate do
        handle.begin_processing(self)
        Sentry.set_user(id: "handler-user")
        Sentry.set_tags(handler: "kept")
        caller.finish
        handle.processing_finished(code: "internal", error: bug)
      end
      handle.finish(delivery: :completed)
      local&.finish
      expect(error_events.size).to eq(1)
      promoted = rpc_transactions("rpc.server").first
      events = { error: error_events.first, promoted: promoted }
      expect(events.transform_values { |event| [event.user, event.tags[:handler]] })
        .to eq(error: [{ id: "handler-user" }, "kept"], promoted: [{ id: "handler-user" }, "kept"])
      # A pushed scope is seeded from the start scope; an unrelated context's own scope is never mutated.
      expect(events.transform_values { |event| event.tags[:request] })
        .to eq(error: span ? nil : "start", promoted: span ? nil : "start")
      expect(trace_of(error_events.first)[:span_id]).to eq(trace_of(promoted)[:span_id])
      expect(trace_of(promoted)).to include(trace_id: caller.trace_id, parent_span_id: caller.span_id)
    end
  end

  it "pops the scope it pushed when seeding it from the start scope raises" do
    sentry(isolation: :fiber)
    instrumentation = bus
    caller = handle = nil
    Fiber.new do
      caller = start_caller("host")
      # A Thread cannot be duplicated, so Scope#dup of the start scope raises after the push.
      Sentry.set_context(:probe, worker: Thread.current)
      handle = instrumentation.start(owner: self, role: :server, method: ECHO::ECHO, carrier: {})
    end.resume
    base = Sentry.get_current_scope
    Sentry.set_tags(base: "kept")
    inside = handle.activate { [Sentry.get_current_scope, scope_span] }
    expect(inside).to eq([base, nil])
    expect(Sentry.get_current_scope).to equal(base)
    expect(base.tags).to eq(base: "kept")
    expect(lifecycle_errors.map(&:class)).to eq([TypeError])
    lifecycle_errors.clear
    handle.begin_processing(self)
    handle.processing_finished(code: "OK")
    handle.finish(delivery: :completed)
    caller.finish
  end

  it "pushes and pops its own scope when a child attaches in a context without a current span" do
    sentry(isolation: :fiber)
    instrumentation = bus
    handle = nil
    caller = nil
    # The caller's span lives on a child fiber's hub; the main fiber's hub has none.
    Fiber.new do
      caller = start_caller
      handle = instrumentation.start(owner: self, role: :client, method: ECHO::ECHO, carrier: {},
                                     server_address: "example.invalid", server_port: 443)
    end.resume
    base = Sentry.get_current_scope
    expect(base.get_span).to be_nil
    inside = handle.activate { [Sentry.get_current_scope, scope_span] }
    expect(inside.first).not_to equal(base)
    expect(inside.last.op).to eq("rpc.client")
    expect(Sentry.get_current_scope).to equal(base)
    expect(base.get_span).to be_nil
    handle.begin_processing(self)
    handle.processing_finished(code: "OK")
    handle.finish(delivery: :unobserved)
    caller.finish
    expect(transactions.first.spans.map { |span| span[:op] }).to eq(["rpc.client"])
  end
end
