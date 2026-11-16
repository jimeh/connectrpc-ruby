# frozen_string_literal: true

RSpec.describe "Rack hosting" do
  let(:rpc_app) { ConnectRPC::Rack.new(server: echo_server(instrumentation: bus)) }

  it "nests in Sentry's Rack transaction, renames it and records no delivery facts" do
    app = with_sentry_middleware(rpc_app)
    expect(rack_post(app, "rack").status).to eq(200)
    expect(transactions.size).to eq(1)
    host = transactions.first
    expect(trace_of(host)[:op]).to eq("http.server")
    expect(host.transaction).to eq("example.v1.EchoService/Echo")
    rpc = host.spans.find { |span| span[:op] == "rpc.server" }
    expect(rpc).to include(parent_span_id: trace_of(host)[:span_id], status: "ok")
    expect(rpc[:data]).to include("rpc.response.status_code" => "OK")
    expect(rpc[:data]).not_to have_key("connectrpc.delivery")
    expect(scope_span).to be_nil
  end

  it "owns a transaction finished at body close without Sentry's middleware" do
    expect(rack_post(rpc_app, "rack").status).to eq(200)
    expect(transactions.size).to eq(1)
    owned = transactions.first
    expect(owned.transaction).to eq("example.v1.EchoService/Echo")
    expect(owned.transaction_info).to eq(source: :route)
    trace = trace_of(owned)
    expect(trace).to include(op: "rpc.server", origin: "auto.rpc.connectrpc", status: "ok", parent_span_id: nil)
    expect(trace[:data]).to include("rpc.system.name" => "connectrpc", "rpc.method" => "example.v1.EchoService/Echo",
                                    "rpc.response.status_code" => "OK", "http.response.status_code" => 200,
                                    "connectrpc.processing.state" => "finished", "connectrpc.decision" => "processed",
                                    "connectrpc.delivery" => "body_closed")
    expect(trace[:data]).to include("connectrpc.request.wire_bytes", "connectrpc.response.wire_bytes",
                                    "connectrpc.processing.duration")
    expect(scope_span).to be_nil
  end

  it "records response_finished delivery from the Rack callback" do
    env = Rack::MockRequest.env_for(ECHO::ECHO.path, method: "POST", "CONTENT_TYPE" => "application/proto",
                                                     input: MESSAGE.new(name: "callback").to_proto)
    callbacks = env["rack.response_finished"] = []
    status, headers, body = rpc_app.call(env)
    body.each { |_| nil }
    expect(transactions).to be_empty
    callbacks.each { |callback| callback.call(env, status, headers, nil) }
    expect(trace_of(transactions.first)[:data]).to include("connectrpc.delivery" => "response_finished")
  ensure
    body&.close if body.respond_to?(:close)
  end

  it "does not touch the caller's scope while it nests under another open span" do
    caller = start_caller
    Sentry.get_current_scope.set_tags(request: "kept")
    expect(rack_post(rpc_app, "nested").status).to eq(200)
    expect(scope_span).to equal(caller)
    expect(Sentry.get_current_scope.tags).to eq(request: "kept")
    caller.finish
    expect(transactions.size).to eq(1)
    expect(transactions.first.spans.map { |span| span[:op] }).to eq(["rpc.server"])
  end

  it "does not touch the caller's scope when it captures a nested handler bug" do
    caller = start_caller
    Sentry.get_current_scope.set_tags(request: "kept")
    app = ConnectRPC::Rack.new(server: echo_server(instrumentation: bus, echo: ->(*) { raise "nested bug" }))
    expect(rack_post(app, "nested").status).to eq(500)
    expect(scope_span).to equal(caller)
    expect(Sentry.get_current_scope.tags).to eq(request: "kept")
    expect(error_events.size).to eq(1)
    expect(error_events.first.tags).to eq(request: "kept", "rpc.system.name" => "connectrpc",
                                          "rpc.method" => "example.v1.EchoService/Echo")
    caller.finish
    expect(transactions.first.tags).to eq(request: "kept")
  end

  it "adds terminal facts to a nested span while its parent is still open" do
    caller = start_caller
    expect(rack_post(rpc_app, "nested").status).to eq(200)
    caller.finish
    nested = transactions.first.spans.find { |span| span[:op] == "rpc.server" }
    expect(nested[:data]).to include("connectrpc.processing.state" => "finished", "connectrpc.decision" => "processed",
                                     "connectrpc.delivery" => "body_closed")
    expect(nested[:status]).to eq("ok")
  end

  it "names unknown methods by system with rpc.method _OTHER and keeps the host transaction name" do
    app = with_sentry_middleware(rpc_app)
    sentry(trace_ignore_status_codes: [])
    Rack::MockRequest.new(app).post("/example.v1.EchoService/Missing", "CONTENT_TYPE" => "application/proto",
                                                                       input: "")
    Rack::MockRequest.new(rpc_app).post("/example.v1.EchoService/Missing", "CONTENT_TYPE" => "application/proto",
                                                                           input: "")
    host, owned = transactions
    expect(host.transaction).to eq("/example.v1.EchoService/Missing")
    nested = host.spans.find { |span| span[:op] == "rpc.server" }
    expect(nested).to include(description: "connectrpc")
    expect(owned.transaction).to eq("connectrpc")
    [nested, trace_of(owned)].each do |span|
      expect(span[:data]).to include("rpc.method" => "_OTHER", "rpc.system.name" => "connectrpc")
      expect(span[:data]).not_to include("example.v1.EchoService/Missing")
    end
  end

  it "records a bare HTTP rejection through its HTTP status" do
    response = Rack::MockRequest.new(rpc_app).post(ECHO::ECHO.path, "CONTENT_TYPE" => "text/plain", input: "x")
    expect(response.status).to eq(415)
    trace = trace_of(rpc_transactions.first)
    expect(trace[:data]).to include("http.response.status_code" => 415, "connectrpc.decision" => "processed")
    expect(trace[:data]).not_to have_key("rpc.response.status_code")
    expect(trace[:status]).to eq(Sentry::Span::STATUS_MAP[415])
  end

  def with_sentry_middleware(app)
    Rack::Builder.new.tap do |builder|
      builder.use Sentry::Rack::CaptureExceptions
      builder.run app
    end.to_app
  end

  [false, true].each do |middleware|
    it "reports a Rack handler bug exactly once on the RPC #{middleware ? 'span' : 'transaction'}" do
      app = ConnectRPC::Rack.new(server: echo_server(instrumentation: bus, echo: ->(*) { raise "rack bug" }))
      app = with_sentry_middleware(app) if middleware
      expect(rack_post(app, "bug").status).to eq(500)
      expect(error_events.size).to eq(1)
      expect(mechanism(error_events.first)).to include(type: "connectrpc", handled: false)
      event = transactions.first
      rpc = middleware ? event.spans.find { |span| span[:op] == "rpc.server" } : trace_of(event)
      expect(rpc).to include(op: "rpc.server", status: "internal_error")
      expect(trace_of(error_events.first)).to include(span_id: rpc[:span_id], trace_id: rpc[:trace_id])
    end
  end
end
