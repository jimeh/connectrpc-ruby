# frozen_string_literal: true

require "rack/mock"
require "rack/lint"
require_relative "../../../examples/localhost"

RSpec.describe "Rack and native Rails tracing" do
  let(:telemetry) { tracing }
  let(:provider) { telemetry[0] }
  let(:exporter) { telemetry[1] }
  let(:bus) { telemetry[2] }
  let(:message) { AsyncHelpers::MESSAGE.new(name: "http") }

  it "gives response_finished precedence over portable body close, including callback errors" do
    app = ConnectRPC::Rack.new(server: server(instrumentation: bus))
    [nil, IOError.new("secret")].each do |failure|
      options = { method: "POST", "CONTENT_TYPE" => "application/proto", input: message.to_proto }
      env = Rack::MockRequest.env_for(AsyncHelpers::ECHO::ECHO.path, **options)
      env["rack.response_finished"] = []
      status, headers, body = app.call(env)
      before = rpc_spans(exporter).size
      body.close if body.respond_to?(:close)
      expect(rpc_spans(exporter).size).to eq(before)
      2.times do
        env["rack.response_finished"].reverse_each { |callback| callback.call(env, status, headers, failure) }
      end
      expect(rpc_spans(exporter).size).to eq(before + 1)
      expected_delivery = failure ? "aborted" : "response_finished"
      expect(rpc_spans(exporter).last.attributes["connectrpc.delivery"]).to eq(expected_delivery)
    end
  end

  it "records body_closed without claiming delivery and leaves unrelated fallback traffic alone" do
    app = ConnectRPC::Rack.new(->(_) { [204, {}, []] }, server: server(instrumentation: bus))
    options = { method: "POST", "CONTENT_TYPE" => "application/proto", input: message.to_proto }
    env = Rack::MockRequest.env_for(AsyncHelpers::ECHO::ECHO.path, **options)
    _status, _headers, body = app.call(env)
    expect(rpc_spans(exporter)).to be_empty
    body.each { |_| nil }
    body.close
    body.close
    expect(rpc_spans(exporter).size).to eq(1)
    expect(rpc_spans(exporter).first.attributes["connectrpc.delivery"]).to eq("body_closed")
    expect(app.call(Rack::MockRequest.env_for("/ordinary")).first).to eq(204)
    expect(rpc_spans(exporter).size).to eq(1)
  end

  %i[proto json].each do |codec|
    it "exports generated #{codec} calls over real Rack/NetHTTP localhost" do
      host = EchoExample::Localhost.new(Rack::Lint.new(ConnectRPC::Rack.new(server: server(instrumentation: bus))))
      typed = client(nil, host.url, codec: codec, instrumentation: bus)
      expect(typed.echo(message).message).to eq(message)
      host.close
      spans = rpc_spans(exporter)
      expect(spans.size).to eq(2)
      caller = spans.find { |span| span.kind == :client }
      receiver = spans.find { |span| span.kind == :server }
      expect(receiver.parent_span_id).to eq(caller.span_id)
      expect(receiver.attributes["connectrpc.delivery"]).to eq("body_closed")
    ensure
      host&.close
    end
  end

  it "coexists with real Rack 0.31.2 and NetHTTP 0.29.2 instrumentation without replacing application globals" do
    require "net/http"
    require "opentelemetry-instrumentation-rack"
    require "opentelemetry-instrumentation-net_http"
    original_provider = OpenTelemetry.tracer_provider
    original_propagator = OpenTelemetry.propagation
    # The test application owns its SDK/global configuration. The adapter never does this.
    OpenTelemetry.tracer_provider = provider
    OpenTelemetry.propagation = OpenTelemetry::Trace::Propagation::TraceContext.text_map_propagator
    rack = OpenTelemetry::Instrumentation::Rack::Instrumentation.instance
    net_http = OpenTelemetry::Instrumentation::Net::HTTP::Instrumentation.instance
    expect(rack.install(use_rack_events: false)).to be(true)
    expect(net_http.install).to be(true)
    app = rack.middleware_args.first.new(ConnectRPC::Rack.new(server: server(instrumentation: bus)))
    host = EchoExample::Localhost.new(app)
    typed = client(nil, host.url, instrumentation: bus)
    parent = provider.tracer("test").start_span("application")
    OpenTelemetry::Trace.with_span(parent) { expect(typed.echo(message).message).to eq(message) }
    host.close
    rpc = rpc_spans(exporter)
    expect(rpc.size).to eq(2)
    caller = rpc.find { |span| span.kind == :client }
    receiver = rpc.find { |span| span.kind == :server }
    http = exporter.finished_spans.reject { |span| span.instrumentation_scope.name == "connectrpc-otel" }
    http_client = http.find { |span| span.kind == :client }
    http_server = http.find { |span| span.kind == :server }
    expect(caller.parent_span_id).to eq(parent.context.span_id)
    expect(http_client.parent_span_id).to eq(caller.span_id)
    # Both servers extract the HTTP client's serialized carrier; the RPC adapter
    # deliberately does not infer a parent by inspecting the ambient HTTP span.
    expect(receiver.parent_span_id).to eq(http_client.span_id)
    expect(http_server.parent_span_id).to eq(http_client.span_id)
    expect((rpc + http).map(&:trace_id).uniq).to eq([parent.context.trace_id])
    expect(OpenTelemetry.tracer_provider).to equal(provider)
    expect(OpenTelemetry.propagation).to equal(OpenTelemetry::Trace::Propagation::TraceContext.text_map_propagator)
  ensure
    host&.close
    parent&.finish
    OpenTelemetry.tracer_provider = original_provider if original_provider
    OpenTelemetry.propagation = original_propagator if original_propagator
  end
end
