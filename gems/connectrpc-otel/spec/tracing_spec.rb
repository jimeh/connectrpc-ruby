# frozen_string_literal: true

RSpec.describe ConnectRPC::OTel::Subscriber do
  let(:telemetry) { tracing }
  let(:provider) { telemetry[0] }
  let(:exporter) { telemetry[1] }
  let(:bus) { telemetry[2] }
  let(:message) { AsyncHelpers::MESSAGE.new(name: "private payload") }

  %i[proto json].product(%i[identity gzip]).each do |codec, encoding|
    it "exports real generated #{codec}/#{encoding} client/server traces with serialized parentage and baggage" do
      seen = nil
      target = server(instrumentation: bus, encoding: :gzip, echo: lambda { |request, _context|
        seen = [current_span.context, OpenTelemetry::Baggage.value("tenant")]
        request
      })
      transport = ConnectRPC::Testing::Transport.new(server: target)
      typed = client(transport, "https://in-process.invalid", codec: codec, encoding: encoding, instrumentation: bus)
      parent = provider.tracer("test").start_span("parent")
      parent_context = OpenTelemetry::Baggage.set_value("tenant", "example",
                                                        context: OpenTelemetry::Trace.context_with_span(parent))
      original = OpenTelemetry::Context.current
      metadata = { "x-safe" => ["original"].freeze }.freeze
      OpenTelemetry::Context.with_current(parent_context) do
        expect(typed.echo(message, metadata: metadata).message).to eq(message)
        expect(current_span).to equal(parent)
      end
      expect(OpenTelemetry::Context.current).to equal(original)
      spans = rpc_spans(exporter)
      expect(spans.size).to eq(2)
      caller = spans.find { |span| span.kind == :client }
      receiver = spans.find { |span| span.kind == :server }
      expect(caller.parent_span_id).to eq(parent.context.span_id)
      expect(receiver.parent_span_id).to eq(caller.span_id)
      expect(spans.map(&:trace_id).uniq).to eq([parent.context.trace_id])
      expect(seen.first).to have_attributes(trace_id: receiver.trace_id, span_id: receiver.span_id)
      expect(seen.last).to eq("example")
      spans.each do |span|
        expect(span.name).to eq("example.v1.EchoService/Echo")
        expect(span.attributes).to include("rpc.system.name" => "connectrpc", "rpc.response.status_code" => "OK",
                                           "connectrpc.delivery" => "unobserved")
        expect(span.instrumentation_scope.version).to eq(ConnectRPC::OTel::VERSION)
        expect(span.instrumentation_scope.attributes).to eq("connectrpc.semconv.version" => "1.44.0")
        expect(span.status.code).to eq(OpenTelemetry::Trace::Status::UNSET)
        expect(Array(span.events)).to be_empty
        expect(span.attributes.to_s).not_to include("private payload", "tenant", "grpc.", "rpc.system=>")
      end
      expect(caller.attributes).to include("server.address" => "in-process.invalid", "server.port" => 443)
      expect(metadata).to eq("x-safe" => ["original"])
    ensure
      parent&.finish
    end
  end

  %w[invalid_argument internal unavailable].each do |code|
    it "uses the v1.44 role-specific status policy for #{code}" do
      target = server(instrumentation: bus, echo: ->(*) { raise ConnectRPC::Error.new(code, "private description") })
      typed = client(ConnectRPC::Testing::Transport.new(server: target), "https://in-process.invalid",
                     instrumentation: bus)
      expect { typed.echo(message) }.to raise_error(ConnectRPC::Error)
      spans = rpc_spans(exporter)
      spans.each do |span|
        expect(span.attributes["rpc.response.status_code"]).to eq(code)
        error = span.kind == :client || code != "invalid_argument"
        expect(span.status.code).to eq(error ? OpenTelemetry::Trace::Status::ERROR : OpenTelemetry::Trace::Status::UNSET)
        expect(span.attributes["error.type"]).to eq(error ? code : nil)
        expect(span.status.description).to eq("")
        expect(Array(span.events)).to be_empty
      end
    end
  end

  it "redacts unknown method names and defers end until delivery, marking an abort after success" do
    target = server(instrumentation: bus)
    handle = target.start_call(path: "/example.v1.EchoService/private-name", headers: {})
    response = target.call(path: "/example.v1.EchoService/private-name", http_method: "POST",
                           headers: { "content-type" => ["application/proto"] }, input: nil, lifecycle: handle)
    expect(response.status).to eq(501)
    expect(rpc_spans(exporter)).to be_empty
    handle.finish(delivery: :completed)
    handle.finish(delivery: :aborted)
    span = rpc_spans(exporter).fetch(0)
    expect(span.name).to eq("connectrpc")
    expect(span.attributes["rpc.method"]).to eq("_OTHER")
    expect(span.attributes.to_s).not_to include("private-name", "rpc.method_original")

    handle = target.start_call(path: AsyncHelpers::ECHO::ECHO.path, headers: {})
    target.call(path: AsyncHelpers::ECHO::ECHO.path, http_method: "POST",
                headers: { "content-type" => ["application/proto"] }, input: StringIO.new(message.to_proto),
                lifecycle: handle)
    handle.finish(delivery: :aborted, error: IOError.new("secret"))
    aborted = rpc_spans(exporter).last
    expect(aborted.attributes).to include("rpc.response.status_code" => "OK", "error.type" => "delivery_aborted")
    expect(aborted.status.code).to eq(OpenTelemetry::Trace::Status::ERROR)
  end

  it "extracts repeated/malformed carriers defensively, without ambient in-process parent fallback" do
    parent = provider.tracer("test").start_span("ambient")
    target = server(instrumentation: bus)
    good = "00-#{parent.context.hex_trace_id}-#{parent.context.hex_span_id}-01"
    OpenTelemetry::Trace.with_span(parent) do
      [{ "traceparent" => [good, good] }, { "traceparent" => ["malformed"] },
       { "traceparent" => [Object.new] }].each do |headers|
        handle = target.start_call(path: AsyncHelpers::ECHO::ECHO.path, headers: headers)
        handle.finish(delivery: :completed, decision: :admission_rejected)
      end
    end
    rpc_spans(exporter).each do |span|
      expect(span.parent_span_id).to eq(OpenTelemetry::Trace::INVALID_SPAN_ID)
      expect(span.trace_id).not_to eq(parent.context.trace_id)
      expect(span.attributes).not_to have_key("rpc.response.status_code")
    end
    expect(current_span.context).not_to eq(parent.context)
  ensure
    parent&.finish
  end

  it "captures only explicit bounded repeated metadata and rejects wildcard configuration" do
    _provider, captured, configured = tracing(request_metadata: ["x-capture"], response_metadata: ["x-result"])
    target = server(instrumentation: configured, echo: lambda { |request, context|
      context.response_metadata["x-result"] = ["response"]
      request
    })
    typed = client(ConnectRPC::Testing::Transport.new(server: target), "https://in-process.invalid",
                   instrumentation: configured)
    typed.echo(message, metadata: { "x-capture" => Array.new(12, "a" * 300), "authorization" => "secret" })
    rpc_spans(captured).each do |span|
      expect(span.attributes["connectrpc.request.metadata.x-capture"]).to eq(Array.new(8, "a" * 256))
      expect(span.attributes["connectrpc.response.metadata.x-result"]).to eq(["response"])
      expect(span.attributes.to_s).not_to include("secret", "authorization")
    end
    expect { described_class.new(request_metadata: ["*"]) }.to raise_error(ArgumentError)
  end
  it "joins repeated W3C tracestate and baggage fields and detaches after a fatal application exit" do
    parent = provider.tracer("test").start_span("remote")
    seen = nil
    target = server(instrumentation: bus, echo: lambda { |request, _|
      seen = [current_span.context.tracestate.to_s, OpenTelemetry::Baggage.value("a"), OpenTelemetry::Baggage.value("b")]
      request
    })
    headers = { "content-type" => ["application/proto"],
                "traceparent" => ["00-#{parent.context.hex_trace_id}-#{parent.context.hex_span_id}-01"],
                "tracestate" => ["first=one", "second=two"], "baggage" => ["a=one", "b=two"] }
    target.call(path: AsyncHelpers::ECHO::ECHO.path, http_method: "POST", headers: headers,
                input: StringIO.new(message.to_proto))
    expect(seen).to eq(["first=one,second=two", "one", "two"])
    expect(rpc_spans(exporter).last.parent_span_id).to eq(parent.context.span_id)
    fatal = Class.new(Exception) # rubocop:disable Lint/InheritException -- prove cancellation cleanup
    target = server(instrumentation: bus, echo: ->(*) { raise fatal })
    original_context = OpenTelemetry::Context.current
    expect do
      target.call(path: AsyncHelpers::ECHO::ECHO.path, http_method: "POST", headers: headers,
                  input: StringIO.new(message.to_proto))
    end.to raise_error(fatal)
    expect(OpenTelemetry::Context.current).to equal(original_context)
    expect(rpc_spans(exporter).size).to eq(2)
    expect(rpc_spans(exporter).last.status.code).to eq(OpenTelemetry::Trace::Status::ERROR)
  ensure
    parent&.finish
  end
  it "distinguishes structured replacement and empty 429 outcomes across the real generated caller" do
    [100, 1].each do |limit|
      exporter.reset
      target = server(instrumentation: bus, response_limit: limit,
                      echo: ->(*) { raise ConnectRPC::Error.new(:permission_denied, "private" * 100) })
      typed = client(ConnectRPC::Testing::Transport.new(server: target), "https://in-process.invalid",
                     instrumentation: bus)
      expect { typed.echo(message) }.to raise_error(ConnectRPC::Error) do |error|
        expect(error.code).to eq(limit == 1 ? "unavailable" : "resource_exhausted")
      end
      caller = rpc_spans(exporter).find { |span| span.kind == :client }
      receiver = rpc_spans(exporter).find { |span| span.kind == :server }
      expect(receiver.attributes["rpc.response.status_code"]).to eq(limit == 1 ? nil : "resource_exhausted")
      expect(caller.attributes["rpc.response.status_code"]).to eq(limit == 1 ? "unavailable" : "resource_exhausted")
      expect(rpc_spans(exporter).size).to eq(2)
    end
  end

  it "observes client validation/encoding failures and bare server preflight with bounded attributes" do
    typed = client(ConnectRPC::Testing::Transport.new(server: server(instrumentation: bus)),
                   "https://in-process.invalid", instrumentation: bus)
    [{ timeout: 0 }, { metadata: { "content-type" => "reserved" } }].each do |options|
      expect { typed.echo(message, **options) }.to raise_error(ArgumentError)
    end
    expect { typed.echo("wrong type") }.to raise_error(ConnectRPC::Error)
    rpc_spans(exporter).each do |span|
      expect(span.kind).to eq(:client)
      expect(span.status.code).to eq(OpenTelemetry::Trace::Status::ERROR)
      expect(Array(span.events)).to be_empty
    end
    target = server(instrumentation: bus)
    %w[GET POST].each do |verb|
      target.call(path: AsyncHelpers::ECHO::ECHO.path, http_method: verb, headers: {}, input: nil)
    end
    rpc_spans(exporter).last(2).each do |span|
      expect(span.kind).to eq(:server)
      expect(span.attributes).not_to have_key("rpc.response.status_code")
      expect(span.attributes["error.type"]).to eq("preflight_rejected")
    end
    expect(rpc_spans(exporter).size).to eq(5)
  end
end
