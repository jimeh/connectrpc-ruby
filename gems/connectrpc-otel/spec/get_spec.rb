# frozen_string_literal: true

RSpec.describe "GET RPC observation" do
  let(:telemetry) { tracing }
  let(:provider) { telemetry[0] }
  let(:exporter) { telemetry[1] }
  let(:bus) { telemetry[2] }

  %i[proto json].each do |codec|
    it "preserves #{codec} parent/outcomes and measures compressed query payload without URL attributes" do
      parent = provider.tracer("test").start_span("parent")
      target = server(instrumentation: bus, encoding: :gzip)
      observed = []
      real = ConnectRPC::Testing::Transport.new(server: target)
      transport = lambda { |request|
        observed << request
        real.call(request)
      }
      typed = client(transport, "https://in-process.invalid", codec: codec, encoding: :gzip,
                                                              use_get: true, instrumentation: bus)
      message = AsyncHelpers::MESSAGE.new(name: "private GET query payload")
      OpenTelemetry::Trace.with_span(parent) { expect(typed.echo(message).message).to eq(message) }
      spans = rpc_spans(exporter)
      expect(spans.size).to eq(2)
      caller = spans.find { |span| span.kind == :client }
      receiver = spans.find { |span| span.kind == :server }
      expect(caller.parent_span_id).to eq(parent.context.span_id)
      expect(receiver.parent_span_id).to eq(caller.span_id)
      payload = URI.decode_www_form(observed.first.url.query).to_h.fetch("message")
      bytes = ConnectRPC::Metadata.decode_binary(payload.tr("-_", "+/"))
      expect(observed.first.body).to eq("")
      spans.each do |span|
        expect(span.attributes).to include("rpc.response.status_code" => "OK", "connectrpc.delivery" => "unobserved",
                                           "connectrpc.request.wire_bytes" => bytes.bytesize)
        expect(span.attributes.keys).not_to include("url.full", "url.query", "http.url")
        expect(span.attributes.to_s).not_to include("private GET", "message=", "base64=", "compression=")
      end
      expect(current_span.context).not_to be_valid
    ensure
      parent&.finish
    end
  end

  %i[async falcon].each do |backend|
    it "retains #{backend} HTTP/2 GET parent and native delivery completion" do
      reactor do |task|
        target = server(instrumentation: bus)
        runner = lambda do |host, origin|
          transport = ConnectRPC::Async::Transport.new(origin: origin)
          typed = client(transport, origin, use_get: true, instrumentation: bus)
          parent = provider.tracer("test").start_span("parent")
          OpenTelemetry::Trace.with_span(parent) do
            expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "private GET")).message.name).to eq("private GET")
          end
          wait_until(task, "GET native completion") { rpc_spans(exporter).size == 2 }
          caller = rpc_spans(exporter).find { |span| span.kind == :client }
          receiver = rpc_spans(exporter).find { |span| span.kind == :server }
          expect(caller.parent_span_id).to eq(parent.context.span_id)
          expect(receiver.parent_span_id).to eq(caller.span_id)
          expect(receiver.attributes).to include("rpc.response.status_code" => "OK",
                                                 "connectrpc.delivery" => "completed")
          expect(receiver.attributes["connectrpc.request.wire_bytes"]).to be_positive
          expect(receiver.attributes.to_s).not_to include("private GET", "message=")
          expect(host.close(grace: 1)).to be(true)
        ensure
          parent&.finish
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

  it "finishes Falcon HTTP/1 GET after native wire completion" do
    reactor do |task|
      falcon(server: server(instrumentation: bus)) do |_host, origin|
        parent = provider.tracer("test").start_span("parent")
        carrier = {}
        OpenTelemetry::Trace::Propagation::TraceContext.text_map_propagator.inject(
          carrier, context: OpenTelemetry::Trace.context_with_span(parent)
        )
        query = "connect=v1&encoding=proto&base64=1&message="
        status, = http1(origin, "#{AsyncHelpers::ECHO::ECHO.path}?#{query}", headers: carrier, body: nil)
        expect(status).to eq(200)
        wait_until(task, "HTTP/1 GET completion") { rpc_spans(exporter).size == 1 }
        span = rpc_spans(exporter).first
        expect(span.parent_span_id).to eq(parent.context.span_id)
        expect(span.attributes).to include("connectrpc.delivery" => "completed", "connectrpc.request.wire_bytes" => 0)
      ensure
        parent&.finish
      end
    end
  end
end
