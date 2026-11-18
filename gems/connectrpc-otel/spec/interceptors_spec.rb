# frozen_string_literal: true

RSpec.describe "OpenTelemetry with unary interceptors" do
  let(:outcomes) { [] }
  let(:recorder) do
    outcomes = self.outcomes
    Object.new.tap do |subscriber|
      subscriber.define_singleton_method(:start) do |call|
        Object.new.tap do |observer|
          observer.define_singleton_method(:processing_finished) { |outcome| outcomes << [call.role, outcome] }
        end
      end
    end
  end
  let(:telemetry) { tracing(subscribers: [recorder]) }
  let(:exporter) { telemetry[1] }
  let(:bus) { telemetry[2] }
  let(:message) { AsyncHelpers::MESSAGE.new(name: "payload") }

  def layer(role, &body)
    Object.new.tap do |interceptor|
      interceptor.define_singleton_method(role) { |call, &continuation| body.call(call, &continuation) }
    end
  end

  def in_process(target, interceptors)
    client(ConnectRPC::Testing::Transport.new(server: target), "https://in-process.invalid", instrumentation: bus,
                                                                                             interceptors: interceptors)
  end

  it "makes the RPC span current inside client and server interceptors" do
    seen = {}
    client_layer = layer(:client_unary) do |_call, &continuation|
      seen[:client] = current_span.context
      continuation.call
    end
    server_layer = layer(:server_unary) do |_call, &continuation|
      seen[:server] = current_span.context
      continuation.call
    end
    in_process(server(instrumentation: bus, interceptors: [server_layer]), [client_layer]).echo(message)
    spans = rpc_spans(exporter)
    caller = spans.find { |span| span.kind == :client }
    receiver = spans.find { |span| span.kind == :server }
    expect(seen[:client]).to have_attributes(trace_id: caller.trace_id, span_id: caller.span_id)
    expect(seen[:server]).to have_attributes(trace_id: receiver.trace_id, span_id: receiver.span_id)
  end

  it "keeps the handler original as the outcome error after an interceptor translates it" do
    original = RuntimeError.new("handler bug")
    translating = layer(:server_unary) do |_call, &continuation|
      continuation.call
    rescue RuntimeError
      raise ConnectRPC::Error.new(:invalid_argument, "Translated")
    end
    target = server(instrumentation: bus, interceptors: [translating], echo: ->(*) { raise original })
    expect { in_process(target, []).echo(message) }
      .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("invalid_argument") }
    _, outcome = outcomes.find { |role, _| role == :server }
    expect(outcome.error).to equal(original)
    expect(outcome.normalized_error.message).to eq("Translated")
    receiver = rpc_spans(exporter).find { |span| span.kind == :server }
    expect(receiver.attributes["rpc.status_code"]).to eq("invalid_argument")
    expect(receiver.status.code).to eq(OpenTelemetry::Trace::Status::UNSET)
  end
end
