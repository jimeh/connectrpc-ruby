# frozen_string_literal: true

RSpec.describe "OpenTelemetry with stream interceptors over the Async host" do
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

  def watch
    @watch ||= ConnectRPC::ServerStreamMethod.new(service: "trace.Streams", name: "Watch",
                                                  request_type: AsyncHelpers::MESSAGE,
                                                  response_type: AsyncHelpers::MESSAGE)
  end

  def layer(role, &body)
    Object.new.tap do |interceptor|
      interceptor.define_singleton_method(role) { |call, &continuation| body.call(call, &continuation) }
    end
  end

  # Runs one Watch over real h2c and waits for both spans.
  def run_watch(handler, server_interceptors: [], client_interceptors: [])
    reactor do |task|
      target = ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: "trace.Streams", rpcs: [watch])],
                                      handlers: { watch.path => handler }, instrumentation: bus,
                                      binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                                      interceptors: server_interceptors)
      host = ConnectRPC::Async::Host.new(server: target).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      core = ConnectRPC::Client.new(base_url: origin, transport: transport, instrumentation: bus,
                                    binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                                    interceptors: client_interceptors)
      yield core
      wait_until(task, "client and server spans") { rpc_spans(exporter).size == 2 }
    ensure
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  def server_span = rpc_spans(exporter).find { |span| span.kind == :server }
  def client_span = rpc_spans(exporter).find { |span| span.kind == :client }

  it "makes the RPC span current inside client and server stream interceptors" do
    seen = {}
    client_layer = layer(:client_stream) do |_call, &continuation|
      seen[:client] = current_span.context
      continuation.call
    end
    server_layer = layer(:server_stream) do |_call, &continuation|
      seen[:server] = current_span.context
      continuation.call
    end
    handler = ->(message, writer, _context) { writer.send_message(message) }
    run_watch(handler, server_interceptors: [server_layer], client_interceptors: [client_layer]) do |core|
      received = []
      core.server_stream(watch, AsyncHelpers::MESSAGE.new(name: "span")) do |reader|
        reader.each { |message| received << message.name }
      end
      expect(received).to eq(["span"])
    end
    expect(seen[:client]).to have_attributes(trace_id: client_span.trace_id, span_id: client_span.span_id)
    expect(seen[:server]).to have_attributes(trace_id: server_span.trace_id, span_id: server_span.span_id)
  end

  { internal: OpenTelemetry::Trace::Status::ERROR, invalid_argument: OpenTelemetry::Trace::Status::UNSET }
    .each do |code, status|
    it "keeps the stream handler's original as the outcome error after a translation to #{code}" do
      original = RuntimeError.new("stream handler bug")
      translating = layer(:server_stream) do |_call, &continuation|
        continuation.call
      rescue RuntimeError
        raise ConnectRPC::Error.new(code, "Translated")
      end
      run_watch(->(*) { raise original }, server_interceptors: [translating]) do |core|
        expect { core.server_stream(watch, AsyncHelpers::MESSAGE.new(name: "bug")) { |reader| reader.each { nil } } }
          .to raise_error(ConnectRPC::Error) { |error| expect([error.code, error.message]).to eq([code.to_s, "Translated"]) }
      end
      _, outcome = outcomes.find { |role, _| role == :server }
      expect(outcome.error).to equal(original)
      expect(outcome.normalized_error.message).to eq("Translated")
      expect(outcome.code).to eq(code.to_s)
      expect(server_span.attributes["rpc.status_code"]).to eq(code.to_s)
      expect(server_span.status.code).to eq(status)
    end
  end
end
