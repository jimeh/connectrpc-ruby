# frozen_string_literal: true

# Runnable from the checkout or a fresh installed consumer with copied generated fixtures.
require "connectrpc/otel"
require "connectrpc/testing"
require "connectrpc/bindings/google_protobuf"
require "echo_connect"
require "opentelemetry/sdk"
require "opentelemetry/sdk/trace/export/in_memory_span_exporter"

exporter = OpenTelemetry::SDK::Trace::Export::InMemorySpanExporter.new
provider = OpenTelemetry::SDK::Trace::TracerProvider.new
provider.add_span_processor(OpenTelemetry::SDK::Trace::Export::SimpleSpanProcessor.new(exporter))
subscriber = ConnectRPC::OTel::Subscriber.new(tracer_provider: provider)
instrumentation = ConnectRPC::Instrumentation.new(subscribers: [subscriber])
binding = ConnectRPC::Bindings::GoogleProtobuf.new
service = Example::V1::EchoService
handlers = service.handlers(echo: ->(message, _) { message },
                            fail: ->(*) { raise ConnectRPC::Error.new(:invalid_argument, "safe failure") })
server = ConnectRPC::Server.new(services: [service::SERVICE], binding: binding, handlers: handlers,
                                encoding: :gzip, instrumentation: instrumentation)
transport = ConnectRPC::Testing::Transport.new(server: server)
%i[proto json].product(%i[identity gzip]).each do |codec, encoding|
  core = ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding, transport: transport,
                                codec: codec, encoding: encoding, instrumentation: instrumentation)
  typed = service::Client.new(client: core)
  message = Example::V1::EchoMessage.new(name: "hello")
  raise "Generated round trip failed" unless typed.echo(message).message == message

  caller, receiver = exporter.finished_spans.last(2).sort_by { |span| span.kind == :client ? 0 : 1 }
  unless receiver.parent_span_id == caller.span_id && receiver.trace_id == caller.trace_id
    raise "Serialized parent lost"
  end
  raise "Wrong RPC semantics" unless [caller, receiver].all? do |span|
    span.attributes["rpc.system.name"] == "connectrpc"
  end
  raise "Ambient context leaked" if OpenTelemetry::Trace.current_span.context.valid?

  puts "Generated #{codec}/#{encoding}: two SDK-exported RPC spans with serialized parentage"
end
raise "Duplicate/missing exports" unless exporter.finished_spans.size == 8

puts "4 generated scenarios, 8 real SDK RPC spans passed (Ruby #{RUBY_VERSION})"
