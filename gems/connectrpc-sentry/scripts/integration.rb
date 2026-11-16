# frozen_string_literal: true

# Runnable from the checkout or a fresh installed consumer with copied generated fixtures.
# The SDK uses DummyTransport and a .invalid DSN, so nothing leaves the process.
require "connectrpc/sentry"
require "connectrpc/testing"
require "connectrpc/bindings/google_protobuf"
require "echo_connect"

Sentry.init do |config|
  config.dsn = "https://public@sentry.invalid/1"
  config.transport.transport_class = Sentry::DummyTransport
  config.background_worker_threads = 0
  config.auto_session_tracking = false
  config.traces_sample_rate = 1.0
  config.sdk_logger = Sentry::Logger.new(File::NULL)
end
events = Sentry.get_current_client.transport.events
instrumentation = ConnectRPC::Instrumentation.new(subscribers: [ConnectRPC::Sentry::Subscriber.new])
binding = ConnectRPC::Bindings::GoogleProtobuf.new
service = Example::V1::EchoService
handlers = service.handlers(echo: ->(message, _) { message },
                            fail: ->(*) { raise "handler bug" })
server = ConnectRPC::Server.new(services: [service::SERVICE], binding: binding, handlers: handlers,
                                instrumentation: instrumentation)
transport = ConnectRPC::Testing::Transport.new(server: server)
core = ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding, transport: transport,
                              instrumentation: instrumentation)
typed = service::Client.new(client: core)
message = Example::V1::EchoMessage.new(name: "hello")

# Without a caller span the client only propagates, and the server owns a continued transaction.
raise "Generated round trip failed" unless typed.echo(message).message == message

receiver = events.grep(Sentry::TransactionEvent).last
trace = receiver.contexts.fetch(:trace)
unless receiver.transaction == "example.v1.EchoService/Echo" && trace[:op] == "rpc.server" &&
       trace[:trace_id] == Sentry.get_current_scope.propagation_context.trace_id
  raise "Owned server transaction did not continue the propagated trace"
end

puts "Untraced caller: one rpc.server transaction continuing the caller's propagation context"

# With a caller transaction the client records a child span; in-process, the server nests below it.
caller = Sentry.start_transaction(name: "caller", op: "task")
Sentry.get_current_scope.set_span(caller)
begin
  typed.fail(message)
rescue ConnectRPC::Error => e
  raise "Unexpected code #{e.code}" unless e.code == "internal"
end
raise "Caller span not restored" unless Sentry.get_current_scope.get_span.equal?(caller)

caller.finish
spans = events.grep(Sentry::TransactionEvent).last.spans
client = spans.find { |span| span[:op] == "rpc.client" }
nested = spans.find { |span| span[:op] == "rpc.server" }
unless client && nested && client[:parent_span_id] == caller.span_id && nested[:parent_span_id] == client[:span_id]
  raise "Caller, client and server spans are not one tree"
end
raise "Wrong status" unless [client, nested].all? { |span| span[:status] == "internal_error" }

errors = events.grep(Sentry::ErrorEvent)
unless errors.size == 1 && errors.first.contexts.dig(:trace, :span_id) == nested[:span_id]
  raise "Handler bug was not captured exactly once on the RPC span"
end

puts "Traced caller: rpc.client and nested rpc.server spans, one linked handler error event"
puts "2 generated scenarios, 2 transactions and 1 error event passed (Ruby #{RUBY_VERSION}, sentry-ruby " \
     "#{Sentry::VERSION})"
