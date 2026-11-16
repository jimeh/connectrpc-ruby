# frozen_string_literal: true

# Runnable from the checkout or a fresh installed consumer with copied generated fixtures.
require "connectrpc/yabeda"
require "connectrpc/testing"
require "connectrpc/bindings/google_protobuf"
require "echo_connect"
require "yabeda/testing"

# Declared before Yabeda.configure!, as an application initializer would.
metrics = ConnectRPC::Yabeda::Metrics.new
Yabeda.configure!
failures = []
instrumentation = ConnectRPC::Instrumentation.new(subscribers: [metrics.subscriber],
                                                  on_error: ->(error) { failures << error })
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

  begin
    typed.fail(message)
    raise "Expected a failure"
  rescue ConnectRPC::Error => e
    raise "Wrong failure" unless e.code == "invalid_argument"
  end
  puts "Generated #{codec}/#{encoding}: echo and failure recorded"
end

adapter = Yabeda::TestAdapter.instance
labels = { grpc_type: "unary", grpc_service: "example.v1.EchoService", grpc_method: "Echo", rpc_protocol: "connect" }
%w[server client].each do |role|
  counters = adapter.counters
  started = counters.fetch(Yabeda.metrics.fetch("grpc_#{role}_started_total")).fetch(labels)
  handled = counters.fetch(Yabeda.metrics.fetch("grpc_#{role}_handled_total")).fetch(labels.merge(grpc_code: "OK"))
  failed = counters.fetch(Yabeda.metrics.fetch("grpc_#{role}_handled_total"))
                   .fetch(labels.merge(grpc_method: "Fail", grpc_code: "InvalidArgument"))
  raise "Wrong #{role} counts: #{[started, handled, failed]}" unless [started, handled, failed] == [4, 4, 4]

  size = adapter.histograms.fetch(Yabeda.metrics.fetch("grpc_#{role}_msg_sent")).fetch(labels)
  raise "Missing #{role} message size" unless size.positive?
end
raise "Recorder failures: #{failures.map(&:message)}" unless failures.empty?

puts "8 generated RPCs recorded through the Yabeda test adapter (Ruby #{RUBY_VERSION})"
