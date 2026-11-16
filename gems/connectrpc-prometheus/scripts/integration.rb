# frozen_string_literal: true

# Runnable from the checkout or a fresh installed consumer with copied generated fixtures.
require "connectrpc/prometheus"
require "connectrpc/testing"
require "connectrpc/bindings/google_protobuf"
require "echo_connect"
require "prometheus/client/formats/text"

registry = Prometheus::Client::Registry.new
metrics = ConnectRPC::Prometheus::Metrics.new(registry: registry)
instrumentation = ConnectRPC::Instrumentation.new(subscribers: [metrics.subscriber])
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

text = Prometheus::Client::Formats::Text.marshal(registry)
labels = 'grpc_type="unary",grpc_service="example.v1.EchoService",grpc_method="%s",rpc_protocol="connect"'
expected = %w[server client].flat_map do |role|
  ["grpc_#{role}_started_total{#{format(labels, 'Echo')}} 4.0",
   "grpc_#{role}_handled_total{#{format(labels, 'Echo')},grpc_code=\"OK\"} 4.0",
   "grpc_#{role}_handled_total{#{format(labels, 'Fail')},grpc_code=\"InvalidArgument\"} 4.0",
   "grpc_#{role}_msg_sent_total{#{format(labels, 'Echo')}} 4.0",
   "grpc_#{role}_handling_seconds_count{#{format(labels, 'Echo')}} 4.0"]
end
missing = expected.reject { |line| text.lines.map(&:chomp).include?(line) }
raise "Missing exposition lines:\n#{missing.join("\n")}\n---\n#{text}" unless missing.empty?

puts "8 generated RPCs exported #{text.lines.count { |line| line.start_with?('grpc_') }} Prometheus samples " \
     "(Ruby #{RUBY_VERSION})"
