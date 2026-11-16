# frozen_string_literal: true

# Runnable from the checkout or a fresh installed consumer.
require "connectrpc/health"
require "connectrpc/testing"
require "connectrpc/bindings/google_protobuf"

health = ConnectRPC::Health::Registry.new
health.set("demo.v1.Demo", :not_serving)
binding = ConnectRPC::Bindings::GoogleProtobuf.new
server = ConnectRPC::Server.new(services: [ConnectRPC::Health::SERVICE], binding: binding, handlers: health.handlers)
transport = ConnectRPC::Testing::Transport.new(server: server)
service = Grpc::Health::V1::Health
%i[proto json].each do |codec|
  core = ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding, transport: transport,
                                codec: codec)
  client = service::Client.new(client: core)
  status = client.check(Grpc::Health::V1::HealthCheckRequest.new(service: "demo.v1.Demo")).message.status
  raise "Check returned #{status}" unless status == :NOT_SERVING

  begin
    client.check(Grpc::Health::V1::HealthCheckRequest.new(service: "missing"))
    raise "Expected not_found"
  rescue ConnectRPC::Error => e
    raise "Wrong failure #{e.code}" unless e.code == "not_found"
  end
  listed = client.list(Grpc::Health::V1::HealthListRequest.new).message.statuses
  raise "List returned #{listed.keys}" unless listed.keys.sort == ["", "demo.v1.Demo"]

  puts "Generated #{codec}: Check, unknown Check and List"
end
health.shutdown
raise "Shutdown did not mark entries" unless health.status("") == :not_serving && !health.set("", :serving)

puts "Installed health registry and generated client passed (Ruby #{RUBY_VERSION})"
