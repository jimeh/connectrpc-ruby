# frozen_string_literal: true

health = ConnectRPC::Health::Registry.new(max_watchers: 10)
[health.set("demo.v1.Demo", :serving), health.set("", :not_serving), health.shutdown?].inspect
health.status("demo.v1.Demo")&.to_proc
health.watcher_count.succ
ConnectRPC::Health::VERSION.size
binding = ConnectRPC::Bindings::GoogleProtobuf.new
server = ConnectRPC::Server.new(services: [ConnectRPC::Health::SERVICE], binding: binding,
                                handlers: { "/demo.v1.Demo/Call" => ->(request, _context) { request } }
                                  .merge(health.handlers))
server.inspect
health.shutdown

core = ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding)
client = Grpc::Health::V1::Health::Client.new(client: core)
request = Grpc::Health::V1::HealthCheckRequest.new(service: "demo.v1.Demo")
client.check(request, timeout: 1).message.status.inspect
client.list(Grpc::Health::V1::HealthListRequest.new).message.statuses["demo.v1.Demo"]&.status
client.watch(request) { |reader| reader.each { |message| message.status.inspect } }

# Health Watch handlers and other host code observe call cancellation.
registration = ConnectRPC::Context.new.on_cancel { nil }
registration.unregister
