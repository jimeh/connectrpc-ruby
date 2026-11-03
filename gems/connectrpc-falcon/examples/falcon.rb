# frozen_string_literal: true

# Run with: bundle exec falcon host examples/falcon.rb
require "connectrpc/falcon"
require "connectrpc/bindings/google_protobuf"
require "google/protobuf/wrappers_pb"

service "connectrpc-echo" do
  include ConnectRPC::Falcon::Environment

  url { ENV.fetch("CONNECTRPC_URL", "http://127.0.0.1:9292") }
  count 2

  # Application initialization runs in each Falcon worker before TLS loads.
  prepare! do |_instance|
    # Initialize application-owned dependencies here, after fork.
  end

  connectrpc_server do
    method = ConnectRPC::Method.new(service: "demo.EchoService", name: "Echo",
                                    request_type: Google::Protobuf::StringValue,
                                    response_type: Google::Protobuf::StringValue, idempotency: :no_side_effects)
    service = ConnectRPC::Service.new(name: "demo.EchoService", methods: [method])
    ConnectRPC::Server.new(services: [service], binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                           handlers: { method.path => ->(message, _context) { message } })
  end

  rack_app do
    ->(_env) { [200, { "content-type" => "text/plain" }, ["Ordinary Rack fallback\n"]] }
  end
end
