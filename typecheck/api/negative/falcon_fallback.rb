# frozen_string_literal: true

ConnectRPC::Falcon::Server.new(
  server: ConnectRPC::Server.new(services: [], binding: APIBinding.new),
  endpoint: ConnectRPC::Falcon::Endpoint.parse("http://127.0.0.1:9292"), rack_app: 42
)
