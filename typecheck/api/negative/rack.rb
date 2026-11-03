# frozen_string_literal: true

ConnectRPC::Rack.new(lambda { |_env|
  ["200", {}, []]
}, server: ConnectRPC::Server.new(services: [], binding: APIBinding.new))
