# frozen_string_literal: true

ConnectRPC::Async::Host.new(server: ConnectRPC::Server.new(services: [], binding: APIBinding.new), fallback: 42)
