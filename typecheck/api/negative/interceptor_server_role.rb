# frozen_string_literal: true

ConnectRPC::Server.new(services: [], binding: APIBinding.new, interceptors: [APIClientInterceptor.new])
