# frozen_string_literal: true

ConnectRPC::Server.new(services: [], binding: APIBinding.new, invoker: ->(rpc, _request, _context) { rpc.abs })
