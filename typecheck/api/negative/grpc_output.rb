# frozen_string_literal: true

ConnectRPC::Server.new(services: [], binding: APIBinding.new).call_grpc(
  path: "/typed.GRPC/Call", http_method: "POST", headers: {}, input: APIStreamExchange.new,
  output: BadGRPCOutput.new, publish: ->(_) {}, http2: true
)
