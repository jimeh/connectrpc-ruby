# frozen_string_literal: true

ConnectRPC::Server.new(services: [], binding: APIBinding.new).call_grpc_web(
  path: "/typed.API/Call", http_method: "POST", headers: {}, input: APIStreamExchange.new,
  output: APIWebOutput.new, publish: APIStreamPublisher.new, capabilities: [:bidi_stream]
)
