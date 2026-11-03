# frozen_string_literal: true

ConnectRPC::Client.new(base_url: "https://typed.invalid", binding: APIBinding.new).server_stream(
  ConnectRPC::ServerStreamMethod.new(service: "typed.Streams", name: "Watch", request_type: String,
                                     response_type: String), "request"
)
