# frozen_string_literal: true

ConnectRPC::Async::HTTP1Transport.new(origin: "https://example.invalid").open_stream(
  ConnectRPC::StreamRequest.new(url: URI("https://example.invalid"), http_method: "POST", headers: {})
)
