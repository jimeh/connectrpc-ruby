# frozen_string_literal: true

# A Connect stream exchange has no gRPC trailers, also through ThreadedTransport.
ConnectRPC::Async::ThreadedTransport.new(origin: "https://example.invalid").open_stream(
  ConnectRPC::StreamRequest.new(url: URI("https://example.invalid/demo.Service/Watch"), http_method: "POST",
                                headers: {})
) { |exchange| exchange.response_trailers.to_s }
