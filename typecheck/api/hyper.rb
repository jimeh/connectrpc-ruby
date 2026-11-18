# frozen_string_literal: true

transport = ConnectRPC::Hyper::Transport.new(
  connections: :per_call, idle_timeout: nil, keepalive: ConnectRPC::Keepalive.new(time: 60), http2: false,
  connect_timeout: 5, ca_file: "ca.pem", certificate_chain: ["-----BEGIN CERTIFICATE-----"], private_key: "key"
)
pooled = ConnectRPC::Hyper::Transport.new
pooled.inspect.size
transport.streaming_capabilities.include?(:bidi_stream)
pooled.grpc_capabilities.include?(:unary)
response = pooled.call(ConnectRPC::TransportRequest.new(url: URI("https://example.invalid/demo.Service/Call"),
                                                        http_method: "POST", headers: {}, body: "",
                                                        response_limit: 1024, deadline: nil))
response.status.succ
request = ConnectRPC::StreamRequest.new(url: URI("https://example.invalid/demo.Service/Stream"), http_method: "POST",
                                        headers: {}, deadline: ConnectRPC::Deadline.new(5))
pooled.open_stream(request) { |exchange| exchange.response_head(timeout: 1).status }.succ
pooled.open_grpc_stream(request) { |exchange| exchange.response_trailers&.keys }&.size
client = ConnectRPC::Client.new(base_url: "https://example.invalid", binding: APIBinding.new, transport: pooled,
                                protocol: :grpc)
client.inspect
pooled.close(grace: 1).to_s
transport.close.to_s
ConnectRPC::Hyper::VERSION.size
