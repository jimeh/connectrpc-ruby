# frozen_string_literal: true

# One PerCallTransport serves every origin; each call opens and closes its own
# connection, from a plain thread or inline in an Async task.
transport = ConnectRPC::Async::PerCallTransport.new(ca_file: "ca.pem", certificate_chain: ["client"],
                                                    private_key: "key", connect_timeout: 5)
ConnectRPC::Async::PerCallTransport.new
method = ConnectRPC::Method.new(service: "demo.Service", name: "Call", request_type: String, response_type: String,
                                idempotency: :no_side_effects)
client = ConnectRPC::Client.new(base_url: "https://device-1.example.invalid", binding: APIBinding.new,
                                transport: transport, use_get: true)
Thread.new { client.call(method, "request", timeout: 2).message.to_s }.join
unary = ConnectRPC::TransportRequest.new(url: URI("https://device-2.example.invalid/demo.Service/Call"),
                                         http_method: "POST", headers: {}, body: "", response_limit: 1024)
response = transport.call(unary)
response.status.succ
stream = ConnectRPC::ServerStreamMethod.new(service: "demo.Service", name: "Watch", request_type: String,
                                            response_type: String)
Thread.new { client.server_stream(stream, "request", timeout: 2) { |reader| reader.each(&:upcase) } }.join
grpc = ConnectRPC::Client.new(base_url: "https://device-3.example.invalid", binding: APIBinding.new,
                              transport: transport, protocol: :grpc)
Thread.new { grpc.call(method, "request", timeout: 2).message.to_s }.join
transport.streaming_capabilities.each(&:to_s)
transport.grpc_capabilities.each(&:to_s)
request = ConnectRPC::StreamRequest.new(url: URI("https://device-4.example.invalid/demo.Service/Watch"),
                                        http_method: "POST", headers: {})
# @type var length: Integer?
length = transport.open_stream(request) do |exchange|
  exchange.write("opaque")
  exchange.close_send
  exchange.response_head(timeout: 1).status.succ
  bytes = exchange.read(1, timeout: 1)
  exchange.finish(timeout: 1)
  exchange.abort
  bytes&.bytesize
end
length&.succ
# @type var trailers: Integer?
trailers = transport.open_grpc_stream(request) do |exchange|
  exchange.response_head(timeout: nil).end_stream
  exchange.read(1, timeout: nil)&.bytesize
  exchange.finish(timeout: nil)
  exchange.response_trailers&.size
end
trailers&.succ
