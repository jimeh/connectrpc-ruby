# frozen_string_literal: true

# One ThreadedTransport is shared by every caller thread; it needs no reactor.
transport = ConnectRPC::Async::ThreadedTransport.new(
  origin: "https://example.invalid", ca_file: "ca.pem", certificate_chain: ["client"], private_key: "key",
  connection_limit: 2, max_requests: 64, connect_timeout: 5,
  connection_policy: ConnectRPC::Async::ConnectionPolicy.new(wait_for_ready: true),
  connection_observer: APIConnectionObserver.new, stream_receive_window: 1_048_576,
  connection_receive_window: 16_777_216, dynamic_receive_windows: true, max_receive_window: 8_388_608
)
method = ConnectRPC::Method.new(service: "demo.Service", name: "Call", request_type: String, response_type: String,
                                idempotency: :no_side_effects)
client = ConnectRPC::Client.new(base_url: "https://example.invalid", binding: APIBinding.new, transport: transport,
                                use_get: true)
Thread.new { client.call(method, "request", timeout: 2).message.to_s }.join
response = transport.call(ConnectRPC::TransportRequest.new(url: URI("https://example.invalid/demo.Service/Call"),
                                                           http_method: "POST", headers: {}, body: "",
                                                           response_limit: 1024))
response.status.succ
# @type var state: ConnectRPC::Async::connection_state
state = transport.connection_state
state.to_s
ConnectRPC::Async::ThreadedTransport.new(origin: "http://example.invalid", connection_observer: lambda { |event|
  event.type.to_s
  nil
})
stream = ConnectRPC::ServerStreamMethod.new(service: "demo.Service", name: "Watch", request_type: String,
                                            response_type: String)
Thread.new { client.server_stream(stream, "request", timeout: 2) { |reader| reader.each(&:upcase) } }.join
grpc = ConnectRPC::Client.new(base_url: "https://example.invalid", binding: APIBinding.new, transport: transport,
                              protocol: :grpc)
Thread.new { grpc.call(method, "request", timeout: 2).message.to_s }.join
transport.streaming_capabilities.each(&:to_s)
transport.grpc_capabilities.each(&:to_s)
request = ConnectRPC::StreamRequest.new(url: URI("https://example.invalid/demo.Service/Watch"), http_method: "POST",
                                        headers: {})
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
transport.open_grpc_stream(request) do |exchange|
  exchange.response_head(timeout: nil).end_stream
  exchange.read(1, timeout: nil)&.bytesize
  exchange.response_trailers&.each_value(&:size)
  exchange.finish(timeout: nil)
end
transport.close(grace: 5) || transport.close
