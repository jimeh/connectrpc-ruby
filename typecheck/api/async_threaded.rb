# frozen_string_literal: true

# One ThreadedTransport is shared by every caller thread; it needs no reactor.
transport = ConnectRPC::Async::ThreadedTransport.new(
  origin: "https://example.invalid", ca_file: "ca.pem", certificate_chain: ["client"], private_key: "key",
  connection_limit: 2, max_requests: 64, connect_timeout: 5,
  connection_policy: ConnectRPC::Async::ConnectionPolicy.new(wait_for_ready: true),
  connection_observer: APIConnectionObserver.new
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
transport.close(grace: 5) || transport.close
