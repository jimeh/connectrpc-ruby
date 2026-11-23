# frozen_string_literal: true

transport = ConnectRPC::Async::HTTP1Transport.new(origin: "https://example.invalid", ca_file: "ca.pem",
                                                  certificate_chain: ["client"], private_key: "key",
                                                  connection_limit: 4, max_requests: 128, connect_timeout: 10)
client = ConnectRPC::Client.new(base_url: "https://example.invalid", binding: APIBinding.new, transport: transport,
                                use_get: true)
# Since 0.3.0, https:// origins tunnel through an http:// proxy and http://
# origins send absolute-form requests to it.
ConnectRPC::Async::HTTP1Transport.new(origin: "https://example.invalid", proxy: "http://proxy.invalid:3128").inspect.size
ConnectRPC::Async::HTTP1Transport.new(origin: "http://example.invalid", proxy: URI("http://proxy.invalid:3128"))
ConnectRPC::Async::HTTP1Transport.new(origin: "http://example.invalid", proxy: nil)
ConnectRPC::Async::HTTP1Transport.new(origin: "http://example.invalid", proxy: :environment)
method = ConnectRPC::ServerStreamMethod.new(service: "test.Service", name: "Watch", request_type: String,
                                            response_type: String)
client.server_stream(method, "query", timeout: nil) { |reader| reader.each(&:upcase) }
transport.open_stream(ConnectRPC::StreamRequest.new(url: URI("https://example.invalid/stream"), http_method: "POST",
                                                    headers: {})) do |exchange|
  exchange.write("opaque")
  exchange.close_send
  exchange.response_head(timeout: 1).status.succ
  exchange.read(1, timeout: 1)&.upcase
  exchange.finish(timeout: 1)
  exchange.abort
end
transport.streaming_capabilities.each(&:to_s)
transport.close(grace: 0)
