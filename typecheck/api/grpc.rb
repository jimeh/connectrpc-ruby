# frozen_string_literal: true

# A composed token binding and gRPC-only transport deliberately require no call.
transport = APIGRPCOnlyTransport.new
# @type var capability: ConnectRPC::_GRPCTransport
capability = transport
codec = APIStatusCodec.new
core = ConnectRPC::Client.new(base_url: "https://typed.invalid", binding: APIBinding.new, transport: capability,
                              protocol: :grpc, status_codec: codec, grpc_status_wire_limit: 65_536,
                              grpc_status_limit: 65_536)
# @type var method: ConnectRPC::Method[String, String]
method = ConnectRPC::Method.new(service: "typed.GRPC", name: "Call", request_type: String, response_type: String)
core.call(method, "request").message.upcase
ConnectRPC::Server.new(services: [], binding: APIBinding.new, protocol: :auto, status_codec: codec)
head = ConnectRPC::GRPCHead.new(status: 200, headers: {}, end_stream: true)
head.status.succ
head.headers.each_value(&:size)
# @type var ended: bool
head.end_stream
codec.decode_status(codec.encode_status(ConnectRPC::Error.new(:internal)), code: 13).details.each(&:type)
request = ConnectRPC::StreamRequest.new(url: URI("https://typed.invalid"), http_method: "POST", headers: {})
capability.open_grpc_stream(request) do |exchange|
  exchange.response_head(timeout: nil).end_stream
  exchange.close_send
  exchange.read(1, timeout: nil)&.bytesize
  exchange.response_trailers&.each_value(&:size)
  exchange.finish(timeout: nil)
end
server = ConnectRPC::Server.new(services: [], binding: APIBinding.new, protocol: :grpc, status_codec: codec)
server.call_grpc(path: "/typed.GRPC/Call", http_method: "POST", headers: {}, input: APIStreamExchange.new,
                 output: APIGRPCOutput.new, publish: APIGRPCPublisher.new, http2: true)
