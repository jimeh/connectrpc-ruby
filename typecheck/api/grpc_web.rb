# frozen_string_literal: true

policy = ConnectRPC::GRPCWebCORS.new(origins: ["https://app.example"], credentials: true,
                                     request_headers: ["authorization"], expose_headers: ["x-leading"])
policy.origins.each(&:upcase)
policy.request_headers.each(&:upcase)
policy.expose_headers.each(&:upcase)
# @type var credentials: bool
policy.credentials
server = ConnectRPC::Server.new(services: [], binding: APIBinding.new, protocol: %i[connect grpc_web],
                                status_codec: APIStatusCodec.new, cors: policy)
server.cors_response(path: "/typed.API/Call", http_method: "OPTIONS", headers: {})&.status&.succ
output = APIWebOutput.new
# @type var capability: ConnectRPC::_GRPCWebOutput
capability = output
capability.write("data", timeout: nil)
capability.finish(terminal: "prepared")
server.call_grpc_web(path: "/typed.API/Call", http_method: "POST", headers: {}, input: APIStreamExchange.new,
                     output: output, capabilities: %i[unary server_stream], publish: APIStreamPublisher.new)

server.start_call(path: "/typed.API/Call", headers: {})&.finish(delivery: :unobserved)
server.start_call(path: "/typed.API/Call", headers: {}, http_method: "GET")&.finish(delivery: :unobserved)
