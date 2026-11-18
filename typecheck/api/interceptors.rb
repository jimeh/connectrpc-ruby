# frozen_string_literal: true

binding = APIBinding.new
method = ConnectRPC::Method.new(service: "demo.Service", name: "Call", request_type: String, response_type: String)
service = ConnectRPC::Service.new(name: method.service, rpcs: [method])
client = ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding,
                                interceptors: [APIClientInterceptor.new, APICachingInterceptor.new])
ConnectRPC::Server.new(services: [service], binding: binding, interceptors: [APIServerInterceptor.new])
begin
  client.call(method, "request").message
rescue ConnectRPC::InterceptorStateError => e
  e.message
end

server_stream = ConnectRPC::ServerStreamMethod.new(service: "demo.Service", name: "Watch", request_type: String,
                                                   response_type: String)
stream_client = ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding,
                                       interceptors: [APIClientInterceptor.new, APIClientStreamInterceptor.new])
stream_client.server_stream(server_stream, "request") { |reader| reader.each(&:size) }
ConnectRPC::Server.new(services: [service], binding: binding,
                       interceptors: [APIServerInterceptor.new, APIServerStreamInterceptor.new])
