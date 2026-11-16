# frozen_string_literal: true

binding = APIBinding.new
method = ConnectRPC::Method.new(service: "demo.Service", name: "Call", request_type: String, response_type: String)
service = ConnectRPC::Service.new(name: method.service, methods: [method])
client = ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding,
                                interceptors: [APIClientInterceptor.new, APICachingInterceptor.new])
ConnectRPC::Server.new(services: [service], binding: binding, interceptors: [APIServerInterceptor.new])
begin
  client.call(method, "request").message
rescue ConnectRPC::InterceptorStateError => e
  e.message
end
