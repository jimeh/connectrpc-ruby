# frozen_string_literal: true

require "greeting_connect"
require "connectrpc/bindings/google_protobuf"
require_relative "localhost"

service = Greeting::V1::GreetingService
type = Greeting::V1::Greeting
binding = ConnectRPC::Bindings::GoogleProtobuf.new
handler = lambda do |request, context|
  context.response_trailers.add("x-finished", "yes")
  type.new(name: "Hello, #{request.name}!")
end
failure = lambda do |request, _context|
  detail = ConnectRPC::ErrorDetail.new(type: type.descriptor.name, value: request.to_proto)
  raise ConnectRPC::Error.new(:invalid_argument, "deliberate", details: [detail])
end
server = ConnectRPC::Server.new(services: [service::SERVICE], binding: binding,
                                handlers: service.handlers(greet: handler, fail: failure))
host = EchoExample::Localhost.new(ConnectRPC::Rack.new(server: server))
begin
  %i[proto json].each do |codec|
    client = service::Client.new(client: ConnectRPC::Client.new(base_url: host.url, binding: binding, codec: codec))
    response = client.greet(type.new(name: "Ruby"), timeout: 2)
    puts "#{codec}: #{response.message.name} trailers=#{response.trailers.to_h}"
    begin
      client.fail(type.new(name: "bad input"), timeout: 2)
      raise "Expected deliberate error"
    rescue ConnectRPC::Error => e
      detail = e.details.first.decode(binding: binding, type: type)
      puts "#{codec}: #{e.code}: #{e.message}, typed detail=#{detail.name}"
    end
  end
ensure
  host.close
end
