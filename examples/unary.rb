# frozen_string_literal: true

require_relative "echo_schema"
require_relative "localhost"

handler = lambda do |request, context|
  context.response_metadata.add("x-example", "localhost")
  context.response_trailers.add("x-finished", "yes")
  EchoExample::Message.new(name: "Hello, #{request.name}!", count: request.count)
end
server = ConnectRPC::Server.new(services: [EchoExample::SERVICE], binding: EchoExample::BINDING,
                                handlers: { EchoExample::ECHO.path => handler })
host = EchoExample::Localhost.new(ConnectRPC::Rack.new(server: server))
begin
  %i[proto json].each do |codec|
    client = ConnectRPC::Client.new(base_url: host.url, binding: EchoExample::BINDING, codec: codec, use_get: true)
    response = client.call(EchoExample::ECHO, EchoExample::Message.new(name: "Ruby", count: 9_007_199_254_740_993),
                           timeout: 2)
    puts "#{codec}: #{response.message.name} count=#{response.message.count} trailers=#{response.trailers.to_h}"
  end
ensure
  host.close
end
