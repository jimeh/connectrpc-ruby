# frozen_string_literal: true

server = ConnectRPC::Server.new(services: [], binding: ConnectRPC::Bindings::GoogleProtobuf.new)
endpoint = ConnectRPC::Falcon::Endpoint.parse("http://127.0.0.1:9292")
ConnectRPC::Falcon::Server.new(server: server, endpoint: endpoint, rack_app: lambda { |env|
  [200, { "content-type" => "text/plain", "x-repeated" => %w[a b] }, [env.fetch("PATH_INFO").to_s]]
})
# @type var body: ^(ConnectRPC::Falcon::_RackStream) -> void
body = lambda { |stream|
  stream.write("ordinary callable body")
  stream.close
}
ConnectRPC::Falcon::Server.new(server: server, endpoint: endpoint, rack_app: ->(_env) { [200, {}, body] })
