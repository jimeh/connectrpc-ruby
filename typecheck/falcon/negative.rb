# frozen_string_literal: true

server = ConnectRPC::Server.new(services: [], binding: ConnectRPC::Bindings::GoogleProtobuf.new)
endpoint = ConnectRPC::Falcon::Endpoint.parse("http://127.0.0.1:9292")
ConnectRPC::Falcon::Server.new(server: server, endpoint: endpoint, rack_app: ->(_env) { [200, {}, 42] })
