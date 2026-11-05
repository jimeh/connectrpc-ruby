# frozen_string_literal: true

server = ConnectRPC::Server.new(services: [], binding: APIBinding.new)
server.call_stream(path: "/typed.Streams/Watch", http_method: "POST", headers: {}, input: APIStreamExchange.new,
                   output: APIWebOutput.new, publish: APIStreamPublisher.new, capabilities: [:unary])
