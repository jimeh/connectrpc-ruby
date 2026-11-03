# frozen_string_literal: true

server = ConnectRPC::Server.new(services: [], binding: APIBinding.new)
app = ConnectRPC::Rack.new(APIApp.new, server: server)
status, headers, _body = app.call("PATH_INFO" => "/ordinary", "REQUEST_METHOD" => "GET")
status.succ
headers.each_key(&:size)
ConnectRPC::Rack.new(->(env) { [404, {}, [env.fetch("PATH_INFO").to_s]] }, server: server)
transport = ConnectRPC::Testing::Transport.new(server: server)
client = ConnectRPC::Client.new(base_url: "https://in-process.invalid", binding: APIBinding.new, transport: transport)
# @type var method: ConnectRPC::Method[String, String]
method = ConnectRPC::Method.new(service: "demo.Service", name: "Call", request_type: String, response_type: String)
client.call(method, "message").message.size
