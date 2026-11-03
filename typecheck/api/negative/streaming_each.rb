# frozen_string_literal: true

# @type var rpc: ConnectRPC::ServerStreamMethod[String, String]
rpc = ConnectRPC::ServerStreamMethod.new(service: "typed.Streams", name: "Call", request_type: String,
                                         response_type: String)
core = ConnectRPC::Client.new(base_url: "https://typed.invalid", binding: APIBinding.new)
core.server_stream(rpc, "q") do |reader|
  reader.each
  nil
end
