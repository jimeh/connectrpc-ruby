# frozen_string_literal: true

# @type var rpc: ConnectRPC::ClientStreamMethod[String, String]
rpc = ConnectRPC::ClientStreamMethod.new(service: "typed.Streams", name: "Call", request_type: String,
                                         response_type: String)
core = ConnectRPC::Client.new(base_url: "https://typed.invalid", binding: APIBinding.new)
core.client_stream(rpc) { |writer| writer.send_message(3) }
