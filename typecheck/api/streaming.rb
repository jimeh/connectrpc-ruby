# frozen_string_literal: true

# @type var ss: ConnectRPC::ServerStreamMethod[String, String]
ss = ConnectRPC::ServerStreamMethod.new(service: "typed.Streams", name: "Watch", request_type: String,
                                        response_type: String)
# @type var cs: ConnectRPC::ClientStreamMethod[String, String]
cs = ConnectRPC::ClientStreamMethod.new(service: "typed.Streams", name: "Collect", request_type: String,
                                        response_type: String)
# @type var bidi: ConnectRPC::BidiStreamMethod[String, String]
bidi = ConnectRPC::BidiStreamMethod.new(service: "typed.Streams", name: "Chat", request_type: String,
                                        response_type: String)
ConnectRPC::Service.new(name: "typed.Streams", methods: [ss, cs, bidi])
transport = APIStreamingTransport.new
# @type var custom: ConnectRPC::_StreamingTransport
custom = transport
core = ConnectRPC::Client.new(base_url: "https://typed.invalid", binding: APIBinding.new, transport: custom,
                              terminal_wire_limit: 65_536, terminal_limit: 65_536, use_get: true)
core.server_stream(ss, "request", timeout: nil) do |reader|
  reader.headers.to_h
  reader.each(&:upcase)
  reader.trailers&.to_h
end
core.client_stream(cs, timeout: nil) do |writer|
  writer.send_message("request")
  writer.close_send
end.message.upcase
core.bidi_stream(bidi) do |duplex|
  duplex.send_message("request")
  duplex.close_send
  duplex.each(&:upcase)
end
head = ConnectRPC::StreamHead[status: 200, headers: {}]
head[-2].succ
head["headers"].each_value(&:size)
request = ConnectRPC::StreamRequest.new(url: URI("https://typed.invalid"), http_method: "POST", headers: {})
request[-4].each_value(&:size)
request[-3]&.remaining
request[:cancellation]&.check!
ConnectRPC::Server.new(services: [], binding: APIBinding.new, terminal_limit: 1, terminal_wire_limit: 2)
