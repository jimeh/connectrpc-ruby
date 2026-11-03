# frozen_string_literal: true

binding = APIBinding.new
method = ConnectRPC::Method.new(service: "demo.Service", name: "Call", request_type: String, response_type: String)
service = ConnectRPC::Service.new(name: method.service, methods: [method])
service.methods.each { |rpc| rpc.path.size }
metadata = ConnectRPC::Metadata.new("x-example" => ["value"])
metadata.add("x-example", "next").add_binary("x-token-bin", "bytes")
metadata.binary("x-token-bin").each(&:bytesize)
metadata.each { |key, values| key.size + values.size }
metadata.each.to_a
ConnectRPC::Metadata.reserved?("connect-timeout-ms")
ConnectRPC::Metadata.decode_binary(ConnectRPC::Metadata.encode_binary("bytes"))
detail = ConnectRPC::ErrorDetail.new(type: "demo.Detail", value: "bytes")
detail.decode(binding: binding, type: String)
error = ConnectRPC::Error.new(:invalid_argument, "safe", details: [detail], metadata: metadata, trailers: {})
error.to_h
error.http_status.succ
ConnectRPC::Deadline.new(1, clock: -> { 0.5 }).timeout_ms.size
bus = ConnectRPC::Instrumentation.new(subscribers: [APISubscriber.new], on_error: lambda { |failure|
  failure.message
  nil
})
server = ConnectRPC::Server.new(services: [service], binding: binding, invoker: APIInvoker.new, instrumentation: bus)
ConnectRPC::Server.new(services: [], binding: binding, invoker: lambda { |rpc, request, context|
  context.check!
  rpc.path
  request
})
handle = server.start_call(path: method.path, headers: {})
if handle
  handle.activate { "value" }.size
  handle.record_failure(RuntimeError.new("local"))
  handle.host_rejected!
  handle.host_rejected?
  server.call(path: method.path, http_method: "POST", headers: {}, input: nil, lifecycle: handle).status.succ
  handle.finish(delivery: :response_finished, decision: :host_rejected, http_status: 503)
end
ConnectRPC::Bindings::GoogleProtobuf.new
request = ConnectRPC::TransportRequest.new(
  url: URI("https://example.invalid"), http_method: "POST", headers: {}, body: "bytes",
  response_limit: 10
)
ConnectRPC::Transports::NetHTTP.new.call(request).body.size

ConnectRPC::Deadline.new(1, clock: APIClock.new).remaining
call = ConnectRPC::Instrumentation::Call.new(:client, :connect, method, nil, nil, {})
call.carrier["traceparent"] = ["application-carrier"]
call.method&.path
call.protocol.to_s
call.server_address&.size
call.server_port&.succ
outcome = ConnectRPC::Instrumentation::Outcome.new(
  code: "OK", http_status: 200, error: nil, normalized_error: nil, duration: 0.1,
  request_bytes: 1, response_bytes: 2, request_metadata: {}, response_metadata: {}
)
outcome.code&.upcase
outcome.http_status&.succ
outcome.error&.message
outcome.normalized_error&.message
outcome.request_bytes&.succ
outcome.response_bytes&.succ
outcome.request_metadata.each_value(&:size)
outcome.response_metadata.each_value(&:size)
