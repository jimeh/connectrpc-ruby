# frozen_string_literal: true

require "connectrpc"
require "connectrpc/bindings/google_protobuf"
require "service_connect"
require_relative "support"

binding = ConnectRPC::Bindings::GoogleProtobuf.new
handlers = RbsFixture::Api::Echo.handlers(
  exchange: lambda do |request, context|
    context.check!
    context.response_metadata.add("checked", "yes")
    RbsFixture::Messages::Envelope::Reply.new(count: request.text.size)
  end,
  plain: ->(request, _context) { RbsPlain::V1::Outer::Reply.new(text: request.text) },
  reset: ->(request, _context) { request }
)
# The returned values retain a callable surface and return a message.
single = RbsFixture::Api::Single.handlers(exchange: GoodHandler.new)
single.each_value do |handler|
  handler.call(RbsFixture::Messages::Envelope::Request.new, ConnectRPC::Context.new).count.succ
end
RbsFixture::Api::Echo.handlers(exchange: GoodHandler.new,
                               plain: ->(_request, _context) { RbsPlain::V1::Outer::Reply.new },
                               reset: ->(request, _context) { request })
recorder = LifecycleRecorder.new
instrumentation = ConnectRPC::Instrumentation.new(subscribers: [recorder], on_error: lambda { |error|
  error.message
  nil
})
server = ConnectRPC::Server.new(services: [RbsFixture::Api::Echo::SERVICE], binding: binding, handlers: handlers,
                                instrumentation: instrumentation)
client = ConnectRPC::Client.new(base_url: "https://rbs.invalid", binding: binding,
                                transport: LoopbackTransport.new(server), use_get: true, get_url_limit: 8192)
generated = RbsFixture::Api::Echo::Client.new(client: client)
RbsFixture::Api::Echo::EXCHANGE.idempotency.to_s
request = RbsFixture::Messages::Envelope::Request.new(text: "hello", tags: ["a"], counts: { "a" => 1 }, state: :READY)
request.tags << "b"
# @type var write_result: nil
write_result = request.tags.[]=(0, :first)
write_result.nil?
request.counts["b"] = 2
request.sent_at = Google::Protobuf::Timestamp.new(seconds: 1)
request.limit = 3
source = ConnectRPC::CancellationSource.new
response = generated.exchange(request, metadata: { "caller" => ["typecheck"] }, timeout: nil,
                                       cancellation: source.token)
response.message.count.succ
response.message = RbsFixture::Messages::Envelope::Reply.new(count: 9)
client.call(RbsFixture::Api::Echo::EXCHANGE, request).message.count.succ
# Fully initialized mutable Struct construction is the documented typed subset.
ConnectRPC::Response.new(message: response.message, metadata: ConnectRPC::Metadata.new, trailers: ConnectRPC::Metadata.new)
RbsFixture::Api::NoCalls.handlers

# A manual token association is an application assertion, rather than inference.
# @type var manual: ConnectRPC::Method[RbsFixture::Messages::Envelope::Request, RbsFixture::Messages::Envelope::Reply]
manual = ConnectRPC::Method.new(service: "rbs_service.v1.Manual", name: "Exchange",
                                request_type: RbsFixture::Messages::Envelope::Request,
                                response_type: RbsFixture::Messages::Envelope::Reply, idempotency: :no_side_effects)
client.call(manual, request).message.count.succ

# Official descriptor overloads preserve class tokens and constructor fields.
pool = Google::Protobuf::DescriptorPool.generated_pool
pool.lookup("rbs_types.v1.Envelope.Request").msgclass.new(text: "lookup").text.size
RbsFixture::Messages::Envelope::Request::State.lookup(1)&.to_s
RbsFixture::Messages::Envelope::Request::State.resolve(:READY)&.succ

handle = server.start_call(path: "/rbs_service.v1.Manual/Exchange", headers: {})
if handle
  handle.activate { 42 }.succ
  handle.finish(delivery: :completed, decision: :admission_rejected, http_status: 503)
end
ConnectRPC::Context.new.record_failure(RuntimeError.new("local"))
recorder.completions.each { |completion| completion.delivery.to_s }

require "streaming_connect"
stream_handlers = RbsFixture::Streaming::Mixed.handlers(
  lookup: ->(request, _context) { RbsFixture::Messages::Envelope::Reply.new(count: request.text.size) },
  watch: lambda { |request, writer, context|
    context.send_headers(metadata: { "early" => ["yes"] })
    writer.send_message(RbsFixture::Messages::Envelope::Reply.new(count: request.text.size))
  },
  collect: lambda { |reader, _context|
    count = 0
    reader.each { |message| count += message.text.size }
    RbsFixture::Messages::Envelope::Reply.new(count: count)
  },
  chat: lambda { |duplex, _context|
    duplex.each { |message| duplex.send_message(RbsFixture::Messages::Envelope::Reply.new(count: message.text.size)) }
  }
)
ConnectRPC::Server.new(services: [RbsFixture::Streaming::Mixed::SERVICE], binding: binding, handlers: stream_handlers)
stream_client = RbsFixture::Streaming::Mixed::Client.new(client: client)
stream_client.lookup(request, timeout: nil).message.count.succ
stream_client.watch(request, timeout: nil, cancellation: source.token) do |reader|
  reader.headers.to_h
  reader.each { |reply| reply.count.succ }
  reader.trailers&.to_h
end
stream_client.collect(timeout: nil) { |writer| writer.send_message(request) }.message.count.succ
stream_client.chat(timeout: nil) do |duplex|
  duplex.send_message(request)
  duplex.close_send
  duplex.each { |reply| reply.count.succ }
end
