# frozen_string_literal: true

# Generated Mixed service peers and raw stream requests for ThreadedTransport
# stream examples. Requires generation/streaming_connect on the load path.
module ThreadedStreamHelpers
  MIXED = GenerationFixture::Streaming::Mixed
  ITEM = GenerationFixture::Streaming::Item
  WATCH = MIXED::WATCH
  BINDING = ConnectRPC::Bindings::GoogleProtobuf.new

  def item(name) = ITEM.new(name: name)
  def watch_method = WATCH

  def mixed_server(watch: nil, lookup: nil, **)
    watch ||= ->(message, writer, _) { writer.send_message(message) }
    lookup ||= ->(message, _) { message }
    handlers = MIXED.handlers(lookup: lookup, watch: watch, collect: ->(_, _) { item("") },
                              chat: ->(duplex, _) { duplex.each { |message| duplex.send_message(message) } })
    ConnectRPC::Server.new(services: [MIXED::SERVICE], binding: BINDING, handlers: handlers, protocol: :auto, **)
  end

  def mixed_client(transport, origin, **)
    MIXED::Client.new(client: ConnectRPC::Client.new(base_url: origin, transport: transport, binding: BINDING, **))
  end

  # +target+ is a method declaration or a raw path.
  def stream_request(origin, target, headers: {}, deadline: nil, cancellation: nil, lifecycle: nil)
    path = target.respond_to?(:path) ? target.path : target
    ConnectRPC::StreamRequest.new(url: URI("#{origin}#{path}"), http_method: "POST",
                                  headers: { "content-type" => ["application/connect+proto"] }.merge(headers),
                                  deadline: deadline, lifecycle: lifecycle, cancellation: cancellation)
  end

  def envelope(message, flags: 0)
    bytes = message.class.encode(message)
    [flags, bytes.bytesize].pack("CN") + bytes
  end

  def stream_mailboxes(transport)
    engine = engine_of(transport)
    return [] unless engine

    stream = ConnectRPC::Async::ThreadedTransport.const_get(:Stream)
    engine.admission.lock.synchronize { engine.calls.keys.grep(stream) }
  end
end
