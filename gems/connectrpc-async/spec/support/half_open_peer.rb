# frozen_string_literal: true

# A raw protocol-http2 peer that ends a response with END_STREAM while the
# client's request body is still open, and never resets the stream itself
# unless asked to. Paths under /half/ are held until the example releases
# them; every other path gets a 200 "ok" unary response.
module HalfOpenPeer
  CONTENT_TYPES = { grpc: "application/grpc+proto", connect: "application/connect+proto",
                    grpc_web: "application/grpc-web+proto", unary: "application/proto" }.freeze
  NOT_FOUND = '{"error":{"code":"not_found","message":"missing"}}'
  WINDOW = Protocol::HTTP2::Settings::INITIAL_WINDOW_SIZE

  # Fails the next RST_STREAM write once armed, as a broken socket would.
  class ResetFailure < SimpleDelegator
    attr_accessor :armed

    def write(buffer)
      raise Errno::EPIPE if @armed && buffer.to_s.getbyte(3) == Protocol::HTTP2::ResetStreamFrame::TYPE

      super
    end
  end

  # Answers every request at once, for peers on another thread.
  Static = Struct.new(:response) do
    def dequeue = response
  end

  # `release`, or `release[path]` when it is a Hash, holds each /half/
  # request after its first request DATA arrived (unless `wait_data` is
  # false) until it yields the response's options for #half_open_response.
  # Settings go to the peer's SETTINGS.
  def half_open_peer(task, listener, release, settings: {}, wait_data: true)
    ConnectionPeer::RecordingServer.new(task, listener, settings: settings, &half_open_handler(release, wait_data))
  end

  def half_open_handler(release, wait_data)
    lambda do |incoming|
      if incoming.path.start_with?("/half/")
        incoming.body.read if wait_data
        queue = release.is_a?(Hash) ? release[incoming.path] : release
        half_open_response(incoming.stream, **queue.dequeue)
      else
        incoming.send_response(Protocol::HTTP::Response[200, {}, ["ok"]])
      end
    rescue Protocol::HTTP2::Error, IOError
      nil
    end
  end

  def half_open_envelope(flags, bytes) = [flags, bytes.bytesize].pack("CN") + bytes.b

  # Writes the response's frames and ends the stream, then resets it only
  # with `reset_after`. A :unary response's `message` is its whole body;
  # `extra` fields follow the gRPC status fields.
  def half_open_response(stream, protocol:, message: nil, code: 5, trailers_only: false, reset_after: nil, extra: [])
    half_open_frames(stream, protocol, message, code, trailers_only, extra: extra)
    stream.send_reset_stream(reset_after) if reset_after && !stream.closed?
  end

  def half_open_frames(stream, protocol, message, code, trailers_only, extra: [])
    head = [[":status", "200"], ["content-type", CONTENT_TYPES.fetch(protocol)]]
    status = [["grpc-status", code.to_s], %w[grpc-message missing], *extra]
    return stream.send_headers(head + status, Protocol::HTTP2::END_STREAM) if trailers_only
    return half_open_unary(stream, head, message) if protocol == :unary

    stream.send_headers(head)
    stream.send_data(half_open_envelope(0, message)) if message
    half_open_status(stream, protocol, code, status)
  end

  def half_open_unary(stream, head, message)
    return stream.send_headers(head, Protocol::HTTP2::END_STREAM) unless message

    stream.send_headers(head)
    stream.send_data(message, Protocol::HTTP2::END_STREAM)
  end

  def half_open_status(stream, protocol, code, status)
    case protocol
    when :grpc then stream.send_headers(status, Protocol::HTTP2::END_STREAM)
    when :connect
      stream.send_data(half_open_envelope(2, code.zero? ? "{}" : NOT_FOUND), Protocol::HTTP2::END_STREAM)
    else
      trailer = status.map { |key, value| "#{key}: #{value}\r\n" }.join
      stream.send_data(half_open_envelope(0x80, trailer), Protocol::HTTP2::END_STREAM)
    end
  end

  def half_open_request(origin, protocol, path: "/half/stream")
    ConnectRPC::StreamRequest.new(url: URI("#{origin}#{path}"), http_method: "POST",
                                  headers: { "content-type" => [CONTENT_TYPES.fetch(protocol)] }, deadline: nil)
  end

  def half_open_method(kind = ConnectRPC::ClientStreamMethod)
    kind.new(service: "half", name: "Stream", request_type: AsyncHelpers::MESSAGE,
             response_type: AsyncHelpers::MESSAGE)
  end

  def half_open_operation(transport) = transport.instance_variable_get(:@calls).keys.first

  # Every RST_STREAM code the client sent on `stream_id` of connection `index`.
  def client_resets(peer, stream_id, index = 0) = peer.wire(index)[:reset_log][stream_id]
end
