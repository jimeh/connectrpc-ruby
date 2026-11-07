# frozen_string_literal: true

module HTTP1ClientHelpers
  def with_raw_http1(peer:, tls: false, alpn: "http/1.1", before_handshake: nil, **)
    with_ca do |ca_file|
      reactor do |task|
        listener = TCPServer.new("127.0.0.1", 0)
        origin = "#{tls ? 'https' : 'http'}://127.0.0.1:#{listener.addr[1]}"
        accepted = []
        peers = []
        acceptor = task.async(finished: false) do
          loop do
            raw = listener.accept
            accepted << raw
            index = accepted.size
            peers << task.async(finished: false) do
              serve_raw_http1(raw, index, peer, tls: tls, alpn: alpn, before_handshake: before_handshake)
            end
          end
        end
        options = tls ? { ca_file: ca_file } : {}
        transport = ConnectRPC::Async::HTTP1Transport.new(origin: origin, **options, **)
        yield transport, origin, accepted, task
      ensure
        transport&.close(grace: 0)
        stop_http1_peers(listener, accepted, [acceptor, *peers].compact)
      end
    end
  end

  def serve_raw_http1(raw, index, peer, tls:, alpn:, before_handshake:)
    socket = raw
    if tls
      before_handshake&.call(raw, index)
      context = OpenSSL::SSL::SSLContext.new
      context.min_version = OpenSSL::SSL::TLS1_2_VERSION
      context.add_certificate(*tls_material.fetch(:leaf))
      context.alpn_select_cb = ->(_) { alpn } if alpn
      socket = OpenSSL::SSL::SSLSocket.new(raw, context)
      socket.sync_close = true
      socket.accept
    end
    native = Protocol::HTTP1::Connection.new(AsyncHelpers::INTERNAL::HTTP1Stream.new(socket))
    peer.call(native, socket, raw, index)
  rescue IOError, SystemCallError, OpenSSL::SSL::SSLError, Protocol::HTTP::Error
    nil
  ensure
    AsyncHelpers::INTERNAL.raw_close(raw)
  end

  def stop_http1_peers(listener, accepted, children)
    listener&.close
    accepted&.each { |raw| AsyncHelpers::INTERNAL.raw_close(raw) }
    # Cancel every peer before joining any peer that may be waiting on it.
    children.each { |child| AsyncHelpers::INTERNAL.cancel(child) }
    children.reverse_each { |child| AsyncHelpers::INTERNAL.join(child) }
  end

  def with_http1_peer(response: nil, **)
    requests = []
    peer = lambda do |native, socket, *_|
      loop do
        incoming = native.read_request
        break unless incoming

        requests << incoming
        incoming.last&.each { |_| nil }
        wire = response.respond_to?(:call) ? response.call(requests.size, incoming) : response
        socket.write(wire)
        native.send_end_stream!
      end
    end
    with_raw_http1(peer: peer, **) do |transport, origin, accepted, task|
      yield transport, origin, accepted, requests, task
    end
  end

  def http1_wire(body = "ok", fields: "", status: "HTTP/1.1 200 OK")
    "#{status}\r\nContent-Length: #{body.bytesize}\r\n#{fields}\r\n#{body}"
  end

  def stream_request(origin, cancellation: nil, deadline: nil)
    ConnectRPC::StreamRequest.new(url: URI("#{origin}/stream"), http_method: "POST", headers: {},
                                  cancellation: cancellation, deadline: deadline)
  end
end
