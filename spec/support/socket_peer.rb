# frozen_string_literal: true

require "socket"

module SocketPeer
  def with_peer(handler)
    listener = TCPServer.new("127.0.0.1", 0)
    socket = nil
    worker = Thread.new do
      socket = listener.accept
      handler.call(socket)
    rescue IOError, Errno::EBADF
      raise unless listener.closed?
    ensure
      socket&.close unless socket&.closed?
    end
    yield "http://127.0.0.1:#{listener.addr[1]}"
    raise "Socket peer did not finish" unless worker.join(5)

    worker.value
  ensure
    listener&.close unless listener&.closed?
    socket&.close unless socket&.closed?
    raise "Socket peer leaked a worker" if worker && !worker.join(5)
  end

  def read_request(socket)
    headers = +""
    headers << socket.read(1) until headers.end_with?("\r\n\r\n")
    size = headers[/Content-Length: ([0-9]+)/i, 1].to_i
    socket.read(size) if size.positive?
    headers
  end

  def transport_request(url, timeout: 1, body: "request", limit: 1024)
    ConnectRPC::TransportRequest.new(url: URI(url), http_method: "POST",
                                     headers: { "content-type" => ["application/proto"] },
                                     body: body, response_limit: limit, deadline: ConnectRPC::Deadline.new(timeout))
  end
end
