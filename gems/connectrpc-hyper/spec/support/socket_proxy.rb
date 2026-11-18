# frozen_string_literal: true

# One-connection scripted proxies for CONNECT reply and stall cases. The
# handler receives the accepted client socket; its return value is the
# block's result. Nothing listens beyond loopback.
module SocketProxy
  def with_socket_proxy(handler)
    listener = TCPServer.new("127.0.0.1", 0)
    worker = Thread.new do
      Thread.current.report_on_exception = false
      socket = listener.accept
      begin
        handler.call(socket)
      ensure
        socket.close unless socket.closed?
      end
    rescue IOError, Errno::EBADF
      raise unless listener.closed?
    end
    yield "http://127.0.0.1:#{listener.addr[1]}", listener
    raise "Socket proxy did not finish" unless worker.join(5)

    worker.value
  ensure
    listener&.close
    worker&.join(1)
  end

  # The request head, read byte-wise so nothing after it is consumed.
  def read_head(socket)
    head = +""
    head << socket.read(1).to_s until head.end_with?("\r\n\r\n") || socket.eof?
    head
  end

  def transport_request(url, timeout: 2, lifecycle: nil)
    ConnectRPC::TransportRequest.new(url: URI(url), http_method: "POST",
                                     headers: { "content-type" => ["application/proto"] }, body: "request",
                                     response_limit: 1024, deadline: timeout && ConnectRPC::Deadline.new(timeout),
                                     lifecycle: lifecycle)
  end
end
