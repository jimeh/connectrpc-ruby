# frozen_string_literal: true

# Scripted proxies for CONNECT reply, stall and held-dial cases. A
# one-connection proxy's handler receives the accepted client socket; its
# return value is the block's result. Nothing listens beyond loopback.
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

  # A CONNECT relay to the loopback `port`, one tunnel per connection. The
  # first tunnel waits for `gate` before it opens; later ones open at once.
  # Yields the proxy URL and a queue of the CONNECT request heads.
  def with_gated_tunnels(port, gate, &)
    with_routed_tunnels(->(_index) { port }, [gate]) { |url, heads, _ended| yield url, heads }
  end

  # A CONNECT relay with one tunnel per connection, numbered in accept order.
  # Tunnel N opens once `gates[N]` (if any) yields, then relays to the
  # loopback port `routes.call(N)`. Yields the proxy URL, a queue of the
  # CONNECT request heads and a queue of the numbers of tunnels that ended.
  def with_routed_tunnels(routes, gates)
    listener = TCPServer.new("127.0.0.1", 0)
    heads = Queue.new
    ended = Queue.new
    tunnels = []
    acceptor = Thread.new { accept_tunnels(listener, routes, gates, heads, ended, tunnels) }
    yield "http://127.0.0.1:#{listener.addr[1]}", heads, ended
  ensure
    listener&.close
    acceptor&.join(5)
    tunnels&.each { |tunnel| tunnel.join(5) }
  end

  def accept_tunnels(listener, routes, gates, heads, ended, tunnels) # rubocop:disable Metrics/ParameterLists
    loop do
      downstream = listener.accept
      index = tunnels.size
      tunnels << Thread.new do
        tunnel(downstream, routes.call(index), gates[index], heads)
      ensure
        ended << index
      end
    end
  rescue IOError, SystemCallError
    nil
  end

  def tunnel(downstream, port, gate, heads)
    heads << read_head(downstream)
    gate&.pop
    upstream = TCPSocket.new("127.0.0.1", port)
    downstream.write("HTTP/1.1 200 Connection established\r\n\r\n")
    [pipe(downstream, upstream), pipe(upstream, downstream)].each(&:join)
  rescue IOError, SystemCallError
    nil
  ensure
    upstream&.close
    downstream.close
  end

  # Copies until EOF, then half-closes the other side so its peer sees EOF.
  def pipe(from, to)
    Thread.new do
      IO.copy_stream(from, to)
      to.close_write
    rescue IOError, SystemCallError
      nil
    end
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
