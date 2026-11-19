# frozen_string_literal: true

require "socket"

# A local HTTP CONNECT proxy on plain threads. Each accepted connection reads
# its request head, records it in `heads`, then runs the handler with the
# client socket, the head and the proxy. The default handler tunnels to
# 127.0.0.1 at the requested port, whatever the requested host, so a proxied
# call to a name the client cannot resolve proves the proxy did the dialing.
# `closed` receives one entry each time a client ends its connection.
class ConnectProxy
  # A name the client never resolves. Loopback names bypass an environment
  # proxy, so environment examples use this one.
  TARGET = "proxied.test"
  PREFACE = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".b
  ESTABLISHED = "HTTP/1.1 200 Connection established\r\n\r\n"

  attr_reader :port, :heads, :closed

  def initialize(&handler)
    @listener = TCPServer.new("127.0.0.1", 0)
    @port = @listener.addr[1]
    @heads = Thread::Queue.new
    @closed = Thread::Queue.new
    @handler = handler || ->(client, head, proxy) { proxy.relay(client, proxy.upstream(head)) }
    @lock = Mutex.new
    @sockets = []
    @threads = []
    @accepted = 0
    @stopping = false
    @acceptor = Thread.new { accept_loop }
  end

  def url(userinfo = nil) = "http://#{"#{userinfo}@" if userinfo}127.0.0.1:#{@port}"
  def accepted = @lock.synchronize { @accepted }

  # A connection to 127.0.0.1 at the CONNECT target's port.
  def upstream(head)
    port = head[/\ACONNECT \S*:([0-9]+) /, 1] || raise("No CONNECT target in #{head.inspect}")
    own(TCPSocket.new("127.0.0.1", Integer(port)))
  end

  # Copies both directions until the client ends its side, then passes the
  # end on to the upstream. An upstream end is passed on to the client.
  def relay(client, upstream)
    client.write(ESTABLISHED)
    splice(client, upstream)
  end

  # Relays after the proxy has already answered the client. A `tap` also
  # receives every byte the client sends.
  def splice(client, upstream, tap: nil)
    pump = Thread.new do
      copy(upstream, client)
      half_close(client)
    end
    wait_closed(client) do |chunk|
      tap&.<<(chunk)
      forward(upstream, chunk)
    end
    half_close(upstream)
    pump.join(5)
  end

  # Reads until the client ends its connection, which it reports.
  def wait_closed(client)
    loop do
      chunk = client.readpartial(16_384)
      yield chunk if block_given?
    end
  rescue IOError, SystemCallError
    @closed << true unless @stopping
  end

  def stop
    @stopping = true
    @listener.close
    @lock.synchronize { @sockets.each { |socket| socket.close unless socket.closed? } }
    threads = @lock.synchronize { [@acceptor, *@threads] }
    raise "Connect proxy leaked a thread" unless threads.all? { |thread| thread.join(5) }
  end

  private

  def own(socket)
    @lock.synchronize { @sockets << socket }
    socket
  end

  def accept_loop
    loop do
      client = own(@listener.accept)
      @lock.synchronize do
        @accepted += 1
        @threads << Thread.new { serve(client) }
      end
    end
  rescue IOError, SystemCallError
    raise unless @stopping
  end

  def serve(client)
    head = +""
    head << client.readpartial(1) until head.end_with?("\r\n\r\n")
    @heads << head
    @handler.call(client, head, self)
  rescue IOError, SystemCallError
    @closed << true unless @stopping
  end

  def copy(from, to)
    loop { to.write(from.readpartial(16_384)) }
  rescue IOError, SystemCallError
    nil
  end

  # An upstream that has gone is the client's to notice, not a client end.
  def forward(upstream, chunk)
    upstream.write(chunk)
  rescue IOError, SystemCallError
    nil
  end

  def half_close(socket)
    socket.shutdown(Socket::SHUT_WR)
  rescue IOError, SystemCallError
    nil
  end
end
