# frozen_string_literal: true

# A minimal HTTP/1.1 server, optionally over TLS with ALPN http/1.1, for TLS
# parity, HTTP/1.1 pooling and body-limit cases. Each request runs the
# responder, which writes raw bytes. Accepts, requests and closes are
# recorded in order, so specs wait on them instead of sleeping.
class TinyHTTP
  Request = Struct.new(:http_method, :path, :headers, :body, :connection, keyword_init: true)

  attr_reader :port

  def initialize(tls: nil, &responder)
    @server = TCPServer.new("127.0.0.1", 0)
    @port = @server.addr[1]
    @tls = tls
    @responder = responder || ->(_request, socket) { TinyHTTP.ok(socket) }
    @lock = Mutex.new
    @changed = ConditionVariable.new
    @events = []
    @connections = 0
    @acceptor = Thread.new { accept_loop }
  end

  def self.ok(socket, body: "", headers: {})
    fields = { "content-type" => "application/proto", "content-length" => body.bytesize.to_s }.merge(headers)
    socket.write("HTTP/1.1 200 OK\r\n#{fields.map { |key, value| "#{key}: #{value}\r\n" }.join}\r\n#{body}")
  end

  def origin = "#{@tls ? 'https' : 'http'}://127.0.0.1:#{@port}"
  def events = @lock.synchronize { @events.dup }
  def count(kind) = events.count { |event| event.first == kind }

  def wait_for(kind, count, timeout: 10)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    @lock.synchronize do
      loop do
        return if @events.count { |event| event.first == kind } >= count

        remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
        raise "Timed out waiting for #{count} #{kind}; events: #{@events.last(10).inspect}" if remaining <= 0

        @changed.wait(@lock, remaining)
      end
    end
  end

  def close
    @server.close
    @acceptor.join
  end

  private

  def record(*event)
    @lock.synchronize do
      @events << event
      @changed.broadcast
    end
  end

  def accept_loop
    loop do
      socket = @server.accept
      Thread.new(socket) { |raw| serve(raw) }
    end
  rescue IOError, SystemCallError
    nil
  end

  def serve(raw)
    id = @lock.synchronize { @connections += 1 }
    record(:accept, id)
    socket = raw
    if @tls
      socket = OpenSSL::SSL::SSLSocket.new(raw, @tls)
      socket.sync_close = true
      socket.accept
    end
    while (request = read_request(socket, id))
      record(:request, id, request.path)
      @responder.call(request, socket)
    end
  rescue IOError, SystemCallError, OpenSSL::SSL::SSLError => e
    record(:error, id, e.class.name)
  ensure
    record(:closed, id)
    socket&.close
  end

  def read_request(socket, id)
    line = socket.gets("\r\n")
    return unless line

    method, path, = line.split
    headers = {}
    while (header = socket.gets("\r\n")) && header != "\r\n"
      name, value = header.split(":", 2)
      headers[name.downcase] = value.strip
    end
    body = read_body(socket, headers)
    Request.new(http_method: method, path: path, headers: headers, body: body, connection: id)
  end

  def read_body(socket, headers)
    return socket.read(headers["content-length"].to_i) if headers["content-length"]
    return +"" unless headers["transfer-encoding"]&.include?("chunked")

    body = +""
    loop do
      size = socket.gets("\r\n").to_i(16)
      chunk = socket.read(size + 2).to_s
      break if size.zero?

      body << chunk.byteslice(0, size)
    end
    body
  end
end
