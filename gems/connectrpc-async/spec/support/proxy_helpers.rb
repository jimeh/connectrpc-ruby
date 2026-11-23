# frozen_string_literal: true

require_relative "forward_proxy"

# Shared proxy example support. spec_helper clears the proxy environment
# around every example; `with_env` sets it only while a transport takes its
# snapshot.
module ProxyHelpers
  # An HTTP/1.1 peer on plain threads, optionally TLS for the proxy target
  # name, that answers each request with `respond.call(head, body)`.
  # `requests` receives every [head, body].
  class HTTP1Peer
    attr_reader :port, :requests

    def initialize(respond, tls: nil)
      @respond = respond
      @context = tls && context(*tls)
      @listener = TCPServer.new("127.0.0.1", 0)
      @port = @listener.addr[1]
      @requests = Thread::Queue.new
      @lock = Mutex.new
      @sockets = []
      @threads = []
      @stopping = false
      @acceptor = Thread.new { accept_loop }
    end

    def stop
      @stopping = true
      @listener.close
      @lock.synchronize { @sockets.each { |socket| socket.close unless socket.closed? } }
      threads = @lock.synchronize { [@acceptor, *@threads] }
      raise "HTTP/1 peer leaked a thread" unless threads.all? { |thread| thread.join(5) }
    end

    private

    def context(certificate, key)
      context = OpenSSL::SSL::SSLContext.new
      context.min_version = OpenSSL::SSL::TLS1_2_VERSION
      context.add_certificate(certificate, key)
      context.alpn_select_cb = ->(_) { "http/1.1" }
      context
    end

    def accept_loop
      loop do
        raw = @listener.accept
        @lock.synchronize do
          @sockets << raw
          @threads << Thread.new { serve(raw) }
        end
      end
    rescue IOError, SystemCallError
      raise unless @stopping
    end

    def serve(raw)
      socket = raw
      if @context
        socket = OpenSSL::SSL::SSLSocket.new(raw, @context)
        socket.sync_close = true
        socket.accept
      end
      while (head = HTTP1Wire.read_head(socket))
        body = HTTP1Wire.read_body(socket, head)
        @requests << [head, body]
        socket.write(@respond.call(head, body))
      end
    rescue IOError, SystemCallError, OpenSSL::SSL::SSLError
      nil
    end
  end

  def target = ConnectProxy::TARGET

  def with_env(values)
    values.each { |key, value| ENV[key] = value }
    yield
  ensure
    values.each_key { |key| ENV.delete(key) }
  end

  def mixed_host(**)
    threaded_host(server: mixed_server(watch: lambda { |message, writer, _|
      2.times { |index| writer.send_message(item("#{message.name}-#{index}")) }
    }), **)
  end

  def streamed(typed, name)
    names = []
    typed.watch(item(name)) { |reader| reader.each { |message| names << message.name } }
    names
  end

  # A certificate and key for the target name, issued by the shared test root.
  def target_identity
    @target_identity ||= begin
      root_certificate, root_key = tls_material.fetch(:root)
      key = OpenSSL::PKey::RSA.new(2048)
      certificate = OpenSSL::X509::Certificate.new
      certificate.version = 2
      certificate.serial = 301
      certificate.subject = OpenSSL::X509::Name.parse("/CN=#{target}")
      certificate.issuer = root_certificate.subject
      certificate.public_key = key
      certificate.not_before = Time.now - 60
      certificate.not_after = Time.now + 3600
      factory = OpenSSL::X509::ExtensionFactory.new
      factory.subject_certificate = certificate
      factory.issuer_certificate = root_certificate
      certificate.add_extension(factory.create_extension("basicConstraints", "CA:FALSE", true))
      certificate.add_extension(factory.create_extension("keyUsage", "digitalSignature,keyEncipherment", true))
      certificate.add_extension(factory.create_extension("extendedKeyUsage", "serverAuth"))
      certificate.add_extension(factory.create_extension("subjectAltName", "DNS:#{target}"))
      certificate.sign(root_key, "SHA256")
      [certificate, key]
    end
  end

  # Host TLS for the target name.
  def target_tls = host_tls(target_identity)

  def connect_head(authority, credentials = nil)
    "CONNECT #{authority} HTTP/1.1\r\nHost: #{authority}\r\n#{proxy_authorization(credentials)}\r\n"
  end

  def proxy_authorization(credentials)
    credentials ? "Proxy-Authorization: Basic #{[credentials].pack('m0')}\r\n" : ""
  end

  # Records each dial's host and port, then stops it before any socket opens.
  def stop_dials
    dials = Thread::Queue.new
    stop = Class.new(StandardError)
    allow(IO::Endpoint).to receive(:tcp).and_wrap_original do |_original, host, port, *|
      dials << [host, port]
      raise stop, "stopped before connecting"
    end
    dials
  end

  # A Connect echo over HTTP/1.1: a unary reply carries the request message
  # back; a server stream returns the request's one envelope, then an empty
  # end-of-stream envelope.
  def connect_echo(head, body)
    streaming = head.match?(%r{^content-type: *application/connect\+proto\r$}i)
    payload = streaming ? "#{body}#{[2, 2].pack('CN')}{}" : body
    type = streaming ? "application/connect+proto" : "application/proto"
    "HTTP/1.1 200 OK\r\nContent-Type: #{type}\r\nContent-Length: #{payload.bytesize}\r\n\r\n#{payload}"
  end
end
