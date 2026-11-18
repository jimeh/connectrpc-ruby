# frozen_string_literal: true

require "pp"
require_relative "support/socket_proxy"

RSpec.describe "Proxies" do
  include SocketProxy

  proxy_env = %w[HTTP_PROXY http_proxy HTTPS_PROXY https_proxy NO_PROXY no_proxy REQUEST_METHOD].freeze
  dead = "http://127.0.0.1:1"
  let(:proxy_env) { proxy_env }
  let(:dead) { dead }

  around do |example|
    saved = proxy_env.to_h { |key| [key, ENV.fetch(key, nil)] }
    proxy_env.each { |key| ENV.delete(key) }
    example.run
  ensure
    saved.each { |key, value| value.nil? ? ENV.delete(key) : ENV[key] = value }
  end

  def with_env(values)
    values.each { |key, value| ENV[key] = value }
    yield
  ensure
    values.each_key { |key| ENV.delete(key) }
  end

  # The userinfo decodes to the Go peer's expected Basic credentials ("proxy
  # user", "p@ss:word"), so auth=ok proves core's userinfo decoding.
  def proxy_url = peer["proxy"].sub("http://", "http://proxy%20user:p%40ss%3Aword@")
  def port_of(name) = URI(peer[name]).port
  def proxy_events(from) = peer.since(from).grep(/\Aproxy (connect|forward) /)

  def failure(transport, request)
    transport.call(request)
    raise "Expected a transport failure"
  rescue ConnectRPC::Error => e
    e
  end

  # Names under .test resolve only at the Go proxy: the client process has no
  # mapping for them, so a successful call proves it never resolved the name.
  describe "through the Go proxy" do
    hostile = { "http_proxy" => dead, "https_proxy" => dead, "no_proxy" => "*" }
    [
      ["grpc-go over h2c", "grpc_h2c", :grpc, {}, /\Agrpc accept /],
      ["grpc-go over TLS", "grpc_tls", :grpc, {}, /\Agrpc accept /],
      ["connect-go over HTTP/1.1 and TLS", "connect_tls", :connect, { http2: false }, %r{\Aconnect http HTTP/1\.1 /}]
    ].each do |label, name, protocol, options, served|
      it "tunnels #{label} with CONNECT and Basic credentials, ignoring a hostile environment" do
        from = peer.mark
        scheme = URI(peer[name]).scheme
        options = options.merge(**tls_options) if scheme == "https"
        transport = with_env(hostile) { hyper(proxy: proxy_url, **options) }
        peer_client = client("#{scheme}://peer.test:#{port_of(name)}", transport: transport, protocol: protocol)
        replies = with_env(hostile) do
          streamed = []
          reply = peer_client.unary(MSG.new(value: label), timeout: 5).message.value
          peer_client.server_stream(MSG.new(value: "count:3:1"), timeout: 5) { |reader| streamed = values(reader) }
          [reply, streamed]
        end
        expect(replies).to eq([label, %w[0 1 2]])
        # One tunnel carried both calls; the CONNECT had no User-Agent.
        expect(proxy_events(from)).to eq(["proxy connect peer.test:#{port_of(name)} auth=ok ua=false"])
        expect(peer.count(served, from: from)).to be >= 1
      end
    end

    it "verifies the target certificate through the tunnel" do
      transport = hyper(proxy: proxy_url, **tls_options)
      error = error_of do
        client("https://other.test:#{port_of('grpc_tls')}", transport: transport).unary(MSG.new, timeout: 5)
      end
      expect([error.code, error.message]).to eq(["unavailable", "HTTP transport failed: TLS verification failed"])
    end

    it "forwards plaintext HTTP/1.1 in absolute form with Proxy-Authorization" do
      from = peer.mark
      transport = with_env("HTTP_PROXY" => dead, "no_proxy" => "*") { hyper(proxy: proxy_url, http2: false) }
      origin = "http://peer.test:#{port_of('connect_h2c')}"
      peer_client = client(origin, transport: transport, protocol: :connect)
      expect(peer_client.unary(MSG.new(value: "forwarded"), timeout: 5).message.value).to eq("forwarded")
      streamed = []
      peer_client.server_stream(MSG.new(value: "count:2:1"), timeout: 5) { |reader| streamed = values(reader) }
      expect(streamed).to eq(%w[0 1])
      prefix = "#{origin}/connectrpc.grpcinterop.v1.Peer"
      expect(proxy_events(from)).to eq(["proxy forward POST #{prefix}/Unary auth=ok",
                                        "proxy forward POST #{prefix}/ServerStream auth=ok"])
      expect(peer.count(%r{\Aconnect http HTTP/1\.1 /}, from: from)).to eq(2)
    end

    it "keeps an h2c server's SETTINGS that arrive with the CONNECT reply" do
      upstream = nil
      relay = lambda do |client|
        read_head(client)
        upstream = TCPSocket.new("127.0.0.1", port_of("grpc_h2c"))
        # grpc-go writes its SETTINGS before reading the client preface.
        raise "No server preface" unless upstream.wait_readable(5)

        client.write("HTTP/1.1 200 Connection established\r\n\r\n#{upstream.readpartial(16_384)}")
        pipes = [[client, upstream], [upstream, client]].map do |from, to|
          Thread.new do
            IO.copy_stream(from, to)
          rescue IOError, SystemCallError
            nil
          end
        end
        pipes.first.join(5)
      ensure
        upstream&.close
      end
      with_socket_proxy(relay) do |url|
        transport = hyper(proxy: url)
        peer_client = client("http://peer.test:#{port_of('grpc_h2c')}", transport: transport)
        expect(peer_client.unary(MSG.new(value: "early"), timeout: 5).message.value).to eq("early")
        transport.close(grace: 1)
      end
    end
  end

  describe "the proxy environment" do
    # The child preloads the getaddrinfo shim, which maps .test names to
    # 127.0.0.1, so direct connections to them succeed without DNS.
    it "follows Go precedence and NO_PROXY through core's resolver, and proxy: nil ignores it" do
      tls = tls_options
      cases = {
        "uppercase wins" => [{ "HTTPS_PROXY" => proxy_url, "https_proxy" => dead },
                             "https://peer.test:#{port_of('grpc_tls')}", {}],
        "lowercase alone" => [{ "http_proxy" => proxy_url }, "http://peer.test:#{port_of('grpc_h2c')}", {}],
        "dead HTTP_PROXY" => [{ "HTTP_PROXY" => dead }, "http://peer.test:#{port_of('grpc_h2c')}", {}],
        "NO_PROXY match" => [{ "HTTP_PROXY" => dead, "NO_PROXY" => "other.test,peer.test" },
                             "http://peer.test:#{port_of('grpc_h2c')}", {}],
        "proxy: nil" => [{ "HTTP_PROXY" => dead, "HTTPS_PROXY" => dead }, "http://peer.test:#{port_of('grpc_h2c')}",
                         { proxy: nil }],
        "loopback" => [{ "HTTP_PROXY" => dead }, peer["grpc_h2c"], {}]
      }
      from = peer.mark
      output, status = ruby_child(<<~RUBY, env: stall_environment)
        names = #{proxy_env.inspect}
        results = #{cases.inspect}.map do |label, (env, origin, options)|
          names.each { |name| ENV.delete(name) }
          env.each { |name, value| ENV[name] = value }
          options = options.merge(#{tls.inspect}) if origin.start_with?("https")
          transport = ConnectRPC::Hyper::Transport.new(**options)
          names.each { |name| ENV.delete(name) }
          outcome = begin
            client(origin, transport).unary(MSG.new(value: label), timeout: 5).message.value
          rescue ConnectRPC::Error => e
            e.message
          end
          [label, outcome]
        end
        p results.to_h
      RUBY
      expect(status).to be_success
      expect(eval(output.lines.last)).to eq( # rubocop:disable Security/Eval
        "uppercase wins" => "uppercase wins", "lowercase alone" => "lowercase alone",
        "dead HTTP_PROXY" => "HTTP transport failed: connection refused", "NO_PROXY match" => "NO_PROXY match",
        "proxy: nil" => "proxy: nil", "loopback" => "loopback"
      )
      expect(proxy_events(from)).to eq(["proxy connect peer.test:#{port_of('grpc_tls')} auth=ok ua=false",
                                        "proxy connect peer.test:#{port_of('grpc_h2c')} auth=ok ua=false"])
    end

    {
      "an unsupported scheme" => [{ "HTTPS_PROXY" => "socks5://127.0.0.1:1080" }, "https", "unsupported proxy scheme"],
      "an https proxy" => [{ "HTTPS_PROXY" => "https://127.0.0.1:1080" }, "https", "unsupported proxy scheme"],
      "an invalid port" => [{ "HTTPS_PROXY" => "http://127.0.0.1:0" }, "https", "invalid proxy address"],
      "a trailing space" => [{ "HTTP_PROXY" => "http://127.0.0.1:9 " }, "http", "invalid proxy address"],
      "a CGI request" => [{ "HTTP_PROXY" => "http://127.0.0.1:9", "REQUEST_METHOD" => "POST" }, "http",
                          "proxy refused in CGI environment"]
    }.each do |label, (env, scheme, reason)|
      it "fails #{label} before opening a socket, on every call shape" do
        transport = with_env(env) { hyper }
        recorded = []
        lifecycle = Object.new
        lifecycle.define_singleton_method(:record_failure) { |error| recorded << error }
        error = failure(transport, transport_request("#{scheme}://peer.test/svc/Method", lifecycle: lifecycle))
        expect([error.code, error.message]).to eq(["unavailable", "HTTP transport failed: #{reason}"])
        expect([error.cause.class, error.cause.cause]).to eq([native::Failure, nil])
        expect(recorded).to eq([error.cause])
        stream = error_of { client("#{scheme}://peer.test", transport: transport).bidi(timeout: 5) { nil } }
        expect(stream.message).to eq("HTTP transport failed: #{reason}")
        expect(stats(transport).first).to eq(0)
      end
    end
  end

  describe "explicit options" do
    {
      "socks5://user:secret@proxy.test:1080" => "Unsupported proxy scheme",
      "https://user:secret@proxy.test" => "Unsupported proxy scheme",
      "http://user:secret@proxy.test:99999" => "Invalid proxy configuration",
      "mailto:demo:secret?broken" => "Invalid proxy configuration",
      URI("http://user:secret@:8080") => "Invalid proxy configuration",
      "http://user:secret@2130706433:8080" => "Invalid proxy configuration",
      42 => "Invalid proxy configuration"
    }.each do |option, message|
      it "rejects #{option.inspect.sub('secret', '***')} with NetHTTP's error and no cause" do
        require "connectrpc/transports/net_http"
        outcomes = [ConnectRPC::Transports::NetHTTP, ConnectRPC::Hyper::Transport].map do |transport|
          raise "ambient"
        rescue RuntimeError
          begin
            transport.new(proxy: option)
          rescue ArgumentError => e
            [e.class, e.message, e.cause]
          end
        end
        expect(outcomes).to eq([[ArgumentError, message, nil]] * 2)
      end
    end

    it "accepts a URI and never shows proxy URLs or credentials" do
      transport = hyper(proxy: URI("http://user:secret@127.0.0.1:1"))
      expect(transport.inspect).to eq("#<ConnectRPC::Hyper::Transport connections=pooled http2=true tls=false " \
                                      "proxy=explicit state=open>")
      expect(hyper(proxy: nil).inspect).to include("proxy=direct")
      expect(hyper.inspect).to include("proxy=environment")
      error = error_of { client("http://peer.test", transport: transport).unary(MSG.new, timeout: 5) }
      expect(error.message).to eq("HTTP transport failed: connection refused")
      texts = [transport.inspect, PP.pp(transport, +""), error.message, error.cause.message, error.cause.inspect]
      expect(texts.join).not_to include("secret", "user")
    end
  end

  describe "CONNECT replies" do
    {
      "a 407 with a body" => ["HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic " \
                              "realm=\"secret\"\r\nContent-Length: 6\r\n\r\nsecret", "proxy authentication failed"],
      "a 403" => ["HTTP/1.1 403 secret reason\r\nX-Secret: secret\r\n\r\n", "proxy tunnel refused"],
      "an informational status" => ["HTTP/1.1 100 Continue secret\r\n\r\n", "proxy tunnel refused"],
      "an empty reply" => ["", "proxy tunnel failed"],
      "a truncated head" => ["HTTP/1.1 200 OK\r\nX-Partial: secret", "proxy tunnel failed"],
      "a malformed status line" => ["HTTX/1.1 200 secret\r\n\r\n", "proxy tunnel failed"],
      "a four-digit status" => ["HTTP/1.1 2000\r\n\r\n", "proxy tunnel failed"],
      "a malformed field" => ["HTTP/1.1 200 OK\r\nsecret field\r\n\r\n", "proxy tunnel failed"],
      "bytes after a 200 head" => ["HTTP/1.1 200 OK\n\nsecret", "proxy tunnel failed"],
      "an oversized head" => ["HTTP/1.1 200 OK\r\nX-Pad: #{'secret' * 20}\r\n\r\n", "proxy tunnel failed"]
    }.each do |label, (reply, reason)|
      it "maps #{label} to #{reason} without proxy bytes in errors" do
        heads = Queue.new
        scripted = lambda do |client|
          heads << read_head(client)
          client.write(reply)
          # Closing with unread bytes would reset rather than end.
          client.read unless reply.empty? || label.include?("truncated")
        rescue Errno::ECONNRESET
          nil
        end
        recorded = []
        lifecycle = Object.new
        lifecycle.define_singleton_method(:record_failure) { |error| recorded << error }
        with_socket_proxy(scripted) do |url, listener|
          transport = hyper(proxy: url.sub("http://", "http://user:hunter2@"))
          if label.include?("oversized")
            allow(transport).to receive(:tunnel_limits).and_return([60.0, 64])
            transport.send(:build_driver)
          end
          error = failure(transport, transport_request("https://peer.test:443/svc/Method", lifecycle: lifecycle))
          expect([error.code, error.message]).to eq(["unavailable", "HTTP transport failed: #{reason}"])
          expect(error.cause).to be_a(native::Failure)
          texts = [error.message, error.cause.message, error.cause.inspect, *recorded.map(&:message)]
          expect(texts.join).not_to include("secret", "hunter2", "127.0.0.1", "peer.test")
          expect(listener.wait_readable(0)).to be_nil
        end
        expect(heads.pop(timeout: 0)).to eq("CONNECT peer.test:443 HTTP/1.1\r\nHost: peer.test:443\r\n" \
                                            "Proxy-Authorization: Basic #{['user:hunter2'].pack('m0')}\r\n\r\n")
      end
    end

    it "brackets an IPv6 target and sends no credentials without userinfo" do
      heads = Queue.new
      scripted = lambda do |client|
        heads << read_head(client)
        client.write("HTTP/1.1 403 Forbidden\r\n\r\n")
        client.read
      end
      with_socket_proxy(scripted) do |url|
        error = failure(hyper(proxy: url), transport_request("https://[2001:db8::1]:8443/svc/Method"))
        expect(error.message).to eq("HTTP transport failed: proxy tunnel refused")
      end
      expect(heads.pop(timeout: 0)).to eq("CONNECT [2001:db8::1]:8443 HTTP/1.1\r\nHost: [2001:db8::1]:8443\r\n\r\n")
    end
  end

  describe "stalled setup" do
    def stalling(phase, progress, closed)
      lambda do |client|
        read_head(client)
        if phase == :handshake
          client.write("HTTP/1.1 200 Connection established\r\n\r\n")
          raise "No ClientHello" unless client.wait_readable(5) && !client.readpartial(1024).empty?
        end
        progress << true
        client.read
        closed << true
      end
    end

    %i[connect handshake].product(%i[deadline cancellation]).each do |phase, ending|
      it "ends a #{phase} stall with #{ending}, closing the proxy connection" do
        progress = Queue.new
        closed = Queue.new
        source = ConnectRPC::CancellationSource.new
        with_socket_proxy(stalling(phase, progress, closed)) do |url, listener|
          request = transport_request("https://peer.test:443/svc/Method", timeout: ending == :deadline ? 0.3 : nil)
          request.cancellation = source.token if ending == :cancellation
          transport = hyper(proxy: url)
          caller = Thread.new { failure(transport, request) }
          expect(progress.pop(timeout: 5)).to be(true)
          source.cancel if ending == :cancellation
          expect(caller.join(5)).to equal(caller)
          expect(caller.value.code).to eq(ending == :deadline ? "deadline_exceeded" : "canceled")
          expect(closed.pop(timeout: 5)).to be(true)
          expect(listener.wait_readable(0)).to be_nil
        ensure
          caller&.join(2)
        end
      end
    end

    it "lands Thread#raise in a stalled CONNECT and closes the proxy connection" do
      progress = Queue.new
      closed = Queue.new
      with_socket_proxy(stalling(:connect, progress, closed)) do |url|
        transport = hyper(proxy: url)
        landed = Queue.new
        caller = Thread.new do
          transport.call(transport_request("https://peer.test:443/svc/Method", timeout: nil))
        rescue RuntimeError => e
          landed << e.message
        end
        expect(progress.pop(timeout: 5)).to be(true)
        wait_parked(caller, "Native::")
        caller.raise(RuntimeError, "interrupted")
        expect(landed.pop(timeout: 5)).to eq("interrupted")
        expect(closed.pop(timeout: 5)).to be(true)
      end
    end

    it "bounds the whole proxied setup with connect_timeout" do
      with_socket_proxy(stalling(:connect, Queue.new, Queue.new)) do |url|
        transport = hyper(proxy: url, connect_timeout: 0.3)
        started = monotonic
        error = failure(transport, transport_request("https://peer.test:443/svc/Method", timeout: nil))
        expect([error.code, error.message]).to eq(["unavailable", "HTTP transport failed: timed out"])
        expect(monotonic - started).to be_between(0.29, 2)
      end
    end

    it "caps a CONNECT exchange without a call deadline or connect_timeout" do
      with_socket_proxy(stalling(:connect, Queue.new, Queue.new)) do |url|
        transport = hyper(proxy: url, connect_timeout: nil)
        allow(transport).to receive(:tunnel_limits).and_return([0.3, 1024])
        transport.send(:build_driver)
        started = monotonic
        error = failure(transport, transport_request("https://peer.test:443/svc/Method", timeout: nil))
        expect([error.code, error.message]).to eq(["unavailable", "HTTP transport failed: proxy tunnel timed out"])
        expect(monotonic - started).to be_between(0.29, 2)
      end
    end
  end

  describe "pooling" do
    it "never reuses a direct connection for a proxied request, or one proxy's credentials for another's" do
      from = peer.mark
      transport = hyper
      address = proxy_url.delete_prefix("http://").split("@").last
      host, port = address.split(":")
      basic = ->(credentials) { "Basic #{[credentials].pack('m0')}" }
      routes = [nil, [host, port.to_i, basic.call("proxy user:p@ss:word")], nil,
                [host, port.to_i, basic.call("other:secret")]]
      allow(transport).to receive(:proxy_route) { routes.shift }
      peer_client = client(peer["grpc_h2c"], transport: transport)
      replies = Array.new(4) { |index| peer_client.unary(MSG.new(value: index.to_s), timeout: 5).message.value }
      expect(replies).to eq(%w[0 1 2 3])
      expect(stats(transport).first).to eq(3)
      tunnel = "proxy connect 127.0.0.1:#{port_of('grpc_h2c')}"
      expect(proxy_events(from)).to eq(["#{tunnel} auth=ok ua=false", "#{tunnel} auth=wrong ua=false"])
    end
  end
end
