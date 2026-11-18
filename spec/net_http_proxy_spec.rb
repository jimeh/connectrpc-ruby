# frozen_string_literal: true

require "connectrpc/transports/net_http"
require_relative "support/socket_peer"
require_relative "support/tls_peer"

RSpec.describe "NetHTTP proxies" do
  include SocketPeer
  include TLSPeer

  around do |example|
    keys = %w[HTTP_PROXY http_proxy HTTPS_PROXY https_proxy NO_PROXY no_proxy REQUEST_METHOD]
    saved = keys.to_h { |key| [key, ENV.fetch(key, nil)] }
    keys.each { |key| ENV.delete(key) }
    example.run
  ensure
    saved.each { |key, value| value.nil? ? ENV.delete(key) : ENV[key] = value }
  end

  # The client never resolves the target name itself unless a test maps it,
  # so a proxied call proves the proxy did the resolving.
  before do
    @dns = {}
    allow(Addrinfo).to receive(:getaddrinfo).and_wrap_original do |original, host, *rest, **options|
      raise SocketError, "unexpected client lookup" if host == TLSPeer::TARGET && !@dns.key?(host)

      original.call(@dns.fetch(host, host), *rest, **options)
    end
  end

  def with_env(values)
    values.each { |key, value| ENV[key] = value }
    yield
  ensure
    values.each_key { |key| ENV.delete(key) }
  end

  def failure(transport, request)
    transport.call(request)
    raise "Expected a transport failure"
  rescue ConnectRPC::Error => e
    e
  end

  def origin(requests)
    lambda do |socket|
      requests << read_request(socket)
      socket.write("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
      socket.read
    end
  end

  def tunnel_to(port, heads)
    lambda do |client|
      heads << read_request(client)
      upstream = TCPSocket.new("127.0.0.1", port)
      client.write("HTTP/1.1 200 Connection established\r\n\r\n")
      relay(client, upstream)
    ensure
      upstream&.close
    end
  end

  def basic(credentials) = "Basic #{[credentials].pack('m0')}"

  it "tunnels https through an HTTPS_PROXY snapshot with Basic credentials over one proxy connection" do
    names = Queue.new
    requests = Queue.new
    heads = Queue.new
    target_port = nil
    with_ca_file do |ca_file|
      with_tls_peer(server_context(names: names), origin(requests)) do |port|
        target_port = port
        with_peer(tunnel_to(port, heads)) do |proxy_url, listener|
          transport = with_env("HTTPS_PROXY" => proxy_url.sub("http://", "us%3Aer:p%40ss+@")) do
            ConnectRPC::Transports::NetHTTP.new(ca_file: ca_file)
          end
          # The environment is gone; the transport keeps its snapshot.
          request = transport_request("https://#{TLSPeer::TARGET}:#{port}/svc/Method")
          expect(transport.call(request).body).to eq("ok")
          expect(listener.wait_readable(0)).to be_nil
          expect(failure(ConnectRPC::Transports::NetHTTP.new(ca_file: ca_file), request).message)
            .to eq("HTTP transport failed: name resolution failed")
        end
      end
    end
    authority = "#{TLSPeer::TARGET}:#{target_port}"
    expect(heads.pop(timeout: 0)).to eq("CONNECT #{authority} HTTP/1.1\r\nHost: #{authority}\r\n" \
                                        "Proxy-Authorization: #{basic('us:er:p@ss+')}\r\n\r\n")
    expect(names.pop(timeout: 0)).to eq(TLSPeer::TARGET)
    head = requests.pop(timeout: 0)
    expect(head).to start_with("POST /svc/Method HTTP/1.1\r\n")
    expect(head).not_to match(/proxy-authorization/i)
  end

  it "verifies the tunnelled certificate against the target hostname" do
    failures = Queue.new
    with_ca_file do |ca_file|
      with_tls_peer(server_context(tls_material.fetch(:other)), origin(Queue.new), failures: failures) do |port|
        with_peer(tunnel_to(port, Queue.new)) do |proxy_url|
          transport = ConnectRPC::Transports::NetHTTP.new(ca_file: ca_file, proxy: proxy_url)
          error = failure(transport, transport_request("https://#{TLSPeer::TARGET}:#{port}"))
          expect(error.message).to eq("HTTP transport failed: TLS verification failed")
        end
      end
    end
    expect(failures.pop(timeout: 0)).to be_a(OpenSSL::SSL::SSLError)
  end

  [
    ["HTTP_PROXY", "http://#{TLSPeer::TARGET}:8080/svc/Method?x=1", {}],
    ["explicit proxy despite NO_PROXY=*", "http://127.0.0.1:8080/svc/Method?x=1", { "NO_PROXY" => "*" }]
  ].each do |label, url, env|
    it "sends an absolute-form http request and Proxy-Authorization through #{label}" do
      heads = Queue.new
      with_peer(origin(heads)) do |proxy_url, listener|
        proxy = proxy_url.sub("http://", "http://user:pa%20ss@")
        transport = with_env(env.merge(label == "HTTP_PROXY" ? { "HTTP_PROXY" => proxy } : {})) do
          ConnectRPC::Transports::NetHTTP.new(proxy: label == "HTTP_PROXY" ? :environment : proxy)
        end
        expect(transport.call(transport_request(url)).body).to eq("ok")
        expect(listener.wait_readable(0)).to be_nil
      end
      head = heads.pop(timeout: 0)
      expect(head).to start_with("POST #{url} HTTP/1.1\r\n")
      expect(head).to include("Proxy-Authorization: #{basic('user:pa ss')}\r\n", "Host: #{URI(url).host}:8080\r\n")
    end
  end

  [
    ["a loopback target", {}, "127.0.0.1"],
    ["localhost", {}, "localhost"],
    ["a NO_PROXY domain", { "NO_PROXY" => "other.test, .#{TLSPeer::TARGET}" }, "api.#{TLSPeer::TARGET}"],
    ["proxy: nil", {}, TLSPeer::TARGET]
  ].each do |label, env, host|
    it "connects directly for #{label}" do
      @dns[host] = "127.0.0.1"
      proxy = TCPServer.new("127.0.0.1", 0)
      proxy_url = "http://127.0.0.1:#{proxy.addr[1]}"
      option = label == "proxy: nil" ? { proxy: nil } : {}
      transport = with_env(env.merge("HTTP_PROXY" => proxy_url, "HTTPS_PROXY" => proxy_url)) do
        ConnectRPC::Transports::NetHTTP.new(**option)
      end
      with_peer(origin(Queue.new)) do |url|
        target = url.sub("127.0.0.1", host.include?(":") ? "[#{host}]" : host)
        expect(transport.call(transport_request(target)).body).to eq("ok")
      end
      expect(proxy.wait_readable(0)).to be_nil
    ensure
      proxy&.close
    end
  end

  describe "stalled setup" do
    %i[connect handshake].product(%i[deadline cancellation]).each do |phase, ending|
      it "ends a #{phase} stall with #{ending}, closing the one proxy connection" do
        progress = Queue.new
        closed = Queue.new
        source = ConnectRPC::CancellationSource.new
        stall = lambda do |client|
          read_request(client)
          if phase == :handshake
            client.write("HTTP/1.1 200 Connection established\r\n\r\n")
            expect(client.readpartial(1024)).not_to be_empty
          end
          progress << true
          client.read
          closed << true
        end
        with_peer(stall) do |proxy_url, listener|
          request = transport_request("https://#{TLSPeer::TARGET}:443", timeout: 0.3)
          request.deadline = nil if ending == :cancellation
          request.cancellation = source.token if ending == :cancellation
          transport = ConnectRPC::Transports::NetHTTP.new(proxy: proxy_url)
          caller = Thread.new { failure(transport, request) }
          expect(progress.pop(timeout: 2)).to be(true)
          source.cancel if ending == :cancellation
          expect(caller.join(2)).to equal(caller)
          expected = ending == :deadline ? "deadline_exceeded" : "canceled"
          expect([caller.value.code, caller.value.cause]).to eq([expected, nil])
          expect(closed.pop(timeout: 2)).to be(true)
          expect(listener.wait_readable(0)).to be_nil
        ensure
          caller&.join(2)
        end
      end
    end

    it "caps a CONNECT exchange without a call deadline" do
      stub_const("ConnectRPC::Transports::NetHTTP::TUNNEL_TIMEOUT", 0.2)
      stall = lambda do |client|
        read_request(client)
        client.read
      end
      with_peer(stall) do |proxy_url|
        request = transport_request("https://#{TLSPeer::TARGET}:443")
        request.deadline = nil
        error = failure(ConnectRPC::Transports::NetHTTP.new(proxy: proxy_url), request)
        expect([error.code, error.message]).to eq(["unavailable", "HTTP transport failed: proxy tunnel timed out"])
      end
    end
  end

  it "rejects a CONNECT reply that is only read after the cap has passed" do
    replied = Queue.new
    sent = Queue.new
    offset = 0
    allow(Process).to receive(:clock_gettime).and_wrap_original do |original, *args|
      original.call(*args) + offset
    end
    # Holds the client's first read until the whole reply is readable, then
    # moves the clock past the cap, as if the client thread were descheduled.
    allow_any_instance_of(Socket).to receive(:read_nonblock).and_wrap_original do |original, *args, **options|
      if offset.zero?
        raise "Reply gate timed out" unless replied.pop(timeout: 2) && original.receiver.wait_readable(2)

        offset = ConnectRPC::Transports::NetHTTP::TUNNEL_TIMEOUT + 1
      end
      original.call(*args, **options)
    end
    proxy = lambda do |client|
      read_request(client)
      client.write("HTTP/1.1 200 Connection established\r\n\r\n")
      replied << true
      # Closing after any byte ends a ClientHello's handshake rather than stalling it.
      sent << client.read(1).to_s
    end
    with_peer(proxy) do |proxy_url|
      request = transport_request("https://#{TLSPeer::TARGET}:443")
      request.deadline = nil
      error = failure(ConnectRPC::Transports::NetHTTP.new(proxy: proxy_url), request)
      expect([error.code, error.message]).to eq(["unavailable", "HTTP transport failed: proxy tunnel timed out"])
    end
    # No TLS ClientHello followed the late reply.
    expect(sent.pop(timeout: 0)).to eq("")
  end

  describe "proxy replies" do
    failures = Struct.new(:errors) do
      def record_failure(error) = errors << error
    end

    {
      "a 407 with a body" => ["HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic " \
                              "realm=\"secret\"\r\nContent-Length: 6\r\n\r\nsecret", "proxy authentication failed"],
      "a 403" => ["HTTP/1.1 403 secret reason\r\nX-Secret: secret\r\n\r\n", "proxy tunnel refused"],
      "an informational status" => ["HTTP/1.1 100 Continue secret\r\n\r\n", "proxy tunnel refused"],
      "an empty reply" => ["", "proxy tunnel failed"],
      "a truncated head" => ["HTTP/1.1 200 OK\r\nX-Partial: secret", "proxy tunnel failed"],
      "a malformed status line" => ["HTTX/1.1 200 secret\r\n\r\n", "proxy tunnel failed"],
      "a malformed field" => ["HTTP/1.1 200 OK\r\nsecret field\r\n\r\n", "proxy tunnel failed"],
      "bytes after a 200 head" => ["HTTP/1.1 200 OK\r\n\r\nsecret", "proxy tunnel failed"],
      "an oversized head" => ["HTTP/1.1 200 OK\r\nX-Pad: #{'secret' * 20}\r\n\r\n", "proxy tunnel failed"]
    }.each do |label, (reply, reason)|
      it "maps #{label} to #{reason} without proxy bytes in errors" do
        stub_const("ConnectRPC::Transports::NetHTTP::MAX_TUNNEL_HEAD", 64) if label.include?("oversized")
        proxy = lambda do |client|
          read_request(client)
          client.write(reply)
          # Closing with unread oversized bytes resets rather than ends.
          client.read unless reply.empty? || label.include?("truncated")
        rescue Errno::ECONNRESET
          nil
        end
        recorded = failures.new([])
        with_peer(proxy) do |proxy_url, listener|
          request = transport_request("https://#{TLSPeer::TARGET}:443")
          request.lifecycle = recorded
          error = failure(ConnectRPC::Transports::NetHTTP.new(proxy: proxy_url), request)
          expect([error.code, error.message]).to eq(["unavailable", "HTTP transport failed: #{reason}"])
          expect(error.cause).to be_a(ConnectRPC::Transports::NetHTTP::ProxyFailure)
          texts = [error.message, error.cause.message, error.cause.inspect, *recorded.errors.map(&:message)]
          expect(texts.join).not_to include("secret", "127.0.0.1", TLSPeer::TARGET)
          expect(listener.wait_readable(0)).to be_nil
        end
      end
    end
  end

  describe "unusable environment proxies" do
    {
      "an unsupported scheme" => [{ "HTTPS_PROXY" => "socks5://127.0.0.1:1080" }, "https", "unsupported proxy scheme"],
      "an https proxy" => [{ "HTTPS_PROXY" => "https://127.0.0.1:1080" }, "https", "unsupported proxy scheme"],
      "an invalid port" => [{ "HTTPS_PROXY" => "http://127.0.0.1:0" }, "https", "invalid proxy address"],
      "a host Go's IDNA rejects" => [{ "HTTPS_PROXY" => "http://proxy\uFF20127.0.0.1:9" }, "https",
                                     "invalid proxy address"],
      "a CGI request" => [{ "HTTP_PROXY" => "http://127.0.0.1:9", "REQUEST_METHOD" => "POST" }, "http",
                          "proxy refused in CGI environment"]
    }.each do |label, (env, scheme, reason)|
      it "fails #{label} before opening a socket" do
        transport = with_env(env) { ConnectRPC::Transports::NetHTTP.new }
        expect(Socket).not_to receive(:new)
        error = failure(transport, transport_request("#{scheme}://#{TLSPeer::TARGET}"))
        expect([error.code, error.message]).to eq(["unavailable", "HTTP transport failed: #{reason}"])
      end
    end

    it "ignores CGI for an explicit proxy" do
      heads = Queue.new
      with_peer(origin(heads)) do |proxy_url|
        transport = with_env("REQUEST_METHOD" => "POST") { ConnectRPC::Transports::NetHTTP.new(proxy: proxy_url) }
        expect(transport.call(transport_request("http://#{TLSPeer::TARGET}/svc")).body).to eq("ok")
      end
      expect(heads.pop(timeout: 0)).to start_with("POST http://#{TLSPeer::TARGET}/svc HTTP/1.1\r\n")
    end
  end

  it "rejects explicit proxy options without echoing them" do
    {
      "socks5://user:secret@proxy.test:1080" => "Unsupported proxy scheme",
      "https://user:secret@proxy.test" => "Unsupported proxy scheme",
      "http://user:secret@proxy.test:99999" => "Invalid proxy configuration",
      URI("http://user:secret@:8080") => "Invalid proxy configuration",
      42 => "Invalid proxy configuration"
    }.each do |option, message|
      expect { ConnectRPC::Transports::NetHTTP.new(proxy: option) }.to raise_error(ArgumentError, message)
    end
  end
end
