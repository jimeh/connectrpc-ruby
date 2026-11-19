# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../../../spec/fixtures/generation/ruby", __dir__))
require "generation/streaming_connect"
require_relative "support/threaded_stream_helpers"
require_relative "support/per_call_helpers"
require_relative "support/connect_proxy"

RSpec.describe "PerCallTransport proxies" do
  include ThreadedStreamHelpers
  include PerCallHelpers

  def target = ConnectProxy::TARGET

  around do |example|
    keys = %w[HTTP_PROXY http_proxy HTTPS_PROXY https_proxy NO_PROXY no_proxy REQUEST_METHOD]
    saved = keys.to_h { |key| [key, ENV.fetch(key, nil)] }
    keys.each { |key| ENV.delete(key) }
    example.run
  ensure
    saved.each { |key, value| value.nil? ? ENV.delete(key) : ENV[key] = value }
  end

  # The environment is set only while a transport takes its snapshot.
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

  # Host TLS for the target name, issued by the shared test root.
  def target_tls
    @target_tls ||= begin
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
      host_tls([certificate, key])
    end
  end

  def connect_head(authority, credentials = nil)
    auth = credentials ? "Proxy-Authorization: Basic #{[credentials].pack('m0')}\r\n" : ""
    "CONNECT #{authority} HTTP/1.1\r\nHost: #{authority}\r\n#{auth}\r\n"
  end

  def failure_of(path, transport, request)
    error = on_path(path) { capture_error { transport.call(request) } }
    raise "Expected a transport failure" unless error.is_a?(Exception)

    error
  end

  # Counts SETTINGS frames with the ACK flag in a client's frames after its
  # preface.
  def settings_acks(bytes)
    acks = 0
    offset = 0
    while offset + 9 <= bytes.bytesize
      acks += 1 if bytes.getbyte(offset + 3) == 4 && bytes.getbyte(offset + 4).anybits?(1)
      offset += 9 + ((bytes.getbyte(offset) << 16) | bytes.byteslice(offset + 1, 2).unpack1("n"))
    end
    acks
  end

  # Sends the client preface to an h2c target ahead of the client and returns
  # the target's first frame, its SETTINGS.
  def target_settings(upstream)
    upstream.write(ConnectProxy::PREFACE)
    frame = upstream.read(9)
    raise "Expected SETTINGS first, got #{frame.inspect}" unless frame.getbyte(3) == 4

    frame << upstream.read((frame.getbyte(0) << 16) | frame.unpack1("@1n")).to_s
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

  %i[h2c tls mtls].each do |mode|
    PerCallHelpers::PATHS.each do |path|
      it "tunnels Connect and gRPC calls to a #{mode} target through an explicit proxy on the #{path} path" do
        proxy = ConnectProxy.new
        with_ca do |ca_file|
          tls, scheme, options = threaded_mode(mode, ca_file)
          peer = mixed_host(tls: tls)
          port = peer.peer.port
          origin = "#{scheme}://127.0.0.1:#{port}"
          transport = per_call(proxy: proxy.url("us%3Aer:p%40ss+"), **options)
          with_boundaries do |boundaries|
            values = on_path(path) do
              [mixed_client(transport, origin).lookup(item("connect")).message.name,
               streamed(mixed_client(transport, origin), "ss"),
               mixed_client(transport, origin, protocol: :grpc).lookup(item("grpc")).message.name]
            end
            raise values if values.is_a?(Exception)

            expect(values).to eq(["connect", %w[ss-0 ss-1], "grpc"])
            observations = boundaries.take
            expect(observations.size).to eq(3)
            observations.each { |observation| expect_released(observation) }
          end
          heads = 3.times.map { pop_within(proxy.heads, "a CONNECT head") }
          expect(heads).to eq([connect_head("127.0.0.1:#{port}", "us:er:p@ss+")] * 3)
          expect(3.times.map { pop_within(proxy.closed, "the client to end its tunnel") }).to eq([true] * 3)
          expect([proxy.accepted, peer.peer.accepts.size]).to eq([3, 3])
        ensure
          peer&.stop
        end
      ensure
        proxy&.stop
      end
    end
  end

  PerCallHelpers::PATHS.each do |path|
    it "tunnels h2c and TLS targets through an HTTPS_PROXY snapshot, never HTTP_PROXY, on the #{path} path" do
      proxy = ConnectProxy.new
      plain = mixed_host
      secure = mixed_host(tls: target_tls)
      with_ca do |ca_file|
        # HTTP_PROXY names a closed port, so using it would refuse the h2c call.
        env = { "HTTPS_PROXY" => proxy.url, "HTTP_PROXY" => "http://127.0.0.1:9" }
        h2c, tls = with_env(env) { [per_call, per_call(ca_file: ca_file)] }
        names = on_path(path) do
          [mixed_client(h2c, "http://#{target}:#{plain.peer.port}").lookup(item("h2c")).message.name,
           mixed_client(tls, "https://#{target}:#{secure.peer.port}", protocol: :grpc)
             .lookup(item("tls")).message.name]
        end
        raise names if names.is_a?(Exception)

        expect(names).to eq(%w[h2c tls])
      end
      heads = 2.times.map { pop_within(proxy.heads, "a CONNECT head") }
      expect(heads).to eq([connect_head("#{target}:#{plain.peer.port}"), connect_head("#{target}:#{secure.peer.port}")])
    ensure
      plain&.stop
      secure&.stop
      proxy&.stop
    end

    it "routes as grpc-go does: HTTPS_PROXY for every scheme by the URL's actual port, then NO_PROXY and " \
       "loopback, on the #{path} path" do
      dials = stop_dials
      proxy = "http://proxy.invalid:3128"
      via_proxy = ["proxy.invalid", 3128]
      cases = [
        [{ "HTTPS_PROXY" => proxy }, {}, "http://#{target}/x", via_proxy],
        [{ "HTTPS_PROXY" => proxy }, {}, "https://#{target}/x", via_proxy],
        [{ "HTTP_PROXY" => proxy }, {}, "http://#{target}:8080/x", [target, 8080]],
        [{ "HTTPS_PROXY" => proxy, "NO_PROXY" => "#{target}:80" }, {}, "http://#{target}/x", [target, 80]],
        [{ "HTTPS_PROXY" => proxy, "NO_PROXY" => "#{target}:80" }, {}, "https://#{target}/x", via_proxy],
        [{ "HTTPS_PROXY" => proxy, "NO_PROXY" => ".test" }, {}, "https://#{target}:8443/x", [target, 8443]],
        [{ "HTTPS_PROXY" => proxy }, {}, "http://127.0.0.1:8080/x", ["127.0.0.1", 8080]],
        [{ "HTTPS_PROXY" => proxy }, {}, "https://localhost:8443/x", ["localhost", 8443]],
        [{ "HTTPS_PROXY" => proxy }, { proxy: nil }, "https://#{target}/x", [target, 443]],
        [{ "NO_PROXY" => "*" }, { proxy: proxy }, "http://127.0.0.1/x", via_proxy]
      ]
      cases.each do |env, options, url, expected|
        transport = with_env(env) { per_call(**options) }
        error = failure_of(path, transport, request(url))
        expect([error.class, error.code]).to eq([ConnectRPC::Error, "unavailable"])
        expect([url, pop_within(dials, "a dial")]).to eq([url, expected])
      end
    end

    it "fails an unusable environment proxy closed before any socket on the #{path} path" do
      listener = TCPServer.new("127.0.0.1", 0)
      port = listener.addr[1]
      {
        "socks5://127.0.0.1:#{port}" => "unsupported proxy scheme",
        "https://127.0.0.1:#{port}" => "unsupported proxy scheme",
        "http://127.0.0.1:0" => "invalid proxy address",
        "http://127.0.0.1:#{port} " => "invalid proxy address"
      }.each do |value, reason|
        transport = with_env("HTTPS_PROXY" => value) { per_call }
        expect(IO::Endpoint).not_to receive(:tcp)
        with_boundaries do |boundaries|
          error = failure_of(path, transport, request("http://#{target}:8080/x"))
          expect([error.class, error.code, error.message])
            .to eq([ConnectRPC::Error, "unavailable", "HTTP/2 transport failed: #{reason}"])
          expect([error.cause.class, error.cause.message,
                  error.cause.cause]).to eq([AsyncHelpers::INTERNAL::ProxyFailure, reason, nil])
          expect(boundaries.dialed).to eq([])
        end
      end
      expect(listener.wait_readable(0)).to be_nil
    ensure
      listener&.close
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
      "bytes after a 200 head to a TLS target" => ["HTTP/1.1 200 OK\r\n\r\nsecret", "proxy tunnel failed"],
      "an oversized head" => ["HTTP/1.1 200 OK\r\nX-Pad: #{'secret' * 20}\r\n\r\n", "proxy tunnel failed"]
    }.each do |label, (reply, reason)|
      it "maps #{label} to #{reason} without proxy bytes or credentials on the #{path} path" do
        stub_const("ConnectRPC::Async::Internal::ProxyTunnel::MAX_HEAD", 64) if label.include?("oversized")
        proxy = ConnectProxy.new do |client, _head, peer|
          client.write(reply)
          # A reply without its ending must end the connection to be complete.
          reply.empty? || label.include?("truncated") ? client.close : peer.wait_closed(client)
        end
        scheme = label.include?("TLS") ? "https" : "http"
        transport = per_call(proxy: proxy.url("user:hunter2"))
        with_boundaries do |boundaries|
          error = failure_of(path, transport, request("#{scheme}://#{target}:8443/x"))
          expect([error.class, error.code, error.message])
            .to eq([ConnectRPC::Error, "unavailable", "HTTP/2 transport failed: #{reason}"])
          expect(error.cause).to be_a(AsyncHelpers::INTERNAL::ProxyFailure)
          texts = [error.message, error.cause.message, error.cause.inspect].join
          expect(texts).not_to include("secret", "hunter2", "user", "127.0.0.1", proxy.port.to_s, target)
          expect_released(boundaries.take.first)
        end
      ensure
        proxy&.stop
      end
    end

    # The proxy sends its 200 and the target's SETTINGS in one write: it
    # prefetches them by sending the client's preface ahead and then drops the
    # client's own copy. The call alone cannot show the SETTINGS reached the
    # connection, since the client does not require them, but its one SETTINGS
    # ACK can.
    it "leaves an h2c target's SETTINGS that arrive with the 200 for the connection on the #{path} path" do
      peer = mixed_host
      sent = +"".b
      proxy = ConnectProxy.new do |client, head, relay|
        upstream = relay.upstream(head)
        client.write(ConnectProxy::ESTABLISHED + target_settings(upstream))
        raise "Expected the client preface" unless client.read(ConnectProxy::PREFACE.bytesize) == ConnectProxy::PREFACE

        relay.splice(client, upstream, tap: sent)
      end
      transport = per_call(proxy: proxy.url)
      name = on_path(path) do
        mixed_client(transport, "http://#{target}:#{peer.peer.port}").lookup(item("settings")).message.name
      end
      expect(name).to eq("settings")
      expect(pop_within(proxy.closed, "the client to end its tunnel")).to be(true)
      expect(settings_acks(sent)).to eq(1)
    ensure
      peer&.stop
      proxy&.stop
    end

    # The 200 head's end is split across two reads: the client has consumed
    # the first part, which holds no end, before the proxy sends the rest
    # together with the target's SETTINGS. The end is found across the reads,
    # and the SETTINGS stay for the connection, as its one SETTINGS ACK shows.
    [["\r\n\r", "\n"], ["\r\n", "\r\n"], ["\r", "\n\r\n"], ["\n", "\n"]].each do |first, rest|
      it "finds a head end split as #{first.inspect} then #{rest.inspect} across reads without consuming past it " \
         "on the #{path} path" do
        peer = mixed_host
        consumed = Thread::Queue.new
        trace = TracePoint.new(:return) { |point| consumed << point.return_value.bytesize if point.return_value }
        trace.enable(target: AsyncHelpers::INTERNAL::ProxyTunnel.method(:consume))
        sent = +"".b
        proxy = ConnectProxy.new do |client, head, relay|
          upstream = relay.upstream(head)
          settings = target_settings(upstream)
          part = "HTTP/1.1 200 Connection established#{first}"
          client.write(part)
          taken = 0
          taken += pop_within(consumed, "the client to consume the head's first part") while taken < part.bytesize
          client.write(rest + settings)
          # A client that consumed the SETTINGS as head bytes never sends its preface; the call then fails.
          next unless client.read(ConnectProxy::PREFACE.bytesize) == ConnectProxy::PREFACE

          relay.splice(client, upstream, tap: sent)
        end
        transport = per_call(proxy: proxy.url, connect_timeout: 5)
        name = on_path(path) do
          mixed_client(transport, "http://#{target}:#{peer.peer.port}").lookup(item("split")).message.name
        end
        expect(name).to eq("split")
        expect(pop_within(proxy.closed, "the client to end its tunnel")).to be(true)
        expect(settings_acks(sent)).to eq(1)
      ensure
        trace&.disable
        peer&.stop
        proxy&.stop
      end
    end

    # An explicit proxy applies to loopback too. The proxy dials the target's
    # port on 127.0.0.1 whatever the requested host, so no IPv6 route is
    # needed to check the request the client sends.
    it "brackets an IPv6 literal target in the CONNECT line and Host field on the #{path} path" do
      proxy = ConnectProxy.new
      peer = mixed_host
      port = peer.peer.port
      transport = per_call(proxy: proxy.url)
      with_boundaries do |boundaries|
        name = on_path(path) { mixed_client(transport, "http://[::1]:#{port}").lookup(item("v6")).message.name }
        expect(name).to eq("v6")
        expect_released(boundaries.take.first)
      end
      expect(pop_within(proxy.heads, "a CONNECT head")).to eq(connect_head("[::1]:#{port}"))
      expect(pop_within(proxy.closed, "the client to end its tunnel")).to be(true)
    ensure
      peer&.stop
      proxy&.stop
    end

    it "aborts a CONNECT the proxy never answers at connect_timeout on the #{path} path" do
      proxy = ConnectProxy.new { |client, _head, peer| peer.wait_closed(client) }
      transport = per_call(proxy: proxy.url, connect_timeout: 0.3)
      with_boundaries do |boundaries|
        error = failure_of(path, transport, request("https://#{target}/x"))
        expect([error.class, error.code, error.message])
          .to eq([ConnectRPC::Error, "unavailable", "HTTP/2 transport failed: timed out"])
        expect_released(boundaries.take.first)
      end
      expect(pop_within(proxy.closed, "the client to end its tunnel")).to be(true)
    ensure
      proxy&.stop
    end

    it "caps a CONNECT exchange by Go's one-minute bound, stubbed shorter, on the #{path} path" do
      stub_const("ConnectRPC::Async::Internal::ProxyTunnel::TIMEOUT", 0.2)
      proxy = ConnectProxy.new { |client, _head, peer| peer.wait_closed(client) }
      transport = per_call(proxy: proxy.url)
      error = failure_of(path, transport, request("http://#{target}/x"))
      expect([error.class, error.code, error.message])
        .to eq([ConnectRPC::Error, "unavailable", "HTTP/2 transport failed: proxy tunnel timed out"])
      expect(pop_within(proxy.closed, "the client to end its tunnel")).to be(true)
    ensure
      proxy&.stop
    end

    %i[deadline cancellation].each do |ending|
      it "ends a pending CONNECT with #{ending} and closes the proxy connection on the #{path} path" do
        progress = Thread::Queue.new
        proxy = ConnectProxy.new do |client, _head, peer|
          progress << true
          peer.wait_closed(client)
        end
        transport = per_call(proxy: proxy.url)
        source = ConnectRPC::CancellationSource.new
        call = request("https://#{target}/x", deadline: (ConnectRPC::Deadline.new(0.3) if ending == :deadline))
        call.cancellation = source.token if ending == :cancellation
        with_boundaries do |boundaries|
          error = if path == :threaded
                    caller = Thread.new { capture_error { transport.call(call) } }
                    pop_within(progress, "the proxy to receive CONNECT")
                    source.cancel
                    value_within(caller, "the caller")
                  else
                    reactor do |task|
                      caller = task.async { capture_error { transport.call(call) } }
                      pop_within(progress, "the proxy to receive CONNECT")
                      source.cancel
                      caller.wait
                    end
                  end
          expected = ending == :deadline ? "deadline_exceeded" : "canceled"
          expect([error.class, error.code]).to eq([ConnectRPC::Error, expected])
          expect_released(boundaries.take.first)
        end
        expect(pop_within(proxy.closed, "the client to end its tunnel")).to be(true)
      ensure
        proxy&.stop
      end
    end
  end

  it "rejects explicit proxy options without echoing them and shows only the proxy mode" do
    {
      "socks5://user:secret@proxy.test:1080" => "Unsupported proxy scheme",
      "https://user:secret@proxy.test" => "Unsupported proxy scheme",
      "http://user:secret@proxy.test:99999" => "Invalid proxy configuration",
      URI("http://user:secret@:8080") => "Invalid proxy configuration",
      :direct => "Invalid proxy configuration",
      42 => "Invalid proxy configuration"
    }.each do |option, message|
      error = capture_error { per_call(proxy: option) }
      expect([error.class, error.message, error.cause]).to eq([ArgumentError, message, nil])
    end
    modes = [
      per_call(proxy: "http://user:secret@proxy.test:3128"),
      per_call(proxy: nil),
      with_env("HTTPS_PROXY" => "http://user:secret@proxy.test:3128") { per_call(connect_timeout: 2) }
    ]
    expect(modes.map(&:inspect)).to eq([
                                         "#<ConnectRPC::Async::PerCallTransport connect_timeout=10 proxy=explicit>",
                                         "#<ConnectRPC::Async::PerCallTransport connect_timeout=10 proxy=direct>",
                                         "#<ConnectRPC::Async::PerCallTransport connect_timeout=2 proxy=environment>"
                                       ])
  end

  # Only PerCallTransport reads the proxy environment in this release.
  it "leaves pooled transports direct whatever the proxy environment" do
    dials = stop_dials
    origin = "http://#{target}:8080"
    with_env("HTTPS_PROXY" => "http://proxy.invalid:3128", "HTTP_PROXY" => "http://proxy.invalid:3128") do
      reactor do
        [ConnectRPC::Async::Transport, ConnectRPC::Async::HTTP1Transport].each do |transport_class|
          transport = transport_class.new(origin: origin)
          capture_error { transport.call(request("#{origin}/x")) }
          transport.close(grace: 0)
        end
      end
      threaded = threaded(origin: origin)
      capture_error { threaded.call(request("#{origin}/x")) }
      threaded.close(grace: 0)
    end
    expect(3.times.map { pop_within(dials, "a pooled dial") }).to eq([[target, 8080]] * 3)
  end
end
