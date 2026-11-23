# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../../../spec/fixtures/generation/ruby", __dir__))
require "generation/streaming_connect"
require_relative "support/threaded_stream_helpers"
require_relative "support/per_call_helpers"
require_relative "support/proxy_helpers"

RSpec.describe "Pooled transport proxies" do
  include ThreadedStreamHelpers
  include PerCallHelpers
  include ProxyHelpers

  def pooled(kind, **)
    { transport: ConnectRPC::Async::Transport, threaded: ConnectRPC::Async::ThreadedTransport,
      http1: ConnectRPC::Async::HTTP1Transport }.fetch(kind).new(**)
  end

  def prefix(kind) = kind == :http1 ? "HTTP/1" : "HTTP/2"

  # Builds a transport and runs the block with it where its calls belong:
  # ThreadedTransport calls on a plain thread, the others inside a reactor.
  # The transport closes afterwards and an exception that escapes the block
  # is raised; one the block returns is its value.
  def with_pooled(kind, build)
    result = if kind == :threaded
               transport = build.call
               begin
                 value_within(Thread.new { capture_error_or_value { [yield(transport)] } }, "a threaded caller")
               ensure
                 transport.close(grace: 0)
               end
             else
               reactor do
                 built = build.call
                 capture_error_or_value { [yield(built)] }
               ensure
                 built&.close(grace: 0)
               end
             end
    raise result if result.is_a?(Exception)

    result.first
  end

  def http1_ok(body = "ok") = "HTTP/1.1 200 OK\r\nContent-Length: #{body.bytesize}\r\n\r\n#{body}"

  def recording_lifecycle(recorded)
    lifecycle = Object.new
    lifecycle.define_singleton_method(:activate) { |&block| block.call }
    lifecycle.define_singleton_method(:record_failure) { |error| recorded << error }
    lifecycle
  end

  def causes(error)
    chain = []
    chain << error while (error = error.cause)
    chain
  end

  # The values of a head's field, matching its name case-insensitively.
  def field_values(head, name)
    head.split("\r\n").drop(1).filter_map do |line|
      field, value = line.split(":", 2)
      value.strip if value && field.casecmp?(name)
    end
  end

  def basic(credentials) = "Basic #{[credentials].pack('m0')}"

  %i[transport threaded].each do |kind|
    it "tunnels h2c through an HTTP_PROXY snapshot and TLS through HTTPS_PROXY with basic auth, one CONNECT per " \
       "pooled connection, on #{kind}" do
      plain_proxy = ConnectProxy.new
      secure_proxy = ConnectProxy.new
      plain = mixed_host
      secure = mixed_host(tls: target_tls)
      env = { "HTTP_PROXY" => plain_proxy.url("us%3Aer:p%40ss+"), "HTTPS_PROXY" => secure_proxy.url("tls:pa%2Fss") }
      with_ca do |ca_file|
        names = [["http://#{target}:#{plain.peer.port}", {}],
                 ["https://#{target}:#{secure.peer.port}", { ca_file: ca_file }]].map do |origin, options|
          with_pooled(kind, -> { with_env(env) { pooled(kind, origin: origin, **options) } }) do |transport|
            expect(transport.inspect).to end_with(" proxy=environment>")
            [mixed_client(transport, origin).lookup(item("connect")).message.name,
             streamed(mixed_client(transport, origin), "ss"),
             mixed_client(transport, origin, protocol: :grpc).lookup(item("grpc")).message.name]
          end
        end
        expect(names).to eq([["connect", %w[ss-0 ss-1], "grpc"]] * 2)
      end
      expect(pop_within(plain_proxy.heads, "a CONNECT head"))
        .to eq(connect_head("#{target}:#{plain.peer.port}", "us:er:p@ss+"))
      expect(pop_within(secure_proxy.heads, "a CONNECT head"))
        .to eq(connect_head("#{target}:#{secure.peer.port}", "tls:pa/ss"))
      # One tunnelled connection per transport carried all three calls.
      expect([plain_proxy.accepted, secure_proxy.accepted, plain_proxy.heads.size, secure_proxy.heads.size,
              plain.peer.accepts.size, secure.peer.accepts.size]).to eq([1, 1, 0, 0, 1, 1])
    ensure
      plain&.stop
      secure&.stop
      plain_proxy&.stop
      secure_proxy&.stop
    end
  end

  it "tunnels an HTTP/1 https origin through HTTPS_PROXY with CONNECT, then TLS to the target, on one pooled " \
     "connection" do
    peer = ProxyHelpers::HTTP1Peer.new(method(:connect_echo), tls: target_identity)
    proxy = ConnectProxy.new
    origin = "https://#{target}:#{peer.port}"
    with_ca do |ca_file|
      env = { "HTTPS_PROXY" => proxy.url("user:pa%3Ass") }
      names = with_pooled(:http1, -> { with_env(env) { pooled(:http1, origin: origin, ca_file: ca_file) } }) do |h1|
        typed = mixed_client(h1, origin)
        [typed.lookup(item("one")).message.name, typed.lookup(item("two")).message.name, streamed(typed, "ss")]
      end
      expect(names).to eq(["one", "two", ["ss"]])
    end
    expect(pop_within(proxy.heads, "a CONNECT head")).to eq(connect_head("#{target}:#{peer.port}", "user:pa:ss"))
    # Inside the tunnel the requests are origin-form and carry no proxy credentials.
    heads = 3.times.map { pop_within(peer.requests, "a tunnelled request").first }
    expect(heads.map { |head| head[/\A\S+ \S+/] }).to all(match(%r{\APOST /\S+\z}))
    expect(heads.join).not_to match(/proxy-authorization/i)
    expect([proxy.accepted, proxy.heads.size]).to eq([1, 0])
  ensure
    peer&.stop
    proxy&.stop
  end

  it "forwards an HTTP/1 http origin's unary and streamed calls to HTTP_PROXY in absolute form with " \
     "Proxy-Authorization on each, over one proxy connection" do
    proxy = ForwardProxy.new { |head, body, _count| connect_echo(head, body) }
    origin = "http://#{target}:8080"
    env = { "HTTP_PROXY" => proxy.url("us%3Aer:p%40ss+") }
    names = with_pooled(:http1, -> { with_env(env) { pooled(:http1, origin: origin) } }) do |h1|
      expect(h1.inspect).to end_with(" proxy=environment>")
      typed = mixed_client(h1, origin)
      [typed.lookup(item("one")).message.name, typed.lookup(item("two")).message.name, streamed(typed, "ss")]
    end
    expect(names).to eq(["one", "two", ["ss"]])
    heads = 3.times.map { pop_within(proxy.heads, "a forwarded request head") }
    heads.each do |head|
      expect(head).to start_with("POST http://#{target}:8080/")
      expect([field_values(head, "host"), field_values(head, "proxy-authorization")])
        .to eq([["#{target}:8080"], [basic("us:er:p@ss+")]])
    end
    expect([proxy.accepted, pop_within(proxy.closed, "the client to end its proxy connection")]).to eq([1, true])
  ensure
    proxy&.stop
  end

  it "forwards to an http origin on the default port with no port in the absolute target or Host, as " \
     "Net::HTTP does" do
    proxy = ForwardProxy.new { http1_ok }
    origin = "http://#{target}"
    body = with_pooled(:http1, -> { pooled(:http1, origin: origin, proxy: proxy.url) }) do |h1|
      h1.call(request("#{origin}/x?y=1")).body
    end
    head = pop_within(proxy.heads, "a forwarded request head")
    expect([body, head.lines.first, field_values(head, "host")])
      .to eq(["ok", "POST http://#{target}/x?y=1 HTTP/1.1\r\n", [target]])
  ensure
    proxy&.stop
  end

  {
    "a 407 with a framed body" => ["HTTP/1.0 407 Proxy Authentication Required\r\nContent-Length: 4\r\n\r\ndeny",
                                   407, "unknown"],
    "a close-delimited 502" => [["HTTP/1.0 502 Bad Gateway\r\nContent-Type: text/html\r\n\r\n<html>502</html>",
                                 :close], 502, "unavailable"]
  }.each do |label, (reply, status, code)|
    it "maps a forward proxy's HTTP/1.0 #{label} as an HTTP error and opens a new proxy connection for the next " \
       "call" do
      proxy = ForwardProxy.new { reply }
      origin = "http://#{target}:8080"
      errors = with_pooled(:http1, -> { pooled(:http1, origin: origin, proxy: proxy.url) }) do |h1|
        typed = mixed_client(h1, origin)
        2.times.map { capture_error { typed.lookup(item("x")) } }
      end
      expect(errors.map { |error| [error.class, error.code, error.message] })
        .to eq([[ConnectRPC::Error, code, "HTTP #{status}"]] * 2)
      # The client retires a connection after any HTTP error reply.
      expect([proxy.accepted, 2.times.map { pop_within(proxy.heads, "a forwarded request head")[/\A\S+/] }])
        .to eq([2, %w[POST POST]])
    ensure
      proxy&.stop
    end
  end

  it "reuses a forward proxy's HTTP/1.0 connection only when its reply asks for keep-alive" do
    { "" => 2, "Connection: keep-alive\r\n" => 1 }.each do |field, connections|
      proxy = ForwardProxy.new { "HTTP/1.0 200 OK\r\n#{field}Content-Length: 2\r\n\r\nok" }
      origin = "http://#{target}:8080"
      bodies = with_pooled(:http1, -> { pooled(:http1, origin: origin, proxy: proxy.url) }) do |h1|
        2.times.map { h1.call(request("#{origin}/x")).body }
      end
      expect([field, bodies, proxy.accepted]).to eq([field, %w[ok ok], connections])
    ensure
      proxy&.stop
    end
  end

  # RFC 9112 section 6.1 makes transfer coding in an HTTP/1.0 message faulty
  # framing, so the connection must not return to the pool.
  it "fails a forward proxy's chunked HTTP/1.0 reply and never reuses its connection" do
    reply = "HTTP/1.0 200 OK\r\nConnection: keep-alive\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nok\r\n0\r\n\r\n"
    proxy = ForwardProxy.new { reply }
    origin = "http://#{target}:8080"
    errors = with_pooled(:http1, -> { pooled(:http1, origin: origin, proxy: proxy.url) }) do |h1|
      2.times.map { capture_error { h1.call(request("#{origin}/x")) } }
    end
    expect(errors.map { |error| [error.class, error.code, error.message] })
      .to eq([[ConnectRPC::Error, "unavailable", "HTTP/1 transport failed: malformed response"]] * 2)
    expect(proxy.accepted).to eq(2)
  ensure
    proxy&.stop
  end

  it "returns a forward proxy's well-formed HTTP error reply as the HTTP/1 call's response" do
    reply = "HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 4\r\n\r\ndeny"
    proxy = ForwardProxy.new { reply }
    origin = "http://#{target}:8080"
    response = with_pooled(:http1, -> { pooled(:http1, origin: origin, proxy: proxy.url) }) do |h1|
      h1.call(request("#{origin}/x"))
    end
    expect([response.status, response.body]).to eq([407, "deny"])
  ensure
    proxy&.stop
  end

  %i[transport threaded http1].each do |kind|
    it "routes #{kind} by its origin's scheme as Go net/http does, then NO_PROXY and loopback, with an explicit " \
       "proxy overriding the environment" do
      dials = stop_dials
      proxy = "http://proxy.invalid:3128"
      via = ["proxy.invalid", 3128]
      cases = [
        [{ "HTTP_PROXY" => proxy }, {}, "http://#{target}:8080", via],
        [{ "HTTP_PROXY" => proxy }, {}, "https://#{target}:8443", [target, 8443]],
        [{ "HTTPS_PROXY" => proxy }, {}, "https://#{target}:8443", via],
        [{ "HTTPS_PROXY" => proxy }, {}, "http://#{target}:8080", [target, 8080]],
        [{ "HTTP_PROXY" => proxy, "NO_PROXY" => "#{target}:8080" }, {}, "http://#{target}:8080", [target, 8080]],
        [{ "HTTPS_PROXY" => proxy, "NO_PROXY" => ".test" }, {}, "https://#{target}:8443", [target, 8443]],
        [{ "HTTP_PROXY" => proxy }, {}, "http://127.0.0.1:8080", ["127.0.0.1", 8080]],
        [{ "HTTPS_PROXY" => proxy }, {}, "https://localhost:8443", ["localhost", 8443]],
        [{ "HTTPS_PROXY" => proxy }, { proxy: nil }, "https://#{target}:8443", [target, 8443]],
        [{ "HTTP_PROXY" => proxy }, { proxy: "http://other.invalid:8888" }, "http://#{target}:8080",
         ["other.invalid", 8888]],
        [{ "NO_PROXY" => "*" }, { proxy: proxy }, "http://127.0.0.1:8080", via]
      ]
      cases.each do |env, options, origin, expected|
        error = with_pooled(kind, -> { with_env(env) { pooled(kind, origin: origin, **options) } }) do |transport|
          capture_error { transport.call(request("#{origin}/x")) }
        end
        expect([error.class, error.code]).to eq([ConnectRPC::Error, "unavailable"])
        expect([env, options, origin, pop_within(dials, "a dial")]).to eq([env, options, origin, expected])
      end
    end

    it "fails an unusable environment proxy closed before any socket on #{kind}" do
      listener = TCPServer.new("127.0.0.1", 0)
      port = listener.addr[1]
      cases = {
        "socks5://127.0.0.1:#{port}" => "unsupported proxy scheme",
        "https://127.0.0.1:#{port}" => "unsupported proxy scheme",
        "http://127.0.0.1:0" => "invalid proxy address"
      }.flat_map do |value, reason|
        [[{ "HTTP_PROXY" => value }, "http://#{target}:8080", reason],
         [{ "HTTPS_PROXY" => value }, "https://#{target}:8443", reason]]
      end
      cases << [{ "REQUEST_METHOD" => "GET", "HTTP_PROXY" => "http://127.0.0.1:#{port}" },
                "http://#{target}:8080", "proxy refused in CGI environment"]
      expect(IO::Endpoint).not_to receive(:tcp)
      cases.each do |env, origin, reason|
        error = with_pooled(kind, -> { with_env(env) { pooled(kind, origin: origin) } }) do |transport|
          capture_error { transport.call(request("#{origin}/x")) }
        end
        expect([env, error.class, error.code, error.message])
          .to eq([env, ConnectRPC::Error, "unavailable", "#{prefix(kind)} transport failed: #{reason}"])
        expect([error.cause.class, error.cause.message, error.cause.cause])
          .to eq([AsyncHelpers::INTERNAL::ProxyFailure, reason, nil])
      end
      expect(listener.wait_readable(0)).to be_nil
    ensure
      listener&.close
    end

    it "fails proxy-hop problems on #{kind} as unavailable without credentials or proxy bytes" do
      reserved = refusing_socket
      replies = {
        "a 407" => ["HTTP/1.1 407 secret\r\nProxy-Authenticate: Basic realm=\"secret\"\r\nContent-Length: 6\r\n\r\n" \
                    "secret", "proxy authentication failed"],
        "a 403" => ["HTTP/1.1 403 secret reason\r\nX-Secret: secret\r\n\r\n", "proxy tunnel refused"]
      }
      replies.each do |label, (reply, reason)|
        proxy = ConnectProxy.new do |client, _head, peer|
          client.write(reply)
          peer.wait_closed(client)
        end
        origin = "https://#{target}:8443"
        error, shown = with_pooled(kind, -> { pooled(kind, origin: origin, proxy: proxy.url("user:hunter2")) }) do |t|
          [capture_error { t.call(request("#{origin}/x")) }, t.inspect]
        end
        expect([label, error.class, error.code, error.message])
          .to eq([label, ConnectRPC::Error, "unavailable", "#{prefix(kind)} transport failed: #{reason}"])
        expect([error.cause.class, error.cause.cause]).to eq([AsyncHelpers::INTERNAL::ProxyFailure, nil])
        texts = [error.message, error.cause.message, error.cause.inspect, shown].join
        expect(texts).not_to include("secret", "hunter2", "user", "127.0.0.1", proxy.port.to_s)
        expect(shown).to end_with(" proxy=explicit>")
        expect(pop_within(proxy.closed, "the client to close its proxy connection")).to be(true)
      ensure
        proxy&.stop
      end
      origins = ["https://#{target}:8443", *("http://#{target}:8080" if kind == :http1)]
      origins.each do |origin|
        url = "http://user:hunter2@127.0.0.1:#{reserved.local_address.ip_port}"
        error = with_pooled(kind, -> { pooled(kind, origin: origin, proxy: url) }) do |transport|
          capture_error { transport.call(request("#{origin}/x")) }
        end
        expect([origin, error.code, error.message])
          .to eq([origin, "unavailable", "#{prefix(kind)} transport failed: connection refused"])
        # The native dial error is kept as the cause, as NetHTTP keeps it; it
        # names the proxy address but never its URL or credentials.
        expect(causes(error).map(&:class)).to eq([Errno::ECONNREFUSED])
        expect([error.message, *causes(error).map(&:message)].join).not_to include("hunter2", "user")
      end
    ensure
      reserved&.close
    end
  end

  it "counts a proxy failure as a failed HTTP/2 connection attempt that backs off" do
    proxy = ConnectProxy.new do |client, _head, peer|
      client.write("HTTP/1.1 407 Proxy Authentication Required\r\n\r\n")
      peer.wait_closed(client)
    end
    reserved = refusing_socket
    origin = "http://#{target}:8080"
    reactor do
      backoff = ConnectRPC::Async::Backoff.new(base: 60, max: 60)
      policy = ConnectRPC::Async::ConnectionPolicy.new(backoff: backoff)
      events = []
      transport = recording_transport(origin, events, connection_policy: policy, proxy: proxy.url)
      # The fail-fast caller joined to the attempt sees its reason; a call
      # during the backoff sees the transient failure without a new dial.
      first = capture_error { transport.call(request("#{origin}/x")) }
      expect(first.message).to eq("HTTP/2 transport failed: proxy authentication failed")
      expect(transport.connection_state).to eq(:transient_failure)
      second = capture_error { transport.call(request("#{origin}/x")) }
      expect([second.code, second.message]).to eq(["unavailable", "HTTP/2 connection is in transient failure"])
      expect([attempt_outcomes(events), state_changes(events), proxy.accepted])
        .to eq([[:other], %i[connecting transient_failure], 1])

      refused_events = []
      refused = recording_transport(origin, refused_events, connection_policy: policy,
                                                            proxy: "http://127.0.0.1:#{reserved.local_address.ip_port}")
      expect(capture_error { refused.call(request("#{origin}/x")) }.message)
        .to eq("HTTP/2 transport failed: connection refused")
      expect(attempt_outcomes(refused_events)).to eq([:refused])
    ensure
      transport&.close(grace: 0)
      refused&.close(grace: 0)
    end
  ensure
    proxy&.stop
    reserved&.close
  end

  %i[transport http1].each do |kind|
    it "aborts a CONNECT the proxy never answers at connect_timeout on #{kind}, closing the proxy socket and " \
       "freeing the slot" do
      proxy = ConnectProxy.new { |client, _head, peer| peer.wait_closed(client) }
      origin = "https://#{target}:8443"
      events = []
      options = { origin: origin, proxy: proxy.url, connect_timeout: 0.3 }
      if kind == :transport
        options.merge!(connection_policy: ConnectRPC::Async::ConnectionPolicy.new(backoff: nil),
                       connection_observer: ->(event) { events << event })
      else
        options[:connection_limit] = 1
      end
      errors = with_pooled(kind, -> { pooled(kind, **options) }) do |transport|
        2.times.map do
          error = capture_error { transport.call(request("#{origin}/x")) }
          # Each attempt's proxy connection closes before the next starts.
          pop_within(proxy.closed, "the client to close its pending tunnel")
          error
        end
      end
      expect(errors.map { |error| [error.code, error.message] })
        .to eq([["unavailable", "#{prefix(kind)} transport failed: timed out"]] * 2)
      expect(2.times.map { pop_within(proxy.heads, "a CONNECT head") }).to eq([connect_head("#{target}:8443")] * 2)
      expect(attempt_outcomes(events)).to eq(%i[timeout timeout]) if kind == :transport
    ensure
      proxy&.stop
    end
  end

  it "completes a surviving waiter through the shared HTTP/2 tunnel after another waiter cancels during CONNECT" do
    progress = Thread::Queue.new
    gate = Thread::Queue.new
    proxy = ConnectProxy.new do |client, head, relay|
      progress << true
      gate.pop
      relay.relay(client, relay.upstream(head))
    end
    peer = mixed_host
    origin = "http://#{target}:#{peer.peer.port}"
    reactor do |task|
      transport = ConnectRPC::Async::Transport.new(origin: origin, proxy: proxy.url)
      source = ConnectRPC::CancellationSource.new
      canceled = request("#{origin}/x")
      canceled.cancellation = source.token
      cancelling = task.async { capture_error { transport.call(canceled) } }
      survivor = task.async { mixed_client(transport, origin).lookup(item("survivor")).message.name }
      pop_within(progress, "the proxy to receive CONNECT")
      source.cancel
      expect(cancelling.wait.code).to eq("canceled")
      gate << true
      expect(survivor.wait).to eq("survivor")
      expect([proxy.accepted, peer.peer.accepts.size]).to eq([1, 1])
    ensure
      gate << true
      transport&.close(grace: 0)
    end
  ensure
    peer&.stop
    proxy&.stop
  end

  it "opens a fresh CONNECT on the next HTTP/2 call after the proxy drops an established tunnel" do
    proxy = ConnectProxy.new
    peer = mixed_host
    origin = "http://#{target}:#{peer.peer.port}"
    reactor do |task|
      transport = ConnectRPC::Async::Transport.new(origin: origin, proxy: proxy.url)
      typed = mixed_client(transport, origin)
      expect(typed.lookup(item("first")).message.name).to eq("first")
      proxy.drop
      wait_until(task, "the dropped tunnel to leave the pool") { transport.connection_state != :ready }
      expect(typed.lookup(item("second")).message.name).to eq("second")
      expect(2.times.map { pop_within(proxy.heads, "a CONNECT head") })
        .to eq([connect_head("#{target}:#{peer.peer.port}")] * 2)
      expect(proxy.accepted).to eq(2)
    ensure
      transport&.close(grace: 0)
    end
  ensure
    peer&.stop
    proxy&.stop
  end

  context "with a stale forwarded HTTP/1 keep-alive connection" do
    # The proxy reads the second request on its first connection, then closes
    # that connection without answering.
    def stale_proxy
      ForwardProxy.new { |_head, _body, count, connection| http1_ok unless connection == 1 && count == 2 }
    end

    it "resends a GET once with the same absolute target and Proxy-Authorization" do
      proxy = stale_proxy
      origin = "http://#{target}:8080"
      bodies = with_pooled(:http1, -> { pooled(:http1, origin: origin, proxy: proxy.url("user:secret")) }) do |h1|
        2.times.map { h1.call(request("#{origin}/get?message=x", method: "GET")).body }
      end
      expect(bodies).to eq(%w[ok ok])
      heads = 3.times.map { pop_within(proxy.heads, "a forwarded GET head") }
      expect(heads.map { |head| head.lines.first }).to eq(["GET http://#{target}:8080/get?message=x HTTP/1.1\r\n"] * 3)
      expect(heads.map { |head| field_values(head, "proxy-authorization") }).to eq([[basic("user:secret")]] * 3)
      expect(proxy.accepted).to eq(2)
    ensure
      proxy&.stop
    end

    it "never resends a POST" do
      proxy = stale_proxy
      origin = "http://#{target}:8080"
      error = with_pooled(:http1, -> { pooled(:http1, origin: origin, proxy: proxy.url) }) do |h1|
        expect(h1.call(request("#{origin}/post")).body).to eq("ok")
        capture_error { h1.call(request("#{origin}/post")) }
      end
      expect([error.code, error.message]).to eq(["unavailable", "HTTP/1 transport failed: connection closed"])
      expect([proxy.accepted, 2.times.map { pop_within(proxy.heads, "a forwarded POST head").lines.first }])
        .to eq([1, ["POST http://#{target}:8080/post HTTP/1.1\r\n"] * 2])
    ensure
      proxy&.stop
    end
  end

  {
    "a malformed head" => "HTTP/1.1 200 OK\r\nsecret field hunter2\r\n\r\n",
    "a malformed chunk" => "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nsecret-hunter2\r\n",
    # protocol-http indexes the fields and rejects a repeated singleton.
    "a duplicate singleton header" => "HTTP/1.1 200 OK\r\nServer: secret\r\nServer: hunter2\r\n" \
                                      "Content-Length: 0\r\n\r\n",
    # Content-Type may not be a trailer field.
    "an invalid trailer" => "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nok\r\n0\r\n" \
                            "Content-Type: secret-hunter2\r\n\r\n",
    # protocol-http converts the range with Integer, whose ArgumentError
    # quotes the value.
    "an unconvertible Range field" => "HTTP/1.1 200 OK\r\nRange: bytes=08-\r\nContent-Length: 0\r\n\r\n"
  }.each do |label, reply|
    it "replaces #{label} from a forward proxy with a fixed failure in the message, causes and lifecycle" do
      proxy = ForwardProxy.new { reply }
      origin = "http://#{target}:8080"
      recorded = []
      error = with_pooled(:http1, -> { pooled(:http1, origin: origin, proxy: proxy.url("user:hunter2")) }) do |h1|
        outgoing = request("#{origin}/x")
        outgoing.lifecycle = recording_lifecycle(recorded)
        capture_error { h1.call(outgoing) }
      end
      expect([error.code, error.message]).to eq(["unavailable", "HTTP/1 transport failed: malformed response"])
      expect(causes(error).map { |cause| [cause.class, cause.message] })
        .to eq([[AsyncHelpers::INTERNAL::ProxyFailure, "malformed response"]])
      expect(recorded).not_to be_empty
      texts = [error, *causes(error), *recorded].flat_map do |entry|
        [entry.message, entry.inspect, entry.detailed_message]
      end.join
      expect(texts).not_to include("secret", "hunter2")
    ensure
      proxy&.stop
    end
  end

  it "keeps a ThreadedTransport's proxy snapshot from its constructing thread across a replaced owner and fork" do
    proxy = ConnectProxy.new
    peer = mixed_host
    origin = "http://#{target}:#{peer.peer.port}"
    transport = with_env("HTTP_PROXY" => proxy.url) { threaded(origin: origin) }
    typed = mixed_client(transport, origin)
    ENV["HTTP_PROXY"] = "socks5://changed.invalid:1080"
    expect(value_within(Thread.new { typed.lookup(item("first")).message.name }, "a caller")).to eq("first")
    killed = engine_of(transport)
    killed.thread.kill
    expect([killed.thread.join(5), killed.state]).to eq([killed.thread, :stopped])
    expect(typed.lookup(item("replaced")).message.name).to eq("replaced")
    expect(engine_of(transport)).not_to equal(killed)
    connects = 2
    if Process.respond_to?(:fork)
      reader, writer = IO.pipe
      child = Process.fork do
        reader.close
        ENV["HTTP_PROXY"] = "socks5://child.invalid:1080"
        writer.write(begin
          typed.lookup(item("child")).message.name
        rescue Exception => e # rubocop:disable Lint/RescueException -- report every child failure to the parent
          "#{e.class}: #{e.message}"
        end)
        writer.close
        exit!(0)
      end
      writer.close
      status = Process.detach(child).join(20)&.value
      Process.kill("KILL", child) unless status
      expect([status&.success?, reader.read]).to eq([true, "child"])
      connects += 1
    end
    expect(connects.times.map { pop_within(proxy.heads, "a CONNECT head") })
      .to eq([connect_head("#{target}:#{peer.peer.port}")] * connects)
  ensure
    reader&.close
    transport&.close(grace: 0)
    peer&.stop
    proxy&.stop
  end

  it "rejects explicit proxy options without echoing them and shows only the proxy mode on every pooled transport" do
    options = {
      "socks5://user:secret@proxy.test:1080" => "Unsupported proxy scheme",
      "https://user:secret@proxy.test" => "Unsupported proxy scheme",
      "http://user:secret@proxy.test:99999" => "Invalid proxy configuration",
      :direct => "Invalid proxy configuration",
      42 => "Invalid proxy configuration"
    }
    %i[transport threaded http1].each do |kind|
      shown = with_pooled(kind, -> { pooled(kind, origin: "http://#{target}:8080", proxy: nil) }) do
        build = ->(**extra) { pooled(kind, origin: "http://#{target}:8080", **extra) }
        options.each do |option, message|
          error = capture_error { build.call(proxy: option) }
          expect([kind, option, error.class, error.message, error.cause])
            .to eq([kind, option, ArgumentError, message, nil])
        end
        built = [build.call(proxy: "http://user:secret@proxy.test:3128"), build.call(proxy: nil),
                 with_env("HTTPS_PROXY" => "http://user:secret@proxy.test:3128") { build.call }]
        built.map { |transport| transport.inspect.tap { transport.close(grace: 0) } }
      end
      expect(shown.map { |text| text[/ proxy=\w+>\z/] })
        .to eq([" proxy=explicit>", " proxy=direct>", " proxy=environment>"])
      expect(shown.join).not_to include("secret", "proxy.test")
    end
  end
end
