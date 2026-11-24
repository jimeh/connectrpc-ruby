# frozen_string_literal: true

require "pathname"

RSpec.describe "TLS parity" do
  def authority(name = "parity CA") = credentials(name, authority: true)

  def leaf(issuer, name: "localhost", san: "DNS:localhost,IP:127.0.0.1", **)
    credentials(name, issuer: issuer, usage: "serverAuth", san: san, **)
  end

  # An HTTP/1.1 TLS server (ALPN http/1.1) answering Connect unary calls.
  def tls_server(certificate, key, chain: [], client_ca: nil)
    context = OpenSSL::SSL::SSLContext.new
    context.min_version = OpenSSL::SSL::TLS1_2_VERSION
    context.add_certificate(certificate, key, chain)
    context.alpn_select_cb = ->(protocols) { protocols.include?("http/1.1") ? "http/1.1" : nil }
    if client_ca
      store = OpenSSL::X509::Store.new
      store.add_cert(client_ca)
      context.cert_store = store
      context.verify_mode = OpenSSL::SSL::VERIFY_PEER | OpenSSL::SSL::VERIFY_FAIL_IF_NO_PEER_CERT
    end
    server = TinyHTTP.new(tls: context)
    (@servers ||= []) << server
    server
  end

  after { @servers&.each(&:close) }

  def unary(origin, transport) = client(origin, transport: transport, protocol: :connect).unary(MSG.new, timeout: 5)

  def ca_file(*certificates)
    path = File.join(Dir.mktmpdir("connectrpc-hyper-ca-"), "ca.pem")
    File.write(path, certificates.map(&:to_pem).join)
    path
  end

  def failure(origin, transport) = error_of { unary(origin, transport) }&.then { |error| [error.code, error.message] }

  it "appends ca_file to the native roots and lets SSL_CERT_FILE replace the platform store" do
    first = authority("native CA")
    second = authority("appended CA")
    native = tls_server(*leaf(first))
    appended = tls_server(*leaf(second))
    output, status = ruby_child(<<~RUBY, env: { "SSL_CERT_FILE" => ca_file(first.first), "SSL_CERT_DIR" => nil })
      plain = ConnectRPC::Hyper::Transport.new
      both = ConnectRPC::Hyper::Transport.new(ca_file: #{ca_file(second.first).dump})
      outcome = lambda do |origin, transport|
        client(origin, transport, protocol: :connect).unary(MSG.new, timeout: 5)
        "ok"
      rescue ConnectRPC::Error => e
        e.message
      end
      p [outcome.call(#{native.origin.dump}, plain), outcome.call(#{appended.origin.dump}, plain),
         outcome.call(#{native.origin.dump}, both), outcome.call(#{appended.origin.dump}, both)]
    RUBY
    expect(status).to be_success
    expect(eval(output.lines.last)).to eq( # rubocop:disable Security/Eval
      ["ok", "HTTP transport failed: TLS verification failed", "ok", "ok"]
    )
  end

  it "presents a client identity for mTLS and reports a missing one as a TLS error" do
    root = authority
    client_ca = authority("client CA")
    identity = credentials("client", issuer: client_ca, usage: "clientAuth")
    server = tls_server(*leaf(root), client_ca: client_ca.first)
    with_identity = hyper(ca_file: ca_file(root.first), certificate_chain: [identity.first.to_pem],
                          private_key: identity.last.private_to_pem)
    expect(unary(server.origin, with_identity).message).to eq(MSG.new)
    expect(failure(server.origin, hyper(ca_file: ca_file(root.first)))).to eq(["unavailable",
                                                                               "HTTP transport failed: TLS error"])
  end

  # A TLS 1.3 server rejects a missing client identity after the client's
  # handshake completes. Each attempt parks before handing hyper its request
  # until the server's alert has ended its connection (the socket dropped), so
  # the first send and the one retry both meet a closed connection; the call
  # still reports the alert the connection read.
  it "reports a missing client identity as a TLS error when the alert closes an idle connection" do
    root = authority
    server = tls_server(*leaf(root), client_ca: authority("client CA").first)
    transport = hyper(ca_file: ca_file(root.first), http2: false)
    native_driver = driver(transport)
    native_driver.hold("request:send", 10)
    native_driver.hold("socket:drop", 0)
    call = Thread.new { failure(server.origin, transport) }
    held = -> { %w[request:send socket:drop].map { |point| native_driver.reached(point) } }
    wait_until("the first connection closed by the alert") { held.call == [1, 1] }
    native_driver.hold("retry:unsent", 10)
    native_driver.release("request:send")
    wait_until("the request handed back unsent") { native_driver.reached("retry:unsent") == 1 }
    # Re-arming replaces the point, so its count starts again from zero.
    native_driver.hold("request:send", 10)
    native_driver.release("retry:unsent")
    wait_until("the retry's connection closed by the alert") { held.call == [1, 2] }
    native_driver.release("request:send")
    expect(call.value).to eq(["unavailable", "HTTP transport failed: TLS error"])
  ensure
    %w[request:send socket:drop retry:unsent].each { |point| native_driver&.release(point) }
  end

  # h2 keeps only an I/O error's kind and text, so the alert must not surface
  # as a malformed response. No hold forces the order here: h2 may write after
  # the server's reset and end on that write error before it reads the alert,
  # and that call reports a lost connection, as grpc-go reports a write error.
  # Each call dials its own connection; about four in five read the alert.
  { "connect_mtls" => :connect, "grpc_mtls" => :grpc }.each do |origin, protocol|
    it "reports a missing client identity over HTTP/2 to #{origin} as a TLS error, never a malformed response" do
      closed = ["unavailable", "HTTP transport failed: connection closed"]
      rejected = ["unavailable", "HTTP transport failed: TLS error"]
      transport = hyper(ca_file: File.join(peer.certs, "ca.pem"), connections: :per_call)
      peer_client = client(peer[origin], transport: transport, protocol: protocol)
      outcomes = Array.new(10) do
        error_of { peer_client.unary(MSG.new, timeout: 5) }&.then { |error| [error.code, error.message] }
      end
      expect(outcomes - [closed]).to all(eq(rejected))
    end
  end

  # request:strand keeps each request away from hyper, as tokio's channel can
  # leave a request sent while the connection's dispatcher was being dropped
  # (#208): never received, never answered. Each attempt's connection then
  # ends with the server's rejection, and its dispatcher's end tells the call.
  # A Connect unary call counts that as unsent and retries once.
  describe "stranded requests" do
    let(:closed) { ["unavailable", "HTTP transport failed: connection closed"] }
    let(:rejected) { ["unavailable", "HTTP transport failed: TLS error"] }

    after { %w[request:strand retry:unsent dispatcher].each { |point| @native_driver&.release(point) } }

    def mtls_transport(**)
      transport = hyper(ca_file: File.join(peer.certs, "ca.pem"), **)
      @native_driver = driver(transport)
      transport
    end

    def outcome(origin, transport, protocol)
      error_of { client(peer[origin], transport: transport, protocol: protocol).unary(MSG.new, timeout: 5) }
        &.then { |error| [error.code, error.message] }
    end

    def counts = %w[request:strand retry:unsent].map { |point| @native_driver.reached(point) }

    it "fails a stranded Connect unary call over HTTP/1.1 with the server's alert after one retry" do
      root = authority
      server = tls_server(*leaf(root), client_ca: authority("client CA").first)
      transport = hyper(ca_file: ca_file(root.first), http2: false)
      @native_driver = driver(transport)
      %w[request:strand retry:unsent].each { |point| @native_driver.hold(point, 0) }
      expect(failure(server.origin, transport)).to eq(rejected)
      expect(counts).to eq([2, 2])
    end

    it "fails a stranded Connect unary call over per-call HTTP/2 after one retry" do
      transport = mtls_transport(connections: :per_call)
      %w[request:strand retry:unsent].each { |point| @native_driver.hold(point, 0) }
      expect(outcome("connect_mtls", transport, :connect)).to eq(closed).or eq(rejected)
      expect(counts).to eq([2, 2])
    end

    # The dispatcher hold keeps each pooled connection admitting until its
    # request is stranded; otherwise the rejection could end it first and the
    # pool dial's waiter would fail before sending (below).
    it "fails a stranded Connect unary call over pooled HTTP/2 after one retry" do
      transport = mtls_transport
      @native_driver.hold("request:strand", 0)
      %w[dispatcher retry:unsent].each { |point| @native_driver.hold(point, 10) }
      call = Thread.new { outcome("connect_mtls", transport, :connect) }
      wait_until("the first request stranded") { @native_driver.reached("request:strand") == 1 }
      @native_driver.release("dispatcher")
      wait_until("the stranded request counted as unsent") { @native_driver.reached("retry:unsent") == 1 }
      @native_driver.hold("dispatcher", 10)
      @native_driver.release("retry:unsent")
      wait_until("the retry stranded") { @native_driver.reached("request:strand") == 2 }
      @native_driver.release("dispatcher")
      expect(call.value).to eq(closed).or eq(rejected)
      expect(@native_driver.reached("request:strand")).to eq(2)
    end

    it "fails a stranded gRPC call with its connection's failure, without a retry" do
      transport = mtls_transport(connections: :per_call)
      @native_driver.hold("request:strand", 0)
      expect(outcome("grpc_mtls", transport, :grpc)).to eq(closed).or eq(rejected)
      expect(@native_driver.reached("request:strand")).to eq(1)
    end

    # The kept body owns the upload channel's receiver, as a stranded
    # envelope's body stays alive while any sender of its pooled connection
    # does, so nothing ever drains the write.
    it "returns a writer blocked on a stranded stream and fails the stream at its head" do
      transport = mtls_transport
      @native_driver.hold("request:strand", 0)
      @native_driver.hold("dispatcher", 10)
      request = ConnectRPC::StreamRequest.new(
        url: URI("#{peer['grpc_mtls']}/raw.v1.Raw/Upload"), http_method: "POST",
        headers: { "content-type" => ["application/grpc"], "te" => ["trailers"] }, deadline: ConnectRPC::Deadline.new(5)
      )
      wrote = Queue.new
      call = Thread.new do
        error_of do
          transport.open_grpc_stream(request) do |exchange|
            exchange.write([0, 0].pack("CN"))
            wrote << :returned
            exchange.response_head
          end
        end
      end
      wait_until("the request stranded") { @native_driver.reached("request:strand") == 1 }
      wait_parked(call, "Exchange#write")
      @native_driver.release("dispatcher")
      expect(wrote.pop(timeout: 10)).to eq(:returned)
      expect(call.value&.then { |error| [error.code, error.message] }).to eq(closed).or eq(rejected)
    end
  end

  # The pool dial's connection is pooled, then parks at dial:pooled until the
  # server's rejection has ended it (its dispatcher closed, so it no longer
  # admits; its socket drops in h2's own task, which may come first), before
  # the waiter checks it out. The rejection came where the server's SETTINGS would have
  # (wire:settings never counts one), so the waiter fails as that connection
  # did instead of dialing again: a server that ends every connection after
  # its handshake would otherwise keep a pooled caller dialing until its
  # deadline (#208). pool_spec.rb covers a connection that closes after the
  # server's SETTINGS, which redials.
  it "fails a pool dial's waiter with its connection's failure when that connection closed before checkout" do
    transport = hyper(ca_file: File.join(peer.certs, "ca.pem"))
    native_driver = driver(transport)
    native_driver.hold("dial:pooled", 10)
    %w[dispatcher:closed dial:started wire:settings].each { |point| native_driver.hold(point, 0) }
    call = Thread.new { failure(peer["connect_mtls"], transport) }
    wait_until("the pooled connection closed") { native_driver.reached("dispatcher:closed") == 1 }
    native_driver.release("dial:pooled")
    expect(call.value).to eq(["unavailable", "HTTP transport failed: connection closed"])
      .or eq(["unavailable", "HTTP transport failed: TLS error"])
    expect(%w[dial:started wire:settings].map { |point| native_driver.reached(point) }).to eq([1, 0])
  ensure
    %w[dial:pooled dispatcher:closed dial:started wire:settings].each { |point| native_driver&.release(point) }
  end

  it "accepts a leaf-first chain with an intermediate" do
    root = authority
    intermediate = credentials("intermediate CA", issuer: root, authority: true)
    identity = credentials("client", issuer: intermediate, usage: "clientAuth")
    server = tls_server(*leaf(root), client_ca: root.first)
    chain = [identity.first.to_pem, intermediate.first.to_pem]
    transport = hyper(ca_file: ca_file(root.first), certificate_chain: chain, private_key: identity.last.private_to_pem)
    expect(unary(server.origin, transport).message).to eq(MSG.new)
  end

  it "verifies names: IP SANs, DNS SANs and mismatches" do
    root = authority
    path = ca_file(root.first)
    ip_only = tls_server(*leaf(root, san: "IP:127.0.0.1"))
    dns_only = tls_server(*leaf(root, san: "DNS:localhost"))
    transport = hyper(ca_file: path)
    expect(unary(ip_only.origin, transport).message).to eq(MSG.new)
    expect(unary("https://localhost:#{dns_only.port}", transport).message).to eq(MSG.new)
    expect(failure(dns_only.origin, transport)).to eq(["unavailable", "HTTP transport failed: TLS verification failed"])
  end

  it "pins rustls rejections OpenSSL would accept: CN-only, X.509 v1 and a CA certificate as the leaf" do
    root = authority
    path = ca_file(root.first)
    cn_only = tls_server(*leaf(root, san: nil))
    version1 = tls_server(*credentials("localhost", issuer: root, version: 0))
    ca_leaf_key = OpenSSL::PKey::EC.generate("prime256v1")
    ca_leaf = credentials("localhost", authority: true, san: "DNS:localhost,IP:127.0.0.1", key: ca_leaf_key)
    as_leaf = tls_server(*ca_leaf)
    expect(failure(cn_only.origin, hyper(ca_file: path)))
      .to eq(["unavailable", "HTTP transport failed: TLS verification failed"])
    expect(failure(version1.origin, hyper(ca_file: path)))
      .to eq(["unavailable", "HTTP transport failed: TLS verification failed"])
    expect(failure(as_leaf.origin, hyper(ca_file: ca_file(ca_leaf.first))))
      .to eq(["unavailable", "HTTP transport failed: TLS verification failed"])
  end

  it "rejects a wildcard over a single label", :dns_shim do
    environment = stall_environment
    root = authority
    server = tls_server(*leaf(root, san: "DNS:*.test"))
    output, status = ruby_child(<<~RUBY, env: environment)
      transport = ConnectRPC::Hyper::Transport.new(ca_file: #{ca_file(root.first).dump})
      begin
        client("https://peer.test:#{server.port}", transport, protocol: :connect).unary(MSG.new, timeout: 5)
        puts "ok"
      rescue ConnectRPC::Error => e
        puts e.message
      end
    RUBY
    expect(status).to be_success
    expect(output.lines.last.chomp).to eq("HTTP transport failed: TLS verification failed")
  end

  describe "client configuration errors" do
    let(:root) { authority }
    let(:identity) { credentials("client", issuer: root, usage: "clientAuth") }

    def invalid(**)
      described = ConnectRPC::Hyper::Transport
      error = nil
      begin
        described.new(**)
      rescue ArgumentError => e
        error = e
      end
      [error&.message, error&.cause]
    end

    it "names only the input, with no cause" do
      expired = credentials("client", issuer: root, usage: "clientAuth", not_after: Time.now - 1)
      encrypted = identity.last.private_to_pem(OpenSSL::Cipher.new("aes-128-cbc"), "secret")
      other_key = OpenSSL::PKey::EC.generate("prime256v1").private_to_pem
      missing = File.join(Dir.mktmpdir, "missing.pem")
      empty = ca_file
      cases = {
        { ca_file: missing } => "ca_file", { ca_file: empty } => "ca_file", { ca_file: 42 } => "ca_file",
        { certificate_chain: identity.first.to_pem, private_key: identity.last.private_to_pem } => "certificate_chain",
        { certificate_chain: [], private_key: identity.last.private_to_pem } => "certificate_chain",
        { certificate_chain: [expired.first.to_pem], private_key: expired.last.private_to_pem } => "certificate_chain",
        { certificate_chain: [identity.first.to_pem], private_key: encrypted } => "private_key",
        { certificate_chain: [identity.first.to_pem], private_key: other_key } => "private_key",
        { certificate_chain: [identity.first.to_pem], private_key: nil } => "private_key",
        { certificate_chain: [identity.first.to_pem, authority("unrelated").first.to_pem],
          private_key: identity.last.private_to_pem } => "certificate_chain"
      }
      cases.each do |options, input|
        message, cause = invalid(**options)
        expect([message, cause]).to eq(["Invalid TLS client configuration: #{input}", nil]), options.keys.inspect
        expect(message).not_to include(Dir.tmpdir)
      end
    end

    it "rejects an identity OpenSSL accepts but aws-lc-rs cannot sign with (Ed448)" do
      ed448 = OpenSSL::PKey.generate_key("ED448")
      certificate = credentials("client", issuer: root, usage: "clientAuth", key: ed448)
      expect(invalid(certificate_chain: [certificate.first.to_pem], private_key: ed448.private_to_pem))
        .to eq(["Invalid TLS client configuration: private_key", nil])
    end

    it "raises when TLS options meet an http:// request" do
      transport = hyper(ca_file: ca_file(root.first))
      expect { client(peer["grpc_h2c"], transport: transport).unary(MSG.new, timeout: 5) }
        .to raise_error(ArgumentError, "TLS options require an HTTPS origin")
    end
  end

  # NetHTTP's ClientTLS is private, so both transports are constructed with the
  # same options. rustls-only rejections (Ed448 above) are documented apart.
  describe "parity with core NetHTTP" do
    let(:root) { authority }
    let(:identity) { credentials("client", issuer: root, usage: "clientAuth") }

    before { require "connectrpc/transports/net_http" }

    def outcome(transport, **)
      transport.new(**)
      :ok
    rescue StandardError => e
      [e.class, e.message, e.cause]
    end

    def file(name, data)
      path = File.join(@directory ||= Dir.mktmpdir("connectrpc-hyper-parity-"), name)
      File.binwrite(path, data)
      path
    end

    # A TRUSTED CERTIFICATE block: the certificate followed by X509_CERT_AUX
    # trust settings, as `openssl x509 -trustout` writes it.
    def trusted(certificate)
      trust = OpenSSL::ASN1::Sequence([OpenSSL::ASN1::Sequence([OpenSSL::ASN1::ObjectId("serverAuth")])])
      body = [certificate.to_der + trust.to_der].pack("m").scan(/.{1,64}/).join("\n")
      "-----BEGIN TRUSTED CERTIFICATE-----\n#{body}\n-----END TRUSTED CERTIFICATE-----\n"
    end

    def crl(certificate, key)
      list = OpenSSL::X509::CRL.new
      list.version = 1
      list.issuer = certificate.subject
      list.last_update = Time.now
      list.next_update = Time.now + 60
      list.sign(key, "SHA256")
      list.to_pem
    end

    it "raises the same errors for the same inputs" do
      pem = root.first.to_pem
      chain = [identity.first.to_pem]
      key = identity.last.private_to_pem
      expired = credentials("client", issuer: root, usage: "clientAuth", not_after: Time.now - 1)
      encrypted = identity.last.private_to_pem(OpenSSL::Cipher.new("aes-128-cbc"), "secret")
      cases = [
        { ca_file: file("ca.pem", pem) }, { ca_file: file("ca.der", root.first.to_der) },
        { ca_file: file("crl.pem", crl(*root)) }, { ca_file: file("trusted.pem", trusted(root.first)) },
        { ca_file: file("mixed.pem", "junk\n#{pem}#{key}#{crl(*root)}") }, { ca_file: file("key.pem", key) },
        { ca_file: file("empty.pem", "") }, { ca_file: file("corrupt.pem", pem.sub(/[A-Za-z]{10}/, "!" * 10)) },
        { ca_file: Pathname(file("path.pem", pem)) }, { ca_file: File.join(Dir.tmpdir, "missing-ca.pem") },
        { ca_file: Dir.tmpdir }, { ca_file: 42 }, { ca_file: false },
        { certificate_chain: chain, private_key: key }, { certificate_chain: chain + [pem], private_key: key },
        { certificate_chain: chain.first, private_key: key }, { certificate_chain: [], private_key: key },
        { certificate_chain: [42], private_key: key },
        { certificate_chain: ["#{chain.first}#{pem}"], private_key: key },
        { certificate_chain: [expired.first.to_pem], private_key: expired.last.private_to_pem },
        { certificate_chain: chain, private_key: encrypted }, { certificate_chain: chain, private_key: nil },
        { certificate_chain: nil, private_key: key }, { certificate_chain: false, private_key: false },
        { certificate_chain: chain, private_key: OpenSSL::PKey::EC.generate("prime256v1").private_to_pem },
        { certificate_chain: chain, private_key: identity.last.to_der },
        { certificate_chain: chain + [authority("unrelated").first.to_pem], private_key: key },
        { ca_file: 42, certificate_chain: [], private_key: nil }
      ]
      outcomes = cases.map do |options|
        expected = outcome(ConnectRPC::Transports::NetHTTP, **options)
        expect(outcome(ConnectRPC::Hyper::Transport, **options)).to eq(expected), options.inspect
        expected
      end
      # The table covers both acceptance and every named input.
      expect(outcomes.map { |result| result == :ok ? :ok : result[1] }.uniq).to contain_exactly(
        :ok, "Invalid TLS client configuration: ca_file", "Invalid TLS client configuration: certificate_chain",
        "Invalid TLS client configuration: private_key"
      )
    end

    it "rejects TLS options on an http:// request with NetHTTP's error" do
      request = ConnectRPC::TransportRequest.new(url: URI("http://127.0.0.1:1/svc/Method"), http_method: "POST",
                                                 headers: {}, body: "", response_limit: 1024)
      errors = [ConnectRPC::Transports::NetHTTP, ConnectRPC::Hyper::Transport].map do |transport|
        transport.new(ca_file: false).call(request)
      rescue ArgumentError => e
        [e.class, e.message]
      end
      expect(errors).to eq([[ArgumentError, "TLS options require an HTTPS origin"]] * 2)
    end
  end
end
