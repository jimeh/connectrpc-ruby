# frozen_string_literal: true

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

  it "rejects a wildcard over a single label" do
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
end
