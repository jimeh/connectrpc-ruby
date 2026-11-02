# frozen_string_literal: true

RSpec.describe "HTTP/2 TLS snapshots and authentication" do
  it "rejects non-PEM, bundled and invalid certificates and unsafe keys without retaining secret text" do
    tls = AsyncHelpers::INTERNAL::TLS
    options = host_tls
    leaf = tls_material.fetch(:leaf)
    encrypted = leaf.last.private_to_pem(OpenSSL::Cipher.new("aes-256-cbc"), "fixture-password")
    legacy = leaf.last.export(OpenSSL::Cipher.new("aes-256-cbc"), "fixture-password")
    public_der_as_private = ["-----BEGIN PRIVATE KEY-----", [leaf.last.public_to_der].pack("m"),
                             "-----END PRIVATE KEY-----"].join("\n")
    [leaf.last.private_to_der, leaf.last.public_to_pem, encrypted, legacy, public_der_as_private,
     tls_material.fetch(:identity).last.private_to_pem, "secret-not-pem"].each do |key|
      expect { tls.server(**options, private_key: key) }.to raise_error(ArgumentError, "Invalid TLS snapshot")
    end
    [[leaf.first.to_der], [leaf.first.to_pem * 2], ["invalid"], [],
     [leaf.first.to_pem, tls_material.fetch(:client_ca).first.to_pem]].each do |chain|
      expect do
        tls.server(**options, certificate_chain: chain)
      end.to raise_error(ArgumentError, "Invalid TLS snapshot")
    end
    expired = OpenSSL::X509::Certificate.new(leaf.first.to_der)
    expired.not_after = Time.now - 1
    expired.sign(tls_material.fetch(:root).last, "SHA256")
    expect { tls.server(**options, certificate_chain: [expired.to_pem]) }.to raise_error(ArgumentError)
    expect { tls.server(**options, client_ca: []) }.to raise_error(ArgumentError)
    expect { tls.server(**options.except(:client_ca)) }.to raise_error(ArgumentError)
    expect do
      tls.identity(options.fetch(:certificate_chain),
                   tls_material.fetch(:identity).last.private_to_pem)
    end.to raise_error(ArgumentError)
    snapshot = tls.server(**options)
    expect(snapshot.inspect).not_to include("BEGIN", leaf.first.subject.to_s, "OpenSSL")
    expect(snapshot.instance_variables).to eq(%i[@context @authenticated @fingerprint @expires_at])
  end

  it "proves private-key possession for Ed25519 and rejects public-only and mismatched keys" do
    key = OpenSSL::PKey.generate_key("ED25519")
    leaf = credentials(301, "ed25519", issuer: tls_material.fetch(:root), key: key, usage: "serverAuth")
    options = host_tls(leaf)
    expect(AsyncHelpers::INTERNAL::TLS.server(**options)).to be_frozen
    other_key = OpenSSL::PKey.generate_key("ED25519")
    expect { AsyncHelpers::INTERNAL::TLS.identity([leaf.first.to_pem], other_key.private_to_pem) }
      .to raise_error(ArgumentError)
    [key.public_to_pem, tls_material.fetch(:leaf).last.private_to_pem].each do |candidate|
      expect do
        AsyncHelpers::INTERNAL::TLS.server(**options, private_key: candidate)
      end.to raise_error(ArgumentError)
    end
  end

  def ssl_client_context(ca_file, identity: nil, chain: [], offers: ["h2"], version: nil)
    context = OpenSSL::SSL::SSLContext.new
    context.set_params(verify_mode: OpenSSL::SSL::VERIFY_PEER, verify_hostname: true)
    store = OpenSSL::X509::Store.new
    store.add_file(ca_file)
    context.cert_store = store
    context.alpn_protocols = offers
    context.min_version = version || OpenSSL::SSL::TLS1_2_VERSION
    context.max_version = version if version
    context.add_certificate(identity.first, identity.last, chain.map(&:first)) if identity
    context
  end

  def tls_peer(host, context, session: nil)
    raw = IO::Endpoint.tcp("127.0.0.1", host.port).connect
    ssl = OpenSSL::SSL::SSLSocket.new(raw, context)
    ssl.sync_close = true
    ssl.hostname = "localhost"
    ssl.session = session if session
    ssl.connect
    yield ssl, raw
  ensure
    AsyncHelpers::INTERNAL.raw_close(raw)
    ssl&.close
  end

  it "serves exactly the explicit chain, including an empty intermediates array with the issuer in client_ca" do
    reactor do
      root = tls_material.fetch(:root)
      intermediate = credentials(10, "intermediate", issuer: root, authority: true)
      leaf = credentials(11, "localhost", issuer: intermediate, usage: "serverAuth")
      identity = credentials(12, "client", issuer: intermediate, usage: "clientAuth")
      with_ca(root) do |ca_file|
        host = ConnectRPC::Async::Host.new(server: server, tls: host_tls(leaf, chain: [intermediate])).start
        context = ssl_client_context(ca_file)
        tls_peer(host, context) { |ssl, _| expect(ssl.peer_cert_chain.map(&:serial).map(&:to_i)).to eq([11, 10]) }
        host.close(grace: 0)
        # Verification is deliberately disabled only in this wire-chain observer.
        # A normal verified connection cannot accept an omitted intermediate.
        context = ssl_client_context(ca_file, identity: identity, chain: [intermediate])
        context.verify_mode = OpenSSL::SSL::VERIFY_NONE
        host = ConnectRPC::Async::Host.new(server: server, tls: host_tls(leaf, anchors: [intermediate])).start
        tls_peer(host, context) { |ssl, _| expect(ssl.peer_cert_chain.map(&:serial).map(&:to_i)).to eq([11]) }
      ensure
        host&.close(grace: 0)
      end
    end
  end

  it "requires verified clients using only the explicit scoped intermediate and rejects a sibling issuer" do
    reactor do |task|
      root = credentials(20, "client root", authority: true)
      intermediate = credentials(21, "allowed intermediate", issuer: root, authority: true)
      sibling = credentials(22, "sibling", issuer: root, authority: true)
      allowed = credentials(23, "allowed client", issuer: intermediate, usage: "clientAuth")
      denied = credentials(24, "denied client", issuer: sibling, usage: "clientAuth")
      seen = []
      implementation = server(echo: lambda { |message, _|
        seen << true
        message
      })
      host = TLSObservedHost.new(server: implementation, tls: host_tls(anchors: [intermediate])).start
      with_ca do |ca_file|
        origin = "https://localhost:#{host.port}"
        accepted = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca_file,
                                                    **host_tls(allowed, chain: [intermediate]).except(:client_ca))
        expect(client(accepted, origin).echo(AsyncHelpers::MESSAGE.new).message).to eq(AsyncHelpers::MESSAGE.new)
        accepted.close
        [nil, denied].each do |identity|
          options = identity ? host_tls(identity, chain: [sibling]).except(:client_ca) : {}
          rejected = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca_file, **options)
          expect { client(rejected, origin).echo(AsyncHelpers::MESSAGE.new) }.to raise_error(ConnectRPC::Error) { |e|
            expect(e.code).to eq("unavailable")
          }
          rejected.close(grace: 0)
        end
        task.yield
        expect(seen.size).to eq(1)
        expect(host.setup_errors.size).to eq(2)
        expect(host.setup_errors.join).to match(/certificate verify failed|peer did not return a certificate/)
      end
    ensure
      host&.close(grace: 0)
    end
  end

  it "scopes detached authenticated client leaf identity to the exact host handler task on every exit" do
    reactor do |_task|
      observed = []
      host = nil
      other = ConnectRPC::Async::Host.new(server: server)
      handler = lambda do |message, _context|
        certificate = host.peer_certificate
        observed << [certificate.serial.to_i, other.peer_certificate, Async::Task.current.async do
          host.peer_certificate
        end.wait]
        certificate.serial = 999
        observed << host.peer_certificate.serial.to_i
        raise "private failure" if message.name == "fail"

        message
      end
      host = ConnectRPC::Async::Host.new(server: server(echo: handler),
                                         tls: host_tls(anchors: [tls_material.fetch(:client_ca)])).start
      with_ca do |ca_file|
        origin = "https://127.0.0.1:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca_file,
                                                     **host_tls(tls_material.fetch(:identity)).except(:client_ca))
        typed = client(transport, origin)
        expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "ok")).message.name).to eq("ok")
        expect { typed.echo(AsyncHelpers::MESSAGE.new(name: "fail")) }.to raise_error(ConnectRPC::Error) { |e|
          expect(e.code).to eq("internal")
          expect(e.message).not_to include("private failure")
        }
        expect(observed).to eq([[201, nil, nil], 201, [201, nil, nil], 201])
        expect(host.peer_certificate).to be_nil
        expect(host_session(host).instance_variable_get(:@identities)).to be_empty
      ensure
        transport&.close
      end
    ensure
      other&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  it "rejects absent and HTTP/1-only client ALPN before any core handler" do
    reactor do |task|
      seen = []
      host = ConnectRPC::Async::Host.new(server: server(echo: lambda { |message, _|
        seen << true
        message
      }), tls: host_tls).start
      with_ca do |ca_file|
        [[], ["http/1.1"]].each do |offers|
          tls_peer(host, ssl_client_context(ca_file, offers: offers)) do |ssl, _|
            ssl.write("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            expect(ssl.read(1)).to be_nil
          end
        rescue OpenSSL::SSL::SSLError, Errno::ECONNRESET
          # Server alert or EOF both prove rejection, with no handler below.
        end
        task.yield
        expect(seen).to be_empty
      end
    ensure
      host&.close(grace: 0)
    end
  end

  it "rotates full snapshots atomically on the same listener and preserves established inflight calls" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      handler = lambda do |message, _|
        if message.name == "hold"
          entered.enqueue(true)
          release.dequeue
        end
        message
      end
      host = ConnectRPC::Async::Host.new(server: server(echo: handler),
                                         tls: host_tls(anchors: [tls_material.fetch(:client_ca)])).start
      original_listener = host.instance_variable_get(:@bound).sockets.first
      with_ca do |ca_file|
        origin = "https://localhost:#{host.port}"
        options = { origin: origin, ca_file: ca_file, **host_tls(tls_material.fetch(:identity)).except(:client_ca) }
        old = ConnectRPC::Async::Transport.new(**options)
        inflight = task.async { client(old, origin).echo(AsyncHelpers::MESSAGE.new(name: "hold")) }
        entered.dequeue
        previous = host_session(host).instance_variable_get(:@tls).instance_variable_get(:@snapshot)
        expect(host.reload_tls(**host_tls(anchors: [tls_material.fetch(:client_ca)]))).to eq(2)
        expect(host_session(host).instance_variable_get(:@tls).instance_variable_get(:@snapshot)).not_to equal(previous)
        snapshot = host_session(host).instance_variable_get(:@tls).instance_variable_get(:@snapshot)
        fresh_context = snapshot.instance_variable_get(:@context)
        expect(fresh_context).not_to equal(previous.instance_variable_get(:@context))
        next_leaf = credentials(102, "renewal", issuer: tls_material.fetch(:root), usage: "serverAuth")
        expect(host.reload_tls(**host_tls(next_leaf, anchors: [tls_material.fetch(:client_ca)]))).to eq(3)
        fresh = ConnectRPC::Async::Transport.new(**options)
        expect(client(fresh, origin).echo(AsyncHelpers::MESSAGE.new(name: "new")).message.name).to eq("new")
        expect(fresh.instance_variable_get(:@resources).first.socket.peer_cert.serial.to_i).to eq(102)
        expect(old.instance_variable_get(:@resources).first.socket.peer_cert.serial.to_i).to eq(101)
        expect { host.reload_tls(**host_tls(next_leaf), private_key: "bad") }.to raise_error(ArgumentError)
        mismatched = host_tls(next_leaf, anchors: [tls_material.fetch(:client_ca)])
        expect { host.reload_tls(**mismatched, private_key: tls_material.fetch(:identity).last.private_to_pem) }
          .to raise_error(ArgumentError)
        expect(host.tls_generation).to eq(3)
        expect(host.instance_variable_get(:@bound).sockets.first).to equal(original_listener)
        release.enqueue(true)
        expect(inflight.wait.message.name).to eq("hold")
      ensure
        old&.close
        fresh&.close
      end
    ensure
      host&.close(grace: 0)
    end
  end
end
