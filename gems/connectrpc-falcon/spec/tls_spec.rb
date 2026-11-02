# frozen_string_literal: true

RSpec.describe "Falcon TLS raw-accept generation" do
  def context(identity: nil, chain: [], offers: ["h2"], version: nil)
    value = OpenSSL::SSL::SSLContext.new
    value.set_params(verify_mode: OpenSSL::SSL::VERIFY_PEER, verify_hostname: true)
    store = OpenSSL::X509::Store.new
    store.add_cert(tls_material[:root].first)
    value.cert_store = store
    value.alpn_protocols = offers if offers
    value.min_version = version || OpenSSL::SSL::TLS1_2_VERSION
    value.max_version = version if version
    value.add_certificate(identity.first, identity.last, chain.map(&:first)) if identity
    value
  end

  def peer(origin, context, sni: true, raw: nil)
    uri = URI(origin)
    raw ||= IO::Endpoint.tcp(uri.hostname, uri.port).connect
    ssl = OpenSSL::SSL::SSLSocket.new(raw, context)
    ssl.sync_close = true
    ssl.hostname = uri.hostname if sni
    ssl.connect
    ssl.post_connection_check(uri.hostname)
    yield ssl
  ensure
    AsyncHelpers::INTERNAL.raw_close(raw)
    ssl&.close
  end

  it "rotates the same listener for SNI/no-SNI/IP while retaining a captured pending handshake and persistent peer" do
    reactor do |task|
      calls = 0
      candidate = host_tls.merge(version: "one")
      provider = lambda do
        calls += 1
        candidate
      end
      rack = ->(_env) { [200, {}, ["ordinary"]] }
      falcon(scheme: "https", tls_provider: provider, rack_app: rack) do |host, origin|
        old = ConnectRPC::Async::Transport.new(origin: origin, ca_file: nil)
        # The normal transport verifies via an explicit CA file.
        with_ca do |ca|
          old.close(grace: 0)
          old = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca)
          expect(client(old, origin).echo(AsyncHelpers::MESSAGE.new(name: "old")).message.name).to eq("old")
          pending = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
          session = host.instance_variable_get(:@session)
          wait_until(task, "raw pending accept") { session.instance_variable_get(:@resources).size == 2 }
          candidate = host_tls(credentials(102, "localhost", issuer: tls_material[:root], usage: "serverAuth"))
                      .merge(version: "two")
          expect(host.refresh_tls).to eq(2)
          peer(origin, context, raw: pending, sni: false) { |ssl| expect(ssl.peer_cert.serial.to_i).to eq(101) }
          [true, false].each do |sni|
            peer(origin, context, sni: sni) { |ssl| expect(ssl.peer_cert.serial.to_i).to eq(102) }
            peer(origin.sub("127.0.0.1", "localhost"), context, sni: sni) do |ssl|
              expect(ssl.peer_cert.serial.to_i).to eq(102)
            end
          end
          expect(client(old,
                        origin).echo(AsyncHelpers::MESSAGE.new(name: "persistent")).message.name).to eq("persistent")
          expect(calls).to eq(2)
          expect(http1(origin, "/ordinary", context: context(offers: nil)).values_at(0, 2)).to eq([200, "ordinary"])
        ensure
          old&.close(grace: 0)
          AsyncHelpers::INTERNAL.raw_close(pending)
        end
      end
    end
  end

  it "enforces strict mTLS on both protocols and scopes a detached authenticated leaf to the exact handler task" do
    reactor do
      seen = []
      host = nil
      implementation = server(echo: lambda { |message, _|
        leaf = host.peer_certificate
        seen << [leaf.serial.to_i, Async::Task.current.async { host.peer_certificate }.wait]
        leaf.serial = 999
        seen << host.peer_certificate.serial.to_i
        message
      })
      falcon(server: implementation, scheme: "https",
             tls: host_tls(anchors: [tls_material[:client_ca]])) do |running, origin|
        host = running
        with_ca do |ca|
          valid = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca,
                                                   **host_tls(tls_material[:identity]).except(:client_ca))
          expect(client(valid, origin).echo(AsyncHelpers::MESSAGE.new).message).to eq(AsyncHelpers::MESSAGE.new)
          %w[h2 http/1.1].each do |protocol|
            [nil, credentials(333, "untrusted", usage: "clientAuth")].each do |identity|
              expect do
                peer(origin, context(identity: identity, offers: [protocol])) do |ssl|
                  ssl.write("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
                  ssl.read(1)
                end
              end.to(raise_error { |error| expect(error).to be_a(OpenSSL::SSL::SSLError).or be_a(Errno::ECONNRESET) })
            end
          end
          bytes = AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new)
          http_context = context(identity: tls_material[:identity], offers: ["http/1.1"])
          result = http1(
            origin, AsyncHelpers::ECHO::ECHO.path, method: "POST", body: bytes,
                                                   headers: { "content-type" => "application/proto" },
                                                   context: http_context
          )
          expect(result.first).to eq(200)
          expect(seen).to eq([[201, nil], 201, [201, nil], 201])
          expect(host.peer_certificate).to be_nil
        ensure
          valid&.close(grace: 0)
        end
      end
    end
  end

  it "preserves explicit wire chains and rotates overlapping/scoped client trust without recapturing old peers" do
    reactor do
      root = tls_material[:root]
      intermediate = credentials(10, "server intermediate", issuer: root, authority: true)
      leaf = credentials(11, "localhost", issuer: intermediate, usage: "serverAuth")
      client_root = tls_material[:client_ca]
      scoped = credentials(20, "scoped", issuer: client_root, authority: true)
      sibling = credentials(21, "sibling", issuer: client_root, authority: true)
      allowed = credentials(22, "allowed", issuer: scoped, usage: "clientAuth")
      denied = credentials(23, "denied", issuer: sibling, usage: "clientAuth")
      candidate = host_tls(leaf, chain: [intermediate], anchors: [scoped]).merge(version: "one")
      falcon(scheme: "https", tls_provider: -> { candidate }) do |host, origin|
        peer(origin, context(identity: allowed, chain: [scoped])) do |ssl|
          expect(ssl.peer_cert_chain.map { |cert| cert.serial.to_i }).to eq([11, 10])
        end
        expect do
          peer(origin, context(identity: denied, chain: [sibling])) do |ssl|
            ssl.write("x")
            ssl.read(1)
          end
        end.to raise_error(OpenSSL::SSL::SSLError)
        candidate = candidate.merge(version: "two", client_ca: [scoped.first.to_pem, sibling.first.to_pem])
        expect(host.refresh_tls).to eq(2)
        peer(origin, context(identity: denied, chain: [sibling])) { |ssl| expect(ssl.peer_cert.serial.to_i).to eq(11) }
        candidate = candidate.merge(version: "three", client_ca: [sibling.first.to_pem])
        expect(host.refresh_tls).to eq(3)
        expect do
          peer(origin, context(identity: allowed, chain: [scoped])) do |ssl|
            ssl.write("x")
            ssl.read(1)
          end
        end.to raise_error(OpenSSL::SSL::SSLError)
      end
    end
  end
  it "rejects expired cached accepts and captured pending handshake starts while preserving established peers" do
    reactor do |task|
      leaf = credentials(404, "localhost", issuer: tls_material[:root], usage: "serverAuth")
      leaf.first.not_after = Time.now + 2
      leaf.first.sign(tls_material[:root].last, "SHA256")
      falcon(scheme: "https", tls: host_tls(leaf)) do |host, origin|
        with_ca do |ca|
          transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca)
          typed = client(transport, origin)
          expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "before")).message.name).to eq("before")
          pending = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
          session = host.instance_variable_get(:@session)
          wait_until(task, "pending pre-expiry socket captured") do
            session.instance_variable_get(:@resources).size == 2
          end
          # Actual certificate wall-clock expiry is the external timing boundary
          # under test. No readiness or asynchronous completion is inferred here.
          remaining = leaf.first.not_after - Time.now
          begin
            task.with_timeout(remaining + 0.01) { Async::Condition.new.wait }
          rescue Async::TimeoutError
            nil
          end
          expect do
            peer(origin, context, raw: pending) { raise "Expired pending handshake succeeded" }
          end.to raise_error(OpenSSL::SSL::SSLError)
          fresh_raw = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
          expect(fresh_raw.read(1)).to be_nil
          expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "established")).message.name).to eq("established")
          expect(host.reload_tls(**host_tls)).to eq(2)
          peer(origin, context) { |ssl| expect(ssl.peer_cert.serial.to_i).to eq(101) }
        ensure
          transport&.close(grace: 0)
          AsyncHelpers::INTERNAL.raw_close(pending)
          AsyncHelpers::INTERNAL.raw_close(fresh_raw)
        end
      end
    end
  end
  it "serves cached handshakes during source outage and concurrent refresh, then recovers atomically" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      mode = :initial
      calls = 0
      initial = host_tls
      next_tls = host_tls(credentials(102, "localhost", issuer: tls_material[:root], usage: "serverAuth"))
      provider = lambda do
        calls += 1
        raise "secret-outage" if mode == :outage

        if mode == :refreshing
          entered.enqueue(true)
          release.dequeue
          next_tls.merge(version: "next")
        else
          initial.merge(version: "initial")
        end
      end
      falcon(scheme: "https", tls_provider: provider, on_tls_error: ->(_) {}) do |host, origin|
        mode = :outage
        expect { host.refresh_tls }.to raise_error(ConnectRPC::Async::TLSConfiguration::Error, /source/)
        peer(origin, context) { |ssl| expect(ssl.peer_cert.serial.to_i).to eq(101) }
        mode = :refreshing
        refreshing = task.async { host.refresh_tls }
        entered.dequeue
        peer(origin, context, sni: false) { |ssl| expect(ssl.peer_cert.serial.to_i).to eq(101) }
        expect(refreshing).not_to be_finished
        release.enqueue(true)
        expect(refreshing.wait).to eq(2)
        peer(origin, context) { |ssl| expect(ssl.peer_cert.serial.to_i).to eq(102) }
        expect(calls).to eq(3)
      ensure
        release.enqueue(true)
      end
    end
  end

  it "accounts for setup timeout and pre-handshake admission rejection without leaking connection metrics" do
    reactor do |task|
      registry = Async::Utilization::Registry.new
      falcon(scheme: "https", tls: host_tls, utilization_registry: registry,
             max_connections: 1, handshake_timeout: 0.03) do |_host, origin|
        first = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
        wait_until(task, "first silent raw socket accepted") { registry.metric(:connections_total).value == 1 }
        excess = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
        expect(excess.read(1)).to be_nil
        expect(first.read(1)).to be_nil
        wait_until(task, "setup rejection metrics finished") { registry.metric(:connections_active).value.zero? }
        expect(registry.metric(:connections_total).value).to eq(2)
        expect(registry.metric(:requests_total).value).to eq(0)
      ensure
        AsyncHelpers::INTERNAL.raw_close(first)
        AsyncHelpers::INTERNAL.raw_close(excess)
      end
    end
  end
end
