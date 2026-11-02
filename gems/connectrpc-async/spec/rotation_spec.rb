# frozen_string_literal: true

RSpec.describe "HTTP/2 TLS retirement and session isolation" do
  def raw_context(authority, identity = nil, version: nil)
    context = OpenSSL::SSL::SSLContext.new
    store = OpenSSL::X509::Store.new
    store.add_cert(authority.first)
    context.cert_store = store
    context.verify_mode = OpenSSL::SSL::VERIFY_PEER
    context.verify_hostname = true
    context.alpn_protocols = ["h2"]
    context.min_version = version || OpenSSL::SSL::TLS1_2_VERSION
    context.max_version = version if version
    context.add_certificate(identity.first, identity.last, []) if identity
    context
  end

  def raw_h2(host, context, session: nil)
    raw = IO::Endpoint.tcp("127.0.0.1", host.port).connect
    ssl = OpenSSL::SSL::SSLSocket.new(raw, context)
    ssl.sync_close = true
    ssl.hostname = "localhost"
    ssl.session = session if session
    ssl.connect
    connection = Async::HTTP::Protocol::HTTP2.client(ssl)
    outgoing = Protocol::HTTP::Request["POST", AsyncHelpers::ECHO::ECHO.path,
                                       [["content-type", "application/proto"]], [], scheme: "https",
                                                                                    authority: "localhost:#{host.port}"]
    response = outgoing.call(connection)
    raise "Raw typed call failed" unless response.status == 200

    response.read
    { reused: ssl.session_reused?, session: ssl.session, leaf: ssl.peer_cert.serial.to_i }
  ensure
    connection&.close
    AsyncHelpers::INTERNAL.raw_close(raw)
    ssl&.close
  end

  [OpenSSL::SSL::TLS1_2_VERSION, OpenSSL::SSL::TLS1_3_VERSION].each do |version|
    it "proves a valid resumable positive control, then disabled/revoked sessions for TLS #{version}" do
      reactor do |task|
        material = tls_material
        ca = material.fetch(:root)
        old_ca = material.fetch(:client_ca)
        old_identity = material.fetch(:identity)
        new_ca = credentials(3, "new client CA", authority: true)
        new_identity = credentials(202, "new client", issuer: new_ca, usage: "clientAuth")
        # Positive control: same identity/session and runtime really can resume.
        control = OpenSSL::SSL::SSLContext.new
        control.add_certificate(material.fetch(:leaf).first, material.fetch(:leaf).last, [])
        control.alpn_select_cb = ->(_protocols) { "h2" }
        control.verify_mode = OpenSSL::SSL::VERIFY_PEER | OpenSSL::SSL::VERIFY_FAIL_IF_NO_PEER_CERT
        client_store = OpenSSL::X509::Store.new
        client_store.add_cert(old_ca.first)
        control.cert_store = client_store
        control.min_version = version
        control.max_version = version
        control.session_id_context = "resumable-positive-control"
        control.session_cache_mode = OpenSSL::SSL::SSLContext::SESSION_CACHE_SERVER
        control.options &= ~OpenSSL::SSL::OP_NO_TICKET
        control.setup
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        listener = bound.sockets.first
        server_task = task.async do
          2.times do
            raw, = listener.accept
            peer = OpenSSL::SSL::SSLSocket.new(raw, control)
            peer.sync_close = true
            begin
              peer.accept
              peer.write("x")
              peer.read(1)
            ensure
              AsyncHelpers::INTERNAL.raw_close(raw)
              peer.close
            end
          end
        end
        saved = nil
        sessions = []
        context = raw_context(ca, old_identity, version: version)
        context.session_new_cb = lambda { |_socket, session|
          saved = session
          sessions << session
        }
        reused = []
        2.times do |index|
          raw = IO::Endpoint.tcp("127.0.0.1", listener.local_address.ip_port).connect
          peer = OpenSSL::SSL::SSLSocket.new(raw, context)
          peer.sync_close = true
          peer.hostname = "localhost"
          peer.session = saved if index.positive?
          peer.connect
          expect(peer.read(1)).to eq("x")
          saved ||= peer.session
          reused << peer.session_reused?
          peer.write("x")
          peer.close
        end
        server_task.wait
        bound.close
        expect(reused).to eq([false, true])
        valid_control_session = saved
        host = TLSObservedHost.new(server: server, tls: host_tls(anchors: [old_ca])).start
        actual_context = context
        sessions.clear
        initial = raw_h2(host, actual_context)
        saved = sessions.last || initial.fetch(:session)
        expect(raw_h2(host, actual_context, session: saved).fetch(:reused)).to be(false)
        previous_context = host.instance_variable_get(:@snapshot).instance_variable_get(:@context)
        host.reload_tls(**host_tls(anchors: [old_ca]))
        expect(host.instance_variable_get(:@snapshot).instance_variable_get(:@context)).not_to equal(previous_context)
        expect(raw_h2(host, actual_context, session: saved).fetch(:reused)).to be(false)
        host.reload_tls(**host_tls(anchors: [old_ca, new_ca]))
        expect(raw_h2(host, actual_context).fetch(:reused)).to be(false)
        expect(raw_h2(host, raw_context(ca, new_identity, version: version)).fetch(:reused)).to be(false)
        host.reload_tls(**host_tls(anchors: [new_ca]))
        [saved, valid_control_session].each do |candidate|
          errors_before = host.setup_errors.size
          expect { raw_h2(host, actual_context, session: candidate) }.to raise_error do |error|
            expect(error).to be_a(OpenSSL::SSL::SSLError).or be_a(EOFError).or be_a(IOError)
          end
          wait_until(task, "server verification rejection") { host.setup_errors.size > errors_before }
          expect(host.setup_errors.last).to match(/certificate verify failed/i)
        end
        expect(raw_h2(host, raw_context(ca, new_identity, version: version)).fetch(:reused)).to be(false)
      ensure
        host&.close(grace: 0)
        bound&.close
      end
    end
  end

  it "captures a cutoff, retires pending old handshakes, rejects old later streams and preserves newer generations" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      host = ConnectRPC::Async::Host.new(server: server(echo: lambda { |message, _|
        if message.name == "hold"
          entered.enqueue(true)
          release.dequeue
        end
        message
      }), tls: host_tls).start
      with_ca do |ca_file|
        origin = "https://localhost:#{host.port}"
        old = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca_file)
        call = task.async { client(old, origin).echo(AsyncHelpers::MESSAGE.new(name: "hold"), timeout: nil) }
        entered.dequeue
        pending = IO::Endpoint.tcp("127.0.0.1", host.port).connect
        wait_until(task, "pending handshake") { host.instance_variable_get(:@resources).size == 2 }
        pending_record = host.instance_variable_get(:@resources).last
        host.reload_tls(**host_tls)
        current = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca_file)
        expect(client(current, origin).echo(AsyncHelpers::MESSAGE.new(name: "current")).message.name).to eq("current")
        drain = task.async { host.drain_tls(grace: 1) }
        wait_until(task, "old generation draining") { pending_record.raw.closed? }
        expect(pending.read(1)).to be_nil
        response = old.call(request("#{origin}#{AsyncHelpers::ECHO::ECHO.path}",
                                    headers: { "content-type" => ["application/proto"] }))
        expect(response.status).to eq(503)
        host.reload_tls(**host_tls)
        latest = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca_file)
        expect(client(latest, origin).echo(AsyncHelpers::MESSAGE.new(name: "latest")).message.name).to eq("latest")
        expect(drain).not_to be_finished
        release.enqueue(true)
        expect(call.wait.message.name).to eq("hold")
        expect(drain.wait).to be(true)
        expect(pending_record.task).to be_finished
        expect(client(current, origin).echo(AsyncHelpers::MESSAGE.new(name: "retained")).message.name).to eq("retained")
        expect(client(latest, origin).echo(AsyncHelpers::MESSAGE.new(name: "newer")).message.name).to eq("newer")
        expect(host.drain_tls(grace: 1)).to be(true)
      ensure
        [old, current, latest].compact.each { |transport| transport.close(grace: 0) }
        AsyncHelpers::INTERNAL.raw_close(pending)
      end
    ensure
      release&.enqueue(true)
      host&.close(grace: 0)
    end
  end
end
