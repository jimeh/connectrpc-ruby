# frozen_string_literal: true

# Client transport failures are local: they keep their code, name what failed
# with a fixed phrase and retain the original exception as their cause. The
# message never carries the origin's host, address or certificate details.
RSpec.describe "Classified client transport failures" do
  def failure
    yield
    raise "Expected a transport failure"
  rescue ConnectRPC::Error => e
    e
  end

  def expect_failure(error, prefix, reason, cause)
    expect([error.code, error.message]).to eq(["unavailable", "#{prefix} transport failed: #{reason}"])
    expect(error.cause).to be_a(cause)
    expect(error.message).not_to include("127.0.0.1", "localhost", ".invalid", "certificate", "SSL")
  end

  # Binding without listening reserves a port whose connections are refused.
  def with_refusing_port
    reserved = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)
    reserved.bind(Socket.sockaddr_in(0, "127.0.0.1"))
    yield reserved.local_address.ip_port
  ensure
    reserved&.close
  end

  # A TLS listener that completes or fails handshakes and never speaks HTTP.
  def with_tls_peer(task, identity, alpn: lambda(&:first))
    bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
    listener = bound.sockets.first
    context = OpenSSL::SSL::SSLContext.new
    context.min_version = OpenSSL::SSL::TLS1_2_VERSION
    context.add_certificate(identity.first, identity.last, [])
    context.alpn_select_cb = alpn
    peers = []
    acceptor = task.async do
      loop do
        raw, = listener.accept
        peers << task.async do
          ssl = OpenSSL::SSL::SSLSocket.new(raw, context)
          ssl.sync_close = true
          ssl.accept
          ssl.read(1)
        rescue OpenSSL::SSL::SSLError, IOError, SystemCallError
          nil
        ensure
          AsyncHelpers::INTERNAL.raw_close(raw)
        end
      end
    end
    yield listener.local_address.ip_port
  ensure
    [acceptor, *peers].compact.each { |peer| AsyncHelpers::INTERNAL.cancel(peer) }
    bound&.close
  end

  def misnamed_identity
    root = tls_material.fetch(:root)
    certificate, key = tls_material.fetch(:leaf)
    misnamed = OpenSSL::X509::Certificate.new(certificate.to_der)
    misnamed.extensions = misnamed.extensions.reject { |extension| extension.oid == "subjectAltName" }
    factory = OpenSSL::X509::ExtensionFactory.new
    factory.subject_certificate = misnamed
    factory.issuer_certificate = root.first
    misnamed.add_extension(factory.create_extension("subjectAltName", "DNS:elsewhere.invalid"))
    misnamed.sign(root.last, "SHA256")
    [misnamed, key]
  end

  def call_once(kind, origin, **)
    transport = kind.new(origin: origin, **)
    transport.call(request("#{origin}/path", deadline: ConnectRPC::Deadline.new(5)))
  ensure
    transport&.close(grace: 0)
  end

  { "HTTP/2" => ConnectRPC::Async::Transport, "HTTP/1" => ConnectRPC::Async::HTTP1Transport }.each do |prefix, kind|
    it "reports a refused #{prefix} connection" do
      reactor do
        with_refusing_port do |port|
          error = failure { call_once(kind, "http://127.0.0.1:#{port}") }
          expect_failure(error, prefix, "connection refused", Errno::ECONNREFUSED)
        end
      end
    end

    it "reports a failed #{prefix} name resolution" do
      reactor do
        error = failure { call_once(kind, "http://connectrpc-ruby-client-failure.invalid:443") }
        expect_failure(error, prefix, "name resolution failed", SocketError)
      end
    end

    it "reports an untrusted #{prefix} server certificate" do
      reactor do |task|
        with_tls_peer(task, tls_material.fetch(:leaf)) do |port|
          error = failure { call_once(kind, "https://localhost:#{port}") }
          expect_failure(error, prefix, "TLS verification failed", OpenSSL::SSL::SSLError)
        end
      end
    end

    %w[localhost 127.0.0.1].each do |host|
      it "reports a #{prefix} server certificate that does not match #{host}" do
        with_ca do |ca_file|
          reactor do |task|
            with_tls_peer(task, misnamed_identity) do |port|
              error = failure { call_once(kind, "https://#{host}:#{port}", ca_file: ca_file) }
              expect_failure(error, prefix, "TLS verification failed", OpenSSL::SSL::SSLError)
            end
          end
        end
      end
    end

    it "reports a #{prefix} ALPN mismatch as a TLS failure" do
      other = kind == ConnectRPC::Async::Transport ? "http/1.1" : "h2"
      with_ca do |ca_file|
        reactor do |task|
          with_tls_peer(task, tls_material.fetch(:leaf), alpn: ->(_offers) { other }) do |port|
            error = failure { call_once(kind, "https://localhost:#{port}", ca_file: ca_file) }
            expect_failure(error, prefix, "ALPN negotiation failed", OpenSSL::SSL::SSLError)
          end
        end
      end
    end
  end

  it "reports a refused connection from a plain thread through ThreadedTransport" do
    with_refusing_port do |port|
      origin = "http://127.0.0.1:#{port}"
      transport = threaded(origin: origin)
      error = failure { transport.call(request("#{origin}/path", deadline: ConnectRPC::Deadline.new(5))) }
      expect_failure(error, "HTTP/2", "connection refused", Errno::ECONNREFUSED)
    ensure
      transport&.close(grace: 0)
    end
  end

  it "keeps the observer outcome for a refused connection" do
    reactor do
      with_refusing_port do |port|
        events = []
        origin = "http://127.0.0.1:#{port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin, connection_observer: ->(event) { events << event })
        failure { transport.call(request("#{origin}/path")) }
        expect(events.select { |event| event.type == :attempt_finished }.map(&:outcome)).to eq([:refused])
      ensure
        transport&.close(grace: 0)
      end
    end
  end

  describe "TLS client configuration" do
    def invalid_input(**)
      reactor { ConnectRPC::Async::Transport.new(origin: "https://localhost:1", **) }
      raise "Expected an invalid TLS configuration"
    rescue ArgumentError => e
      e
    end

    {
      ca_file: ->(_identity) { { ca_file: "/nonexistent/private-ca.pem" } },
      certificate_chain: lambda { |identity|
        { certificate_chain: ["not a certificate"], private_key: identity.last.private_to_pem }
      },
      private_key: ->(identity) { { certificate_chain: [identity.first.to_pem], private_key: "not a key" } }
    }.each do |input, options|
      it "names an invalid #{input} without its value or a cause" do
        error = invalid_input(**options.call(tls_material.fetch(:identity)))
        expect([error.message, error.cause]).to eq(["Invalid TLS client configuration: #{input}", nil])
        expect(error.message).not_to include("nonexistent", "not a")
      end
    end

    it "names a private key that does not belong to the certificate chain" do
      chain = [tls_material.fetch(:identity).first.to_pem]
      error = invalid_input(certificate_chain: chain, private_key: tls_material.fetch(:leaf).last.private_to_pem)
      expect([error.message, error.cause]).to eq(["Invalid TLS client configuration: private_key", nil])
    end
  end
end
