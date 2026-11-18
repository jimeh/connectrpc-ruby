# frozen_string_literal: true

require "openssl"
require "socket"
require "tmpdir"

# Disposable local certificates and TLS peers. Nothing leaves loopback.
module TLSPeer # rubocop:disable Metrics/ModuleLength
  TARGET = "connectrpc-target.test"

  def tls_credentials(serial, name, issuer: nil, authority: false, usage: nil, names: nil, not_after: Time.now + 3600) # rubocop:disable Metrics/CyclomaticComplexity
    key = OpenSSL::PKey::EC.generate("prime256v1")
    certificate = OpenSSL::X509::Certificate.new
    certificate.version = 2
    certificate.serial = serial
    certificate.subject = OpenSSL::X509::Name.parse("/CN=#{name}")
    certificate.issuer = issuer ? issuer.first.subject : certificate.subject
    certificate.public_key = key
    certificate.not_before = Time.now - 120
    certificate.not_after = not_after
    factory = OpenSSL::X509::ExtensionFactory.new
    factory.subject_certificate = certificate
    factory.issuer_certificate = issuer ? issuer.first : certificate
    certificate.add_extension(factory.create_extension("basicConstraints", authority ? "CA:TRUE" : "CA:FALSE", true))
    key_usage = authority ? "keyCertSign,cRLSign" : "digitalSignature,keyEncipherment"
    certificate.add_extension(factory.create_extension("keyUsage", key_usage, true))
    certificate.add_extension(factory.create_extension("extendedKeyUsage", usage)) if usage
    certificate.add_extension(factory.create_extension("subjectAltName", names)) if names
    certificate.sign(issuer ? issuer.last : key, "SHA256")
    [certificate, key]
  end

  def tls_material
    @tls_material ||= begin
      root = tls_credentials(1, "server root", authority: true)
      client_root = tls_credentials(2, "client root", authority: true)
      client_intermediate = tls_credentials(3, "client intermediate", issuer: client_root, authority: true)
      {
        root: root, client_root: client_root, client_intermediate: client_intermediate,
        leaf: tls_credentials(101, "localhost", issuer: root, usage: "serverAuth",
                                                names: "DNS:localhost,DNS:#{TARGET},IP:127.0.0.1"),
        other: tls_credentials(102, "other", issuer: root, usage: "serverAuth", names: "DNS:other.test"),
        client: tls_credentials(201, "client", issuer: client_intermediate, usage: "clientAuth"),
        expired: tls_credentials(202, "expired", issuer: client_intermediate, usage: "clientAuth",
                                                 not_after: Time.now - 60)
      }
    end
  end

  def with_ca_file(certificate = tls_material.fetch(:root).first)
    Dir.mktmpdir("connectrpc-net-http-ca-") do |directory|
      file = File.join(directory, "ca.pem")
      File.write(file, certificate.to_pem)
      yield file
    end
  end

  def client_identity(chain = %i[client client_intermediate], key: :client)
    { certificate_chain: chain.map { |name| tls_material.fetch(name).first.to_pem },
      private_key: tls_material.fetch(key).last.private_to_pem }
  end

  # Server names arrive on +names+; a client certificate requirement trusts
  # only +client_ca+.
  def server_context(leaf = tls_material.fetch(:leaf), client_ca: nil, names: nil)
    context = OpenSSL::SSL::SSLContext.new
    context.add_certificate(leaf.first, leaf.last)
    if client_ca
      store = OpenSSL::X509::Store.new
      store.add_cert(client_ca.first)
      context.cert_store = store
      context.verify_mode = OpenSSL::SSL::VERIFY_PEER | OpenSSL::SSL::VERIFY_FAIL_IF_NO_PEER_CERT
    end
    context.servername_cb = lambda { |(_socket, name)|
      names&.push(name)
      nil
    }
    context
  end

  # Accepts one TLS connection. A failed handshake goes to +failures+ and the
  # handler does not run.
  def with_tls_peer(context, handler, failures: Queue.new) # rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
    listener = TCPServer.new("127.0.0.1", 0)
    raw = nil
    worker = Thread.new do
      raw = listener.accept
      ssl = OpenSSL::SSL::SSLSocket.new(raw, context)
      ssl.sync_close = true
      begin
        ssl.accept
      rescue OpenSSL::SSL::SSLError => e
        failures << e
        next
      end
      handler.call(ssl)
    rescue IOError, Errno::EBADF, Errno::ECONNRESET, Errno::EPIPE
      raise unless listener.closed?
    ensure
      raw&.close unless raw&.closed?
    end
    yield listener.addr[1], listener
    raise "TLS peer did not finish" unless worker.join(5)

    worker.value
  ensure
    listener&.close unless listener&.closed?
    raw&.close unless raw&.closed?
    raise "TLS peer leaked a worker" if worker && !worker.join(5)
  end

  # Copies bytes both ways until either side closes.
  def relay(left, right)
    loop do
      readable, = IO.select([left, right])
      readable.each do |source|
        data = source.readpartial(16_384)
        (source.equal?(left) ? right : left).write(data)
      end
    end
  rescue IOError, SystemCallError
    nil
  end
end
