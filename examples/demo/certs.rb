# frozen_string_literal: true

require "fileutils"
require "openssl"
require_relative "support"

module DemoExample
  # Demo plumbing, not ConnectRPC usage: a throwaway certificate authority (CA)
  # that issues one server and one client certificate for the --mtls examples.
  # Real deployments get these from their own PKI. The files live in a tls
  # directory next to the state file (vendor/examples/tls by default), and a
  # server started with --mtls reuses them until they are about to expire.
  module Certs
    FILES = {
      ca: "ca.pem", server: "server.pem", server_key: "server-key.pem", client: "client.pem",
      client_key: "client-key.pem"
    }.freeze
    CERTIFICATES = %i[ca server client].freeze
    VALIDITY = 7 * 24 * 60 * 60
    RENEW_BEFORE = 24 * 60 * 60

    module_function

    def dir = File.join(File.dirname(State.path), "tls")
    def path(name) = File.join(dir, FILES.fetch(name))
    def read(name) = File.read(path(name))
    def present? = FILES.each_key.all? { |name| File.file?(path(name)) }
    def display_dir = dir.delete_prefix("#{ROOT}/")

    # True when every certificate is present and valid for at least another day.
    def usable?
      present? && CERTIFICATES.all? do |name|
        OpenSSL::X509::Certificate.new(read(name)).not_after > Time.now + RENEW_BEFORE
      end
    rescue OpenSSL::X509::CertificateError
      false
    end

    # Writes the set to a private directory, then renames it into place, so a
    # client or a second server never reads a half-written set.
    def issue_all
      staging = "#{dir}.#{Process.pid}"
      FileUtils.rm_rf(staging)
      FileUtils.mkdir_p(staging, mode: 0o700)
      write_set(staging)
      FileUtils.rm_rf(dir)
      File.rename(staging, dir)
    rescue Errno::EEXIST, Errno::ENOTEMPTY
      FileUtils.rm_rf(staging) # Another server installed a set first; use that one.
    end

    def write_set(target)
      ca_key, server_key, client_key = Array.new(3) { OpenSSL::PKey::EC.generate("prime256v1") }
      ca = authority(ca_key)
      # The server certificate names the addresses clients verify as its hostname.
      server = leaf("CN=demo-server", server_key, ca, ca_key, usage: "serverAuth",
                                                              names: "DNS:localhost,IP:127.0.0.1")
      client = leaf("CN=demo-client", client_key, ca, ca_key, usage: "clientAuth")
      { ca: ca, server: server, client: client }.each { |name, cert| write(target, name, cert.to_pem) }
      { server_key: server_key, client_key: client_key }.each do |name, key|
        write(target, name, key.private_to_pem, mode: 0o600)
      end
    end

    # A self-signed CA certificate that may only sign other certificates.
    def authority(key)
      name = OpenSSL::X509::Name.parse("CN=ConnectRPC demo CA")
      cert = unsigned(name, key, name)
      add_extensions(cert, cert, [["basicConstraints", "CA:TRUE", true], ["keyUsage", "keyCertSign,cRLSign", true]])
      cert.sign(key, "SHA256")
    end

    # A certificate signed by the CA for one purpose: serverAuth or clientAuth.
    def leaf(subject, key, issuer, issuer_key, usage:, names: nil)
      cert = unsigned(OpenSSL::X509::Name.parse(subject), key, issuer.subject)
      add_extensions(cert, issuer, [["basicConstraints", "CA:FALSE", true], ["keyUsage", "digitalSignature", true],
                                    ["extendedKeyUsage", usage], (["subjectAltName", names] if names)].compact)
      cert.sign(issuer_key, "SHA256")
    end

    def unsigned(subject, key, issuer)
      cert = OpenSSL::X509::Certificate.new
      cert.version = 2 # X.509 v3, which allows extensions.
      cert.serial = OpenSSL::BN.rand(64)
      cert.subject = subject
      cert.issuer = issuer
      cert.public_key = key
      cert.not_before = Time.now - 60
      cert.not_after = Time.now + VALIDITY
      cert
    end

    # The key identifiers link each certificate to the CA key that signed it.
    def add_extensions(cert, issuer, extensions)
      factory = OpenSSL::X509::ExtensionFactory.new(issuer, cert)
      [*extensions, %w[subjectKeyIdentifier hash], %w[authorityKeyIdentifier keyid:always]].each do |extension|
        cert.add_extension(factory.create_extension(*extension))
      end
    end

    def write(target, name, pem, mode: 0o644)
      File.write(File.join(target, FILES.fetch(name)), pem, perm: mode)
    end
  end
end
