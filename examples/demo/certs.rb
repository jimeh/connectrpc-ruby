# frozen_string_literal: true

require "fileutils"
require "openssl"
require_relative "support"

module DemoExample
  # Demo plumbing, not ConnectRPC usage: a throwaway certificate authority (CA)
  # that issues one server and one client certificate for the --mtls examples.
  # Real deployments get these from their own PKI. The files live in a tls
  # directory next to the state file (vendor/examples/tls by default). A server
  # started with --mtls reuses them until they are about to expire.
  #
  # Like most short-lived setups, renewal issues new leaf certificates under the
  # same CA, so servers and clients that still hold the previous leaves keep
  # trusting each other. Only an expiring CA is replaced.
  module Certs
    FILES = {
      ca: "ca.pem", ca_key: "ca-key.pem", server: "server.pem", server_key: "server-key.pem",
      client: "client.pem", client_key: "client-key.pem"
    }.freeze
    KEYS = %i[ca_key server_key client_key].freeze
    CERTIFICATES = %i[ca server client].freeze
    CA_VALIDITY = 30 * 24 * 60 * 60
    LEAF_VALIDITY = 7 * 24 * 60 * 60
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

    # Returns :reused or :issued. A server that starts at the same time as
    # another waits for the lock, then reuses the set the other one issued.
    def prepare!
      locked do
        next :reused if usable?

        issue_all
        :issued
      end
    end

    # Issues new server and client certificates now, under the current CA.
    # Running --mtls servers load them at their next TLS refresh:
    #   ruby -r ./examples/demo/certs -e DemoExample::Certs.renew!
    def renew! = locked { issue_all }

    def locked
      FileUtils.mkdir_p(File.dirname(dir))
      File.open("#{dir}.lock", File::RDWR | File::CREAT, 0o600) do |lock|
        lock.flock(File::LOCK_EX)
        yield
      end
    end

    # Writes the set to a private directory, then renames it into place, so a
    # reader never sees a half-written set. A reader that looks during the
    # rename finds no files and tries again later.
    def issue_all
      staging = "#{dir}.#{Process.pid}"
      FileUtils.rm_rf(staging)
      FileUtils.mkdir_p(staging, mode: 0o700)
      write_set(staging, *(current_authority || new_authority))
      FileUtils.rm_rf(dir)
      File.rename(staging, dir)
    end

    # The current CA and its key while the CA stays valid for another day.
    def current_authority
      return unless File.file?(path(:ca)) && File.file?(path(:ca_key))

      ca = OpenSSL::X509::Certificate.new(read(:ca))
      [ca, OpenSSL::PKey.read(read(:ca_key))] if ca.not_after > Time.now + RENEW_BEFORE
    rescue OpenSSL::OpenSSLError
      nil
    end

    def new_authority
      key = OpenSSL::PKey::EC.generate("prime256v1")
      [authority(key), key]
    end

    def write_set(target, issuer, issuer_key)
      server_key, client_key = Array.new(2) { OpenSSL::PKey::EC.generate("prime256v1") }
      # The server certificate names the addresses clients verify as its hostname.
      server = leaf("CN=demo-server", server_key, issuer, issuer_key, usage: "serverAuth",
                                                                      names: "DNS:localhost,IP:127.0.0.1")
      client = leaf("CN=demo-client", client_key, issuer, issuer_key, usage: "clientAuth")
      { ca: issuer, server: server, client: client }.each { |name, cert| write(target, name, cert.to_pem) }
      KEYS.zip([issuer_key, server_key, client_key]).each do |name, key|
        write(target, name, key.private_to_pem, mode: 0o600)
      end
    end

    # A self-signed CA certificate that may only sign other certificates.
    def authority(key)
      name = OpenSSL::X509::Name.parse("CN=ConnectRPC demo CA")
      cert = unsigned(name, key, name, CA_VALIDITY)
      add_extensions(cert, cert, [["basicConstraints", "CA:TRUE", true], ["keyUsage", "keyCertSign,cRLSign", true]])
      cert.sign(key, "SHA256")
    end

    # A certificate signed by the CA for one purpose: serverAuth or clientAuth.
    def leaf(subject, key, issuer, issuer_key, usage:, names: nil)
      cert = unsigned(OpenSSL::X509::Name.parse(subject), key, issuer.subject, LEAF_VALIDITY)
      add_extensions(cert, issuer, [["basicConstraints", "CA:FALSE", true], ["keyUsage", "digitalSignature", true],
                                    ["extendedKeyUsage", usage], (["subjectAltName", names] if names)].compact)
      cert.sign(issuer_key, "SHA256")
    end

    def unsigned(subject, key, issuer, validity)
      cert = OpenSSL::X509::Certificate.new
      cert.version = 2 # X.509 v3, which allows extensions.
      cert.serial = OpenSSL::BN.rand(64)
      cert.subject = subject
      cert.issuer = issuer
      cert.public_key = key
      cert.not_before = Time.now - 60
      cert.not_after = Time.now + validity
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
