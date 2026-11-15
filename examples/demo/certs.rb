# frozen_string_literal: true

require "fileutils"
require "openssl"
require_relative "support"

module DemoExample
  # Demo plumbing, not ConnectRPC usage: a throwaway certificate authority (CA)
  # that issues one server and one client certificate for the --mtls examples.
  # Real deployments get these from their own PKI. The files are read through a
  # tls symlink next to the state file (vendor/examples/tls by default). A server
  # started with --mtls reuses them until they are about to expire.
  #
  # Each issue writes a new set directory and then repoints the symlink at it,
  # so readers never see a set change while they read it.
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

    # One issued set. A set never changes after the symlink points at it, so
    # every file read through one Generation belongs to the same set.
    Generation = Struct.new(:dir) do
      def path(name) = File.join(dir, FILES.fetch(name))
      def read(name) = File.read(path(name))
      def present?(*names) = names.all? { |name| File.file?(path(name)) }
    end

    module_function

    def dir = File.join(File.dirname(State.path), "tls")
    def display_dir = dir.delete_prefix("#{ROOT}/")

    # The set the tls symlink points at now, or nil before the first issue.
    def current
      Generation.new(File.realpath(dir))
    rescue Errno::ENOENT
      nil
    end

    def present?(*names) = current&.present?(*names) || false

    # True when every certificate is present and valid for at least another day.
    def usable?
      set = current
      set&.present?(*FILES.keys) && CERTIFICATES.all? do |name|
        OpenSSL::X509::Certificate.new(set.read(name)).not_after > Time.now + RENEW_BEFORE
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

    # Writes a complete new set, then publishes it.
    def issue_all
      target = "#{dir}.#{Time.now.utc.strftime('%Y%m%d%H%M%S%N')}"
      FileUtils.mkdir_p(target, mode: 0o700)
      write_set(target, *(current_authority || new_authority))
      publish(target)
    end

    # Renaming a new symlink over tls switches every later reader to the new set
    # at once. The replaced set stays for a minute for readers that resolved it
    # already, then a later issue removes it.
    def publish(target)
      replaced = current&.dir if File.symlink?(dir)
      FileUtils.rm_rf(dir) unless File.symlink?(dir) # A tls directory from older examples.
      link = "#{dir}.link"
      FileUtils.rm_f(link)
      File.symlink(File.basename(target), link)
      File.rename(link, dir)
      FileUtils.touch(replaced) if replaced # Its mtime now records when it was replaced.
      remove_replaced_sets
    end

    # The new set and the one it replaced both have a fresh mtime, so only sets
    # replaced over a minute ago go.
    def remove_replaced_sets
      FileUtils.rm_rf(Dir.glob("#{dir}.[0-9]*").select { |set| File.mtime(set) < Time.now - 60 })
    end

    # The current CA and its key while the CA stays valid for another day.
    def current_authority
      set = current
      return unless set&.present?(:ca, :ca_key)

      ca = OpenSSL::X509::Certificate.new(set.read(:ca))
      [ca, OpenSSL::PKey.read(set.read(:ca_key))] if ca.not_after > Time.now + RENEW_BEFORE
    rescue OpenSSL::OpenSSLError
      nil
    end

    def new_authority
      key = OpenSSL::PKey::EC.generate("prime256v1")
      [X509.authority(key), key]
    end

    def write_set(target, issuer, issuer_key)
      server_key, client_key = Array.new(2) { OpenSSL::PKey::EC.generate("prime256v1") }
      # The server certificate names the addresses clients verify as its hostname.
      server = X509.leaf("CN=demo-server", server_key, issuer, issuer_key, usage: "serverAuth",
                                                                           names: "DNS:localhost,IP:127.0.0.1")
      client = X509.leaf("CN=demo-client", client_key, issuer, issuer_key, usage: "clientAuth")
      { ca: issuer, server: server, client: client }.each { |name, cert| write(target, name, cert.to_pem) }
      KEYS.zip([issuer_key, server_key, client_key]).each do |name, key|
        write(target, name, key.private_to_pem, mode: 0o600)
      end
    end

    def write(target, name, pem, mode: 0o644)
      File.write(File.join(target, FILES.fetch(name)), pem, perm: mode)
    end

    # Builds the X.509 certificates themselves.
    module X509
      module_function

      # A self-signed CA certificate that may only sign other certificates.
      def authority(key)
        name = OpenSSL::X509::Name.parse("CN=ConnectRPC demo CA")
        cert = unsigned(name, key, name, CA_VALIDITY)
        add_extensions(cert, cert, [["basicConstraints", "CA:TRUE", true], ["keyUsage", "keyCertSign,cRLSign", true]])
        cert.sign(key, "SHA256")
      end

      # A certificate signed by the CA for one purpose: serverAuth or clientAuth.
      # It never outlives the CA that signed it.
      def leaf(subject, key, issuer, issuer_key, usage:, names: nil)
        validity = [LEAF_VALIDITY, issuer.not_after - Time.now].min
        cert = unsigned(OpenSSL::X509::Name.parse(subject), key, issuer.subject, validity)
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
    end
  end
end
