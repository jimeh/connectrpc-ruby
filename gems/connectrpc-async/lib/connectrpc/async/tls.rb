# frozen_string_literal: true

require "securerandom"

module ConnectRPC
  module Async
    module Internal
      class TLS
        CERTIFICATE = %r{\A-----BEGIN CERTIFICATE-----\s+[A-Za-z0-9+/=\s]+-----END CERTIFICATE-----\z}
        PRIVATE_KEY = %r{
          \A-----BEGIN\ (RSA\ PRIVATE\ KEY|EC\ PRIVATE\ KEY|PRIVATE\ KEY)-----
          \s+[A-Za-z0-9+/=\s]+-----END\ \1-----\z
        }x

        def self.certificate(pem)
          raise ArgumentError unless pem.is_a?(String) && CERTIFICATE.match?(pem.strip)

          certificates = OpenSSL::X509::Certificate.load(pem.dup)
          raise ArgumentError unless certificates.size == 1

          certificate = certificates.first
          raise ArgumentError unless certificate.not_before <= Time.now && certificate.not_after > Time.now

          certificate
        end

        def self.identity(chain, pem)
          raise ArgumentError unless chain.is_a?(Array) && !chain.empty?
          raise ArgumentError unless pem.is_a?(String) && PRIVATE_KEY.match?(pem.strip)

          certificates = chain.map { |element| certificate(element) }
          key = OpenSSL::PKey.read(pem.dup) { nil }
          digest = %w[ED25519 ED448].include?(key.oid) ? nil : "SHA256"
          challenge = SecureRandom.random_bytes(32)
          signature = key.sign(digest, challenge)
          raise ArgumentError unless certificates.first.public_key.verify(digest, signature, challenge)

          certificates.each_cons(2) do |child, issuer|
            raise ArgumentError unless child.issuer == issuer.subject && child.verify(issuer.public_key)
          end
          [certificates, key]
        end

        def self.configure(context)
          context.min_version = OpenSSL::SSL::TLS1_2_VERSION
          context.options |= OpenSSL::SSL::OP_NO_TICKET
          context.session_cache_mode = OpenSSL::SSL::SSLContext::SESSION_CACHE_OFF
          context.session_id_context = SecureRandom.random_bytes(16)
        end

        def self.server(certificate_chain:, private_key:, client_ca:)
          certificates, key = identity(certificate_chain, private_key)
          context = OpenSSL::SSL::SSLContext.new
          configure(context)
          context.add_certificate(certificates.first, key, certificates.drop(1))
          unless client_ca.nil?
            raise ArgumentError unless client_ca.is_a?(Array) && !client_ca.empty?

            store = OpenSSL::X509::Store.new
            client_ca.each { |pem| store.add_cert(certificate(pem)) }
            store.flags = OpenSSL::X509::V_FLAG_PARTIAL_CHAIN
            context.cert_store = store
            context.verify_mode = OpenSSL::SSL::VERIFY_PEER | OpenSSL::SSL::VERIFY_FAIL_IF_NO_PEER_CERT
          end
          context.alpn_select_cb = lambda do |protocols|
            raise OpenSSL::SSL::SSLError, "HTTP/2 ALPN is required" unless protocols.include?("h2")

            "h2"
          end
          context.setup
          new(context, !client_ca.nil?)
        rescue StandardError
          raise ArgumentError, "Invalid TLS snapshot", cause: nil
        end

        def self.client(ca_file:, certificate_chain:, private_key:)
          context = OpenSSL::SSL::SSLContext.new
          context.set_params(verify_mode: OpenSSL::SSL::VERIFY_PEER, verify_hostname: true)
          configure(context)
          store = OpenSSL::X509::Store.new
          store.set_default_paths
          store.add_file(ca_file) if ca_file
          context.cert_store = store
          if certificate_chain || private_key
            certificates, key = identity(certificate_chain, private_key)
            context.add_certificate(certificates.first, key, certificates.drop(1))
          end
          context.alpn_protocols = ["h2"]
          context.setup
          new(context, false)
        rescue StandardError
          raise ArgumentError, "Invalid TLS client configuration", cause: nil
        end

        def initialize(context, authenticated)
          @context = context
          @authenticated = authenticated
          freeze
        end

        def socket(raw)
          OpenSSL::SSL::SSLSocket.new(raw, @context).tap { |socket| socket.sync_close = true }
        end

        def authenticated? = @authenticated
        def inspect = "#<#{self.class} private TLS snapshot>"
      end
    end
  end
end
