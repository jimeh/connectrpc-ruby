# frozen_string_literal: true

require "openssl"
require "securerandom"

module ConnectRPC
  module Hyper
    # Validates TLS client inputs as core NetHTTP's private ClientTLS does:
    # an appended `ca_file`, a `certificate_chain` Array of PEM strings (leaf
    # first) and a `private_key` PEM string. Rust receives DER.
    module TLS
      CERTIFICATE = %r{\A-----BEGIN CERTIFICATE-----\s+[A-Za-z0-9+/=\s]+-----END CERTIFICATE-----\z}
      PRIVATE_KEY = %r{
        \A-----BEGIN\ (RSA\ PRIVATE\ KEY|EC\ PRIVATE\ KEY|PRIVATE\ KEY)-----
        \s+[A-Za-z0-9+/=\s]+-----END\ \1-----\z
      }x
      CERTIFICATE_BLOCK = /
        -----BEGIN\ (?:X509\ |TRUSTED\ )?CERTIFICATE-----.*?-----END\ (?:X509\ |TRUSTED\ )?CERTIFICATE-----
      /mx

      # Names the input that failed validation, never its value.
      class InvalidInput < ArgumentError
        attr_reader :input

        def initialize(input)
          @input = input
          super("Invalid #{input}")
        end
      end

      def self.certificate(pem)
        raise ArgumentError unless pem.is_a?(String) && CERTIFICATE.match?(pem.strip)

        certificates = OpenSSL::X509::Certificate.load(pem.dup)
        raise ArgumentError unless certificates.size == 1

        certificate = certificates.first
        raise ArgumentError unless certificate.not_before <= Time.now && certificate.not_after > Time.now

        certificate
      end

      def self.input(name)
        yield
      rescue InvalidInput
        raise
      rescue StandardError
        raise InvalidInput.new(name), cause: nil
      end

      def self.identity(chain, pem) # rubocop:disable Metrics/MethodLength, Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
        certificates = input(:certificate_chain) do
          raise ArgumentError unless chain.is_a?(Array) && !chain.empty?

          chain.map { |element| certificate(element) }
        end
        key = input(:private_key) do
          raise ArgumentError unless pem.is_a?(String) && PRIVATE_KEY.match?(pem.strip)

          OpenSSL::PKey.read(pem.dup) { nil }
        end
        input(:private_key) do
          digest = %w[ED25519 ED448].include?(key.oid) ? nil : "SHA256"
          challenge = SecureRandom.random_bytes(32)
          signature = key.sign(digest, challenge)
          raise ArgumentError unless certificates.first.public_key.verify(digest, signature, challenge)
        end
        input(:certificate_chain) do
          certificates.each_cons(2) do |child, issuer|
            raise ArgumentError unless child.issuer == issuer.subject && child.verify(issuer.public_key)
          end
        end
        [certificates, key]
      end

      # NetHTTP's check: the file must load into an OpenSSL store. The
      # certificates OpenSSL would trust are appended (trust settings of a
      # TRUSTED CERTIFICATE block are not kept); CRLs and keys are skipped.
      def self.ca_file(path)
        input(:ca_file) do
          OpenSSL::X509::Store.new.add_file(path)
          File.binread(path).scan(CERTIFICATE_BLOCK).map do |block|
            OpenSSL::X509::Certificate.new(block.gsub(/(BEGIN|END) (?:X509 |TRUSTED )/, '\1 ')).to_der
          end
        end
      end

      # [chain DERs, key PKCS#8 DER], or [nil, nil] without an identity. As in
      # NetHTTP, a false option counts as given but adds nothing.
      def self.client_identity(chain, pem)
        return [nil, nil] unless chain || pem

        certificates, key = identity(chain, pem)
        [certificates.map(&:to_der), key.private_to_der]
      end

      # [ca_file DERs, chain DERs, key PKCS#8 DER], or nil without TLS options.
      # Errors name only the input: never paths or key material, and no cause.
      def self.inputs(ca_file:, certificate_chain:, private_key:)
        return if [ca_file, certificate_chain, private_key].all?(&:nil?)

        [ca_file ? self.ca_file(ca_file) : [], *client_identity(certificate_chain, private_key)]
      rescue InvalidInput => e
        raise ArgumentError, "Invalid TLS client configuration: #{e.input}", cause: nil
      rescue StandardError
        raise ArgumentError, "Invalid TLS client configuration", cause: nil
      end
    end
  end
end
