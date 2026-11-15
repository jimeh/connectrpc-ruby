# frozen_string_literal: true

require "digest"
require_relative "certs"

module DemoExample
  # Mutual TLS: each side presents a certificate and verifies the other's
  # against a CA it trusts. The server proves it is 127.0.0.1, and the client
  # proves it is demo-client. Both sides here trust one throwaway demo CA (see
  # certs.rb); in production each side names the CAs it trusts for the other.
  module MTLS
    module_function

    # The server's TLS settings. The values are PEM strings, not paths.
    #
    # - certificate_chain and private_key are the server's identity. The chain
    #   lists the leaf first, then any intermediates.
    # - client_ca lists the CAs whose client certificates the server accepts.
    #   Any non-empty list makes a client certificate mandatory, and the TLS
    #   handshake fails without one. `client_ca: nil` would mean server-only TLS.
    #
    # Passed as `tls:`, ConnectRPC::Async::Host and ConnectRPC::Falcon::Server
    # load it once at startup. host_tls_provider below reloads it instead.
    def host_tls
      { certificate_chain: [Certs.read(:server)], private_key: Certs.read(:server_key),
        client_ca: [Certs.read(:ca)] }
    end

    # The `tls_provider:` option of both hosts, for certificates that rotate
    # without a restart. The host calls it at startup and then every
    # `refresh_interval` seconds, and expects host_tls plus a `version`.
    #
    # - A new version makes new connections use the new certificates.
    # - The same version keeps the current ones, so an unchanged file is cheap.
    # - If the call raises or returns invalid certificates, the host keeps the
    #   last good set, logs the failure and tries again at the next refresh.
    #
    # Connections that are already open keep the certificate they started
    # with. `host.drain_tls(grace:)` retires them after a rotation: calls already
    # running finish, and later calls on those connections are refused.
    # on_change receives each new version.
    def host_tls_provider(&on_change)
      current = nil
      lambda do
        tls = host_tls
        version = Digest::SHA256.hexdigest(tls.values.join)[0, 16]
        on_change&.call(version) unless version == current
        current = version
        { version: version, **tls }
      end
    end

    # Keyword options for ConnectRPC::Async::Transport and HTTP1Transport.
    #
    # - ca_file is a path to extra CAs the client trusts. The client always
    #   verifies the server's certificate and hostname (127.0.0.1 here) against
    #   the system roots plus this file.
    # - certificate_chain and private_key are the client's identity, as PEM
    #   strings. Pass both or neither.
    #
    # A transport keeps the identity and trust it was built with. To rotate a
    # short-lived client certificate, build a new transport and client from the
    # renewed files, then close the old transport once its calls finish. These
    # example clients make one call per run, so each run reads the current files.
    def client_tls
      { ca_file: Certs.path(:ca), certificate_chain: [Certs.read(:client)], private_key: Certs.read(:client_key) }
    end
  end
end
