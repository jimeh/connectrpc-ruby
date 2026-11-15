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
    # Every file comes from one issued set, so a renewal cannot mix old and new.
    def host_tls
      set = Certs.current
      { certificate_chain: [set.read(:server)], private_key: set.read(:server_key), client_ca: [set.read(:ca)] }
    end

    # The `tls_provider:` option of both hosts, for certificates that rotate
    # without a restart. The host calls it at startup and then every
    # `refresh_interval` seconds, and expects host_tls plus a `version`.
    #
    # - A new version that passes validation makes new connections use the new
    #   certificates.
    # - The same version keeps the current certificates and open connections.
    # - If the call raises or returns invalid certificates, the host keeps the
    #   last good set, reports the failure to `on_tls_error:` and tries again at
    #   the next refresh.
    #
    # Connections that are already open keep the certificate they started
    # with. `host.drain_tls(grace:)` retires them after a rotation: calls already
    # running get `grace` seconds to finish, and later calls on those
    # connections are refused. on_read receives each new version as it is
    # read, before the host validates it.
    def host_tls_provider(&on_read)
      current = nil
      lambda do
        tls = host_tls
        version = Digest::SHA256.hexdigest(tls.values.join)[0, 16]
        on_read&.call(version) unless version == current
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
      set = Certs.current
      { ca_file: set.path(:ca), certificate_chain: [set.read(:client)], private_key: set.read(:client_key) }
    end
  end
end
