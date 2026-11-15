# frozen_string_literal: true

require_relative "certs"

module DemoExample
  # Mutual TLS: each side presents a certificate and verifies the other's
  # against a CA it trusts. The server proves it is 127.0.0.1, and the client
  # proves it is demo-client. Both sides here trust one throwaway demo CA (see
  # certs.rb); in production each side names the CAs it trusts for the other.
  module MTLS
    module_function

    # The `tls:` option of ConnectRPC::Async::Host and ConnectRPC::Falcon::Server.
    # The values are PEM strings, not paths.
    #
    # - certificate_chain and private_key are the server's identity. The chain
    #   lists the leaf first, then any intermediates.
    # - client_ca lists the CAs whose client certificates the server accepts.
    #   Any non-empty list makes a client certificate mandatory, and the TLS
    #   handshake fails without one. `client_ca: nil` would mean server-only TLS.
    def host_tls
      { certificate_chain: [Certs.read(:server)], private_key: Certs.read(:server_key),
        client_ca: [Certs.read(:ca)] }
    end

    # Keyword options for ConnectRPC::Async::Transport and HTTP1Transport.
    #
    # - ca_file is a path to extra CAs the client trusts. The client always
    #   verifies the server's certificate and hostname (127.0.0.1 here) against
    #   the system roots plus this file.
    # - certificate_chain and private_key are the client's identity, as PEM
    #   strings. Pass both or neither.
    def client_tls
      { ca_file: Certs.path(:ca), certificate_chain: [Certs.read(:client)], private_key: Certs.read(:client_key) }
    end
  end
end
