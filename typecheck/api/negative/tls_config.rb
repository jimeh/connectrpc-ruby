# frozen_string_literal: true

ConnectRPC::Async::TLSConfiguration.new(tls: { certificate_chain: 1, private_key: "key", client_ca: nil })
