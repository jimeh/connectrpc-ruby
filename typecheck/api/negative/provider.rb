# frozen_string_literal: true

ConnectRPC::Async::TLSConfiguration.new(tls_provider: BadAPIProvider.new)
