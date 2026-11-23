# frozen_string_literal: true

# Only :environment, a proxy URL or nil select routing.
ConnectRPC::Async::ThreadedTransport.new(origin: "http://example.invalid", proxy: :direct)
