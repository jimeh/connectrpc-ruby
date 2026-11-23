# frozen_string_literal: true

# Only :environment, a proxy URL or nil select routing.
ConnectRPC::Async::Transport.new(origin: "http://example.invalid", proxy: :direct)
