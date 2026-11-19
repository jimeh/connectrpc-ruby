# frozen_string_literal: true

# connect_timeout must be finite positive seconds; nil is rejected.
ConnectRPC::Hyper::Transport.new(connect_timeout: nil)
