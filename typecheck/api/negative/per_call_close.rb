# frozen_string_literal: true

# Each call closes its own connection; PerCallTransport has no close.
ConnectRPC::Async::PerCallTransport.new.close
