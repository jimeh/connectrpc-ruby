# frozen_string_literal: true

require "connectrpc"
require "connectrpc/proxy"
require_relative "hyper/version"
begin
  # Platform gems ship one extension per Ruby minor.
  require_relative "hyper/#{RUBY_VERSION[/\A\d+\.\d+/]}/connectrpc_hyper"
rescue LoadError
  # The source gem compiles one extension at install time.
  require_relative "hyper/connectrpc_hyper"
end

module ConnectRPC
  # Explicit, optional reactor-free client transport. A Rust extension (hyper,
  # h2, rustls, tokio) owns connections, HTTP/2 streams and blocking waits;
  # Ruby owns protocol logic, deadlines, cancellation and error phrases.
  module Hyper
  end
end

require_relative "hyper/errors"
require_relative "hyper/tls"
require_relative "hyper/exchange"
require_relative "hyper/transport"

ConnectRPC::Hyper.send(:private_constant, :Native, :Errors, :TLS, :Exchange)
