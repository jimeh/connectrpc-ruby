# frozen_string_literal: true

require "connectrpc"
require "async"
require "async/queue"
require "async/condition"
require "async/pool/controller"
require "async/http/protocol/http2"
require "protocol/http/request"
require "protocol/http/response"
require "io/endpoint/host_endpoint"
require "io/endpoint/bound_endpoint"
require "openssl"

module ConnectRPC
  # Explicit, optional integration. The application owns the Async reactor.
  module Async
    class LifecycleError < StandardError; end
    private_constant :LifecycleError
  end
end

require_relative "async/internal"
require_relative "async/tls"
require_relative "async/connection"
require_relative "async/host"
require_relative "async/transport"
