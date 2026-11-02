# frozen_string_literal: true

require "connectrpc"
require "connectrpc/bindings/google_protobuf"
require "service_connect"
require_relative "../support"
RbsFixture::Api::Echo.handlers(
  exchange: lambda { |request, _context|
    request.count
    RbsFixture::Messages::Envelope::Reply.new
  },
  plain: ->(_request, _context) { RbsPlain::V1::Outer::Reply.new },
  reset: ->(request, _context) { request }
)
