# frozen_string_literal: true

require "connectrpc"
require "connectrpc/bindings/google_protobuf"
require "service_connect"
RbsFixture::Api::Single.handlers(exchange: lambda { |_request, _context|
  ConnectRPC::Response.new(message: RbsFixture::Messages::Envelope::Reply.new,
                           metadata: ConnectRPC::Metadata.new, trailers: ConnectRPC::Metadata.new)
})
