# frozen_string_literal: true

require "connectrpc"
require "connectrpc/bindings/google_protobuf"
require "streaming_connect"
require_relative "../support"
core = ConnectRPC::Client.new(base_url: "https://rbs.invalid", binding: ConnectRPC::Bindings::GoogleProtobuf.new)
RbsFixture::Streaming::Mixed::Client.new(client: core)
# @type var unary: ConnectRPC::Method[RbsFixture::Messages::Envelope::Request, RbsFixture::Messages::Envelope::Reply]
unary = RbsFixture::Streaming::Mixed::WATCH
unary.name
