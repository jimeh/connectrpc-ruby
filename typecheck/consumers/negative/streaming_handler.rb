# frozen_string_literal: true

require "connectrpc"
require "connectrpc/bindings/google_protobuf"
require "streaming_connect"
require_relative "../support"
core = ConnectRPC::Client.new(base_url: "https://rbs.invalid", binding: ConnectRPC::Bindings::GoogleProtobuf.new)
RbsFixture::Streaming::Mixed::Client.new(client: core)
RbsFixture::Streaming::Mixed.handlers(
  lookup: ->(request, _) { RbsFixture::Messages::Envelope::Reply.new(count: request.text.size) },
  watch: ->(_, writer, _) { writer.send_message(RbsFixture::Messages::Envelope::Reply.new) },
  collect: ->(_, _) { RbsFixture::Messages::Envelope::Request.new },
  chat: ->(duplex, _) { duplex.each { |request| duplex.send_message(RbsFixture::Messages::Envelope::Reply.new(count: request.text.size)) } }
)
