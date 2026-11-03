# frozen_string_literal: true

require "connectrpc"
require "connectrpc/bindings/google_protobuf"
require "streaming_connect"
require_relative "../support"
core = ConnectRPC::Client.new(base_url: "https://rbs.invalid", binding: ConnectRPC::Bindings::GoogleProtobuf.new)
client = RbsFixture::Streaming::Mixed::Client.new(client: core)
client.watch(RbsFixture::Messages::Envelope::Reply.new) { nil }
