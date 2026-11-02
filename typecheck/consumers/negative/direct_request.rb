# frozen_string_literal: true

require "connectrpc"
require "connectrpc/bindings/google_protobuf"
require "service_connect"
require_relative "../support"
client = ConnectRPC::Client.new(base_url: "https://rbs.invalid", binding: ConnectRPC::Bindings::GoogleProtobuf.new)
client.call(RbsFixture::Api::Echo::EXCHANGE, RbsFixture::Messages::Envelope::Reply.new)
