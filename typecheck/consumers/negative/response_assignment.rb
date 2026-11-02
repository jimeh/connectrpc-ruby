# frozen_string_literal: true

require "connectrpc"
require "connectrpc/bindings/google_protobuf"
require "service_connect"
require_relative "../support"
client = ConnectRPC::Client.new(base_url: "https://rbs.invalid", binding: ConnectRPC::Bindings::GoogleProtobuf.new)
generated = RbsFixture::Api::Echo::Client.new(client: client)
response = generated.exchange(RbsFixture::Messages::Envelope::Request.new)
# @type var wrong: RbsFixture::Messages::Envelope::Request
wrong = response.message
wrong.text
