# frozen_string_literal: true

require "connectrpc"
require "connectrpc/bindings/google_protobuf"
require "service_connect"
require_relative "../support"
response = ConnectRPC::Response.new(message: "reply", metadata: ConnectRPC::Metadata.new, trailers: ConnectRPC::Metadata.new)
response[:metadata] = nil
