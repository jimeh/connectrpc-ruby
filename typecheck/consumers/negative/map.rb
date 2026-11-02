# frozen_string_literal: true

require "connectrpc"
require "connectrpc/bindings/google_protobuf"
require "service_connect"
require_relative "../support"
RbsFixture::Messages::Envelope::Request.new.counts["x"] = "wrong"
