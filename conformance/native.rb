# frozen_string_literal: true

require "connectrpc/async"
require_relative "schema"
require "tmpdir"

module Conformance
  SERVER_STREAM = ConnectRPC::ServerStreamMethod.new(service: SERVICE_NAME, name: "ServerStream",
                                                     request_type: ServerStreamRequest,
                                                     response_type: ServerStreamResponse)
  CLIENT_STREAM = ConnectRPC::ClientStreamMethod.new(service: SERVICE_NAME, name: "ClientStream",
                                                     request_type: ClientStreamRequest,
                                                     response_type: ClientStreamResponse)
  BIDI_STREAM = ConnectRPC::BidiStreamMethod.new(service: SERVICE_NAME, name: "BidiStream",
                                                 request_type: BidiStreamRequest, response_type: BidiStreamResponse)
  NATIVE_SERVICE = ConnectRPC::Service.new(name: SERVICE_NAME,
                                           methods: [UNARY, IDEMPOTENT_UNARY, UNIMPLEMENTED, SERVER_STREAM,
                                                     CLIENT_STREAM, BIDI_STREAM])

  def self.proto_error(error)
    details = error.details.map do |detail|
      Google::Protobuf::Any.new(type_url: "type.googleapis.com/#{detail.type}", value: detail.value)
    end
    Error.new(code: :"CODE_#{error.code.upcase}", message: error.message, details: details)
  end

  # These sleeps are the official instruction's external protocol delays. They
  # never synchronize readiness or move the original cancellation instruction.
  def self.delay(milliseconds)
    ::Async::Task.current.sleep(milliseconds / 1000.0) if milliseconds.positive?
  end
end
