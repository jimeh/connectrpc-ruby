# frozen_string_literal: true

class ScalarController < EchoController
  private

  def connect_service
    type = Google::Protobuf::StringValue
    method = ConnectRPC::Method.new(service: "example.Scalar", name: "Echo", request_type: type, response_type: type)
    ConnectRPC::Service.new(name: method.service, methods: [method])
  end

  def connect_handlers = { "/example.Scalar/Echo" => method(:rpc_scalar) }
  def rpc_scalar(message, _context) = message
end
