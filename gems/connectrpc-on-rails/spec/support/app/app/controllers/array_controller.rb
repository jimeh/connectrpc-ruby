# frozen_string_literal: true

class ArrayController < EchoController
  private

  def connect_service
    type = Google::Protobuf::ListValue
    rpc = ConnectRPC::Method.new(service: "example.Array", name: "Echo", request_type: type, response_type: type)
    ConnectRPC::Service.new(name: rpc.service, methods: [rpc])
  end

  def connect_handlers = { "/example.Array/Echo" => method(:rpc_array) }
  def rpc_array(message, _context) = message
end
