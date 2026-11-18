# frozen_string_literal: true

# Relies on the default connect_binding hook instead of overriding it.
class DefaultBindingController < ActionController::API
  include ConnectRPC::OnRails::Controller

  private

  def connect_service = Example::V1::EchoService::SERVICE
  def connect_handlers = Example::V1::EchoService.handlers(echo: method(:rpc_echo), fail: method(:rpc_echo))
  def rpc_echo(message, _context) = message
end
