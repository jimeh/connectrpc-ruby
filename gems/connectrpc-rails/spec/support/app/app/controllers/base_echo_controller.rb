# frozen_string_literal: true

class BaseEchoController < ActionController::Base
  include ConnectRPC::Rails::Controller

  before_action(only: :connect) { connect_context.response_metadata.add("x-base", "before") }
  after_action(only: :connect) { connect_context.response_trailers.add("x-base", "after") }

  def ordinary
    cookies[:ordinary] = "yes"
    render json: { name: params[:name], token: form_authenticity_token }
  end

  private

  def connect_service = Example::V1::EchoService::SERVICE
  def connect_binding = ConnectRPC::Bindings::GoogleProtobuf.new
  def connect_handlers = Example::V1::EchoService.handlers(echo: method(:rpc_echo), fail: method(:rpc_echo))
  def connect_limits = request.headers["x-gzip"] == "yes" ? { encoding: :gzip } : {}
  def rpc_echo(message, _context) = message
end
