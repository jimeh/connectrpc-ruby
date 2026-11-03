# frozen_string_literal: true

class BaseWebEchoController < BaseEchoController
  private

  def connect_protocol = :grpc_web
  def connect_status_codec = connect_binding

  def connect_cors
    ConnectRPC::GRPCWebCORS.new(origins: ["http://www.example.com"], credentials: true,
                                request_headers: ["x-csrf-token"])
  end
end
