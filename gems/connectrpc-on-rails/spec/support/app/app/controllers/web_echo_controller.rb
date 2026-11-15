# frozen_string_literal: true

class WebEchoController < EchoController
  private

  def connect_protocol = %i[connect grpc_web]
  def connect_status_codec = connect_binding

  def connect_cors
    ConnectRPC::GRPCWebCORS.new(origins: ["https://browser.example"], credentials: true,
                                request_headers: %w[authorization x-csrf-token], expose_headers: ["x-leading"])
  end
end
