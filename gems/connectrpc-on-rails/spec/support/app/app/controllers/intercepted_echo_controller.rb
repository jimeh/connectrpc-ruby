# frozen_string_literal: true

class InterceptedEchoController < EchoController
  private

  def connect_interceptors = request.get_header("connectrpc.test.interceptors") || []
end
