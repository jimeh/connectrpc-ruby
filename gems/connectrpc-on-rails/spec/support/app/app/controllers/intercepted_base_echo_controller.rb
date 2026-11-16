# frozen_string_literal: true

class InterceptedBaseEchoController < BaseEchoController
  private

  def connect_interceptors = request.get_header("connectrpc.test.interceptors") || []
end
