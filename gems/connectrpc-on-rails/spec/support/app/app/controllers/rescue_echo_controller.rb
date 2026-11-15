# frozen_string_literal: true

class RescueEchoController < EchoController
  rescue_from StandardError do |_error|
    render plain: "catch all private", status: :internal_server_error
  end
  # Native Rails precedence: declare this after the application's catch-all.
  rescue_from ConnectRPC::Error do |error|
    raise error
  end
end
