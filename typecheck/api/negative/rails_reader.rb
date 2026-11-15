# frozen_string_literal: true

class BadController
  include ConnectRPC::OnRails::Controller

  def fail_reader
    connect_context.check!
  end
end
