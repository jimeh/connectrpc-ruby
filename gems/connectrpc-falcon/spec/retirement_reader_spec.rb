# frozen_string_literal: true

require_relative "../../connectrpc-async/spec/support/retirement_reader"

RSpec.describe "Falcon H2 reader ownership" do
  def reader_listener
    endpoint = ConnectRPC::Falcon::Endpoint.parse("http://127.0.0.1:0")
    host = ConnectRPC::Falcon::Server.new(server: server, endpoint: endpoint, idle_timeout: nil)
    [host, host.instance_variable_get(:@owned_bound), nil]
  end

  def start_reader_listener(host, _listener, _session)
    host.run
  end

  include_examples "eager reader retirement"
end
