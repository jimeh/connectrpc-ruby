# frozen_string_literal: true

require_relative "support/ingress_host_examples"

RSpec.describe "Standalone native request ingress" do
  def with_ingress_host(app, tls: nil)
    host = ConnectRPC::Async::Host.new(server: server, tls: tls).start
    application = host_session(host).instance_variable_get(:@application)
    application.instance_variable_set(:@fallback, lambda { |request|
      status, headers, body = app.call(request.body, -> { request.body.read }, request.path)
      Protocol::HTTP::Response[status, headers, body]
    })
    yield host, "#{tls ? 'https' : 'http'}://127.0.0.1:#{host.port}"
  ensure
    host&.close(grace: 0)
  end

  include_examples "hosted HTTP/2 ingress backpressure"
end
