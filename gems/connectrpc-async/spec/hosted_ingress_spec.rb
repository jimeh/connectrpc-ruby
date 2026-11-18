# frozen_string_literal: true

require_relative "support/ingress_host_examples"
require_relative "support/response_credit_examples"

RSpec.describe "Standalone native request ingress" do
  def with_ingress_host(app, tls: nil, **)
    # Barrier PINGs outpace grpc-go's enforcement without server writes between them.
    host = ConnectRPC::Async::Host.new(server: server, tls: tls,
                                       keepalive: ConnectRPC::Async::ServerKeepalive.new(min_time: nil), **).start
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
  include_examples "hosted HTTP/2 shared response credit"
end
