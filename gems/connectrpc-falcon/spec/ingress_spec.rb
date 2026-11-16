# frozen_string_literal: true

require_relative "../../connectrpc-async/spec/support/ingress_host_examples"

RSpec.describe "Falcon native HTTP/2 ordinary Rack request ingress" do
  def with_ingress_host(app, tls: nil, &)
    rack = lambda do |env|
      body = env.fetch("protocol.http.request").body
      app.call(body, -> { env.fetch("rack.input").read(16_384) }, env.fetch("PATH_INFO"))
    end
    # Barrier PINGs outpace grpc-go's enforcement without server writes between them.
    falcon(rack_app: rack, tls: tls, scheme: tls ? "https" : "http",
           keepalive: ConnectRPC::Async::ServerKeepalive.new(min_time: nil), &)
  end

  include_examples "hosted HTTP/2 ingress backpressure"
end
