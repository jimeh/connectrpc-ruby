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

  it "advertises the configured receive windows on native HTTP/2 connections" do
    reactor do |task|
      falcon(rack_app: ->(_env) { [200, {}, ["ok"]] }, stream_receive_window: 131_072,
             connection_receive_window: 262_144) do |_host, origin|
        with_ingress_client(task, origin, :h2c) do |connection, wire|
          expect(connection.remote_settings.initial_window_size).to eq(131_072)
          expect(wire[:credit][0]).to eq(262_144 - 65_535)
        end
      end
    end
  end
end
