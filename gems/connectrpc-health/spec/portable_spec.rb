# frozen_string_literal: true

require "net/http"
require "rack/mock_request"
require_relative "../../../examples/localhost"

RSpec.describe "Health on portable unary adapters" do
  let(:health) { ConnectRPC::Health::Registry.new }
  let(:request) { HealthHelpers::REQUEST }
  let(:watch_path) { HealthHelpers::HEALTH::WATCH.path }

  def rack_post(app, path, content_type, body)
    env = Rack::MockRequest.env_for(path, method: "POST", input: body, "CONTENT_TYPE" => content_type)
    status, headers, response = app.call(env)
    bytes = +""
    response.each { |chunk| bytes << chunk }
    response.close if response.respond_to?(:close)
    [status, headers, bytes]
  end

  it "serves Connect Check and List through Rack to the NetHTTP client" do
    health.set("demo.v1.Demo", :not_serving)
    host = EchoExample::Localhost.new(ConnectRPC::Rack.new(server: health_server(health, protocol: :connect)))
    client = health_client(ConnectRPC::Transports::NetHTTP.new, host.url)
    expect(client.check(request.new(service: "demo.v1.Demo")).message.status).to eq(:NOT_SERVING)
    expect { client.check(request.new(service: "missing")) }
      .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("not_found") }
    expect(client.list(Grpc::Health::V1::HealthListRequest.new).message.statuses.keys.sort)
      .to eq(["", "demo.v1.Demo"])
    expect { client.watch(request.new(service: "")) { |reader| reader.each { |_| nil } } }
      .to raise_error(ArgumentError, "Transport does not support server_stream")
    expect(health.watcher_count).to eq(0)
  ensure
    host&.close
  end

  it "rejects Watch by protocol in Rack before dispatching the handler" do
    app = ConnectRPC::Rack.new(server: health_server(health, protocol: :connect))
    message = request.new(service: "").to_proto
    status, = rack_post(app, watch_path, "application/connect+proto", [0, message.bytesize].pack("CN") + message)
    expect(status).to eq(415)
    status, _headers, body = rack_post(app, watch_path, "application/proto", message)
    expect(status).to eq(501)
    expect(JSON.parse(body).fetch("code")).to eq("unimplemented")
    expect(health.watcher_count).to eq(0)
  end

  it "serves Check in process and fails a generated Watch locally through Testing" do
    transport = ConnectRPC::Testing::Transport.new(server: health_server(health, protocol: :connect))
    client = health_client(transport, "https://example.invalid")
    expect(client.check(request.new(service: "")).message.status).to eq(:SERVING)
    expect { client.watch(request.new(service: "")) { |reader| reader.each { |_| nil } } }
      .to raise_error(ArgumentError, "Transport does not support server_stream")
    expect(health.watcher_count).to eq(0)
  end
end
