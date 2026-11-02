# frozen_string_literal: true

require "connectrpc/rack"
require "rack/lint"
require "rack/mock"
require "stringio"
require_relative "../../../examples/echo_schema"

RSpec.describe ConnectRPC::Rack do
  let(:binding) { EchoExample::BINDING }
  let(:method) { EchoExample::ECHO }
  let(:message) { EchoExample::Message.new(name: "hello") }
  let(:handler) do
    lambda do |request, context|
      context.response_metadata["x-request"] = context.request_metadata["x-request"] || ["absent"]
      context.response_metadata["x-repeat"] = ["one,two", "three"]
      context.response_trailers["x-tail"] = %w[four five]
      request
    end
  end
  let(:server) do
    ConnectRPC::Server.new(services: [EchoExample::SERVICE], binding: binding, handlers: { method.path => handler })
  end
  let(:app) { described_class.new(->(_) { [202, { "content-type" => "text/plain" }, ["downstream"]] }, server: server) }

  def request(body: nil, codec: :proto, **env)
    body ||= binding.encode(message, type: EchoExample::Message, codec: codec)
    environment = Rack::MockRequest.env_for(method.path, method: "POST", input: body,
                                                         "CONTENT_TYPE" => ConnectRPC::Protocol::CONTENT_TYPES.fetch(codec))
    environment.merge!(env.transform_keys(&:to_s))
    status, headers, chunks = Rack::Lint.new(app).call(environment)
    bytes = +""
    begin
      chunks.each { |chunk| bytes << chunk }
    ensure
      chunks.close if chunks.respond_to?(:close)
    end
    [status, headers, bytes]
  end

  it "loads middleware exclusively from the optional package" do
    owner = Gem.loaded_specs.fetch("connectrpc-rack")
    expect(owner.full_gem_path).to eq(File.expand_path("..", __dir__))
    source = File.join(owner.full_gem_path, "lib/connectrpc/rack.rb")
    expect(described_class.instance_method(:call).source_location.first).to eq(source)
    expect($LOADED_FEATURES.grep(%r{/connectrpc/rack\.rb$})).to eq([source])
    expect(File).not_to exist(File.expand_path("../../../lib/connectrpc/rack.rb", __dir__))
  end

  it "returns the default downstream 404 for an unrelated path" do
    status, headers, body = described_class.new(server: server).call("PATH_INFO" => "/unrelated")
    expect([status, headers, body]).to eq([404, { "content-type" => "text/plain" }, ["Not found"]])
  end

  %i[proto json].each do |codec|
    it "serves typed #{codec} with separate repeated metadata arrays under Rack::Lint" do
      status, headers, body = request(codec: codec, HTTP_X_REQUEST: "client")
      expect(status).to eq(200)
      expect(binding.decode(body, type: EchoExample::Message, codec: codec)).to eq(message)
      expect(headers).to include("x-repeat" => ["one,two", "three"], "trailer-x-tail" => %w[four five],
                                 "x-request" => ["client"])
    end
  end

  it "routes mounted PATH_INFO and falls through only for unrelated services" do
    expect(request(PATH_INFO: "/unrelated").first).to eq(202)
    expect(request(SCRIPT_NAME: "/rpc").first).to eq(200)
    expect(request(PATH_INFO: "/example.v1.EchoService/Missing").first).to eq(501)
    expect(request(PATH_INFO: "/example.v1.EchoService").first).to eq(501)
  end

  it "uses a bare 405 with Allow for unsupported verbs" do
    status, headers, body = request(REQUEST_METHOD: "GET", CONTENT_LENGTH: "0")
    expect([status, headers["allow"], body]).to eq([405, ["POST"], ""])
  end

  ["", "text/plain", "application/connect+proto", "application/grpc", "application/grpc-web+proto",
   "application/json; charset=latin1"].each do |type|
    it "rejects #{type.inspect} with bare 415 and Accept-Post" do
      status, headers, body = request(CONTENT_TYPE: type)
      expect(status).to eq(415)
      expect(headers["accept-post"]).to eq(["application/proto, application/json"])
      expect(body).to eq("")
    end
  end

  it "accepts JSON utf-8 charset and optional protocol version" do
    expect(request(codec: :json, CONTENT_TYPE: "application/json; charset=utf-8",
                   HTTP_CONNECT_PROTOCOL_VERSION: "1").first).to eq(200)
  end

  [{ HTTP_CONNECT_PROTOCOL_VERSION: "2" }, { HTTP_CONNECT_TIMEOUT_MS: "0" }, { HTTP_CONNECT_TIMEOUT_MS: "-1" },
   { HTTP_CONNECT_TIMEOUT_MS: "1.0" }, { HTTP_CONNECT_TIMEOUT_MS: "10000000000" }].each do |env|
    it "rejects invalid protocol headers #{env}" do
      status, _headers, body = request(**env)
      expect(status).to eq(400)
      expect(JSON.parse(body)["code"]).to eq("invalid_argument")
    end
  end

  it "rejects unsupported compression with a safe supported-encoding message" do
    status, _headers, body = request(HTTP_CONTENT_ENCODING: "gzip")
    expect(status).to eq(501)
    expect(JSON.parse(body)).to include("code" => "unimplemented", "message" => /identity/)
  end

  %w[Identity IDENTITY].each do |encoding|
    it "accepts case-insensitive #{encoding} request encoding" do
      expect(request(HTTP_CONTENT_ENCODING: encoding).first).to eq(200)
    end
  end

  it "filters HTTP fields outside the metadata contract while preserving adjacent valid fields" do
    allow(handler).to receive(:call) do |value, context|
      expect(context.request_metadata.to_h).to include("x-request" => ["client"], "x-valid" => ["one,two"])
      %w[user-agent x-tab legal! x-injection x-null].each do |key|
        expect(context.request_metadata[key]).to be_nil
      end
      value
    end
    status, = request(HTTP_USER_AGENT: "caf\xff".b, HTTP_X_TAB: "a\tb", "HTTP_LEGAL!" => "value",
                      HTTP_X_REQUEST: "client", HTTP_X_VALID: "one,two",
                      HTTP_X_INJECTION: "bad\r\nfield: value", HTTP_X_NULL: "bad\x00")
    expect(status).to eq(200)
  end

  [[:proto, "\xff".b], [:json, "{"]].each do |codec, body|
    it "returns invalid_argument for malformed #{codec}" do
      status, _headers, bytes = request(codec: codec, body: body)
      expect(status).to eq(400)
      expect(JSON.parse(bytes)["code"]).to eq("invalid_argument")
    end
  end

  it "returns safe generic internal errors for unexpected application exceptions" do
    [RuntimeError, ArgumentError, ConnectRPC::CodecError].each do |exception|
      allow(handler).to receive(:call).and_raise(exception, "secret database password")
      status, _headers, body = request
      expect(status).to eq(500)
      expect(JSON.parse(body)).to include("code" => "internal", "message" => "Internal error")
      expect(body).not_to include("secret")
    end
  end

  it "preserves deliberate typed errors, metadata and trailers" do
    detail = ConnectRPC::ErrorDetail.new(type: "example.v1.EchoMessage", value: message.to_proto)
    error = ConnectRPC::Error.new(:permission_denied, "deliberate", details: [detail],
                                                                    metadata: { "x-error" => %w[a b] },
                                                                    trailers: { "x-end" => %w[c d] })
    allow(handler).to receive(:call).and_raise(error)
    status, headers, body = request
    expect(status).to eq(403)
    expect(headers).to include("x-error" => %w[a b], "trailer-x-end" => %w[c d])
    expect(JSON.parse(body)).to include("code" => "permission_denied", "message" => "deliberate",
                                        "details" => [detail.to_h])
  end

  it "creates independent contexts for a reusable handler" do
    expect(request(HTTP_X_REQUEST: "first")[1]["x-request"]).to eq(["first"])
    expect(request[1]["x-request"]).to eq(["absent"])
  end

  it "exposes cooperative expiry and checks after handlers finish" do
    now = 0.0
    allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { now }
    allow(handler).to receive(:call) do |request, context|
      expect(context.cancelled?).to be(false)
      now = 0.02
      expect(context.cancelled?).to be(true)
      request
    end
    status, _headers, body = request(HTTP_CONNECT_TIMEOUT_MS: "10")
    expect(status).to eq(504)
    expect(JSON.parse(body)["code"]).to eq("deadline_exceeded")
  end
end
