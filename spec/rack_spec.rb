# frozen_string_literal: true

require "connectrpc/rack"
require "rack/lint"
require "rack/mock"
require "stringio"
require_relative "../examples/echo_schema"

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

  [[:proto, "\xff".b], [:json, "{"]].each do |codec, body|
    it "returns invalid_argument for malformed #{codec}" do
      status, _headers, bytes = request(codec: codec, body: body)
      expect(status).to eq(400)
      expect(JSON.parse(bytes)["code"]).to eq("invalid_argument")
    end
  end

  it "bounds unknown-length input before decoding and never reads it without a size" do
    limited = ConnectRPC::Server.new(services: [EchoExample::SERVICE], binding: binding,
                                     handlers: { method.path => handler }, request_limit: 3)
    input = StringIO.new("a" * 100)
    expect(binding).not_to receive(:decode)
    expect(input).to receive(:read).with(4).and_call_original
    result = limited.call(path: method.path, http_method: "POST",
                          headers: { "content-type" => ["application/proto"] },
                          input: input)
    expect(result.status).to eq(429)
    expect(input.pos).to eq(4)
  end

  it "rejects oversized Content-Length before reading and malformed/overlong lengths" do
    expect(binding).not_to receive(:decode)
    ["-1", "abc", "1, 1", "9" * 21, "5000000"].each do |length|
      input = double("input")
      expect(input).not_to receive(:read)
      result = server.call(path: method.path, http_method: "POST",
                           headers: { "content-type" => ["application/proto"],
                                      "content-length" => [length] },
                           input: input)
      expect(result.status).to eq(length == "5000000" ? 429 : 400)
    end
  end

  it "treats missing input as empty and rejects a truncated declared body" do
    response = server.call(path: method.path, http_method: "POST",
                           headers: { "content-type" => ["application/proto"] }, input: nil)
    expect(response.status).to eq(200)
    response = server.call(path: method.path, http_method: "POST",
                           headers: { "content-type" => ["application/proto"],
                                      "content-length" => ["2"] }, input: nil)
    expect(response.status).to eq(400)
  end

  it "bounds successful and deliberate error output" do
    limited = ConnectRPC::Server.new(services: [EchoExample::SERVICE], binding: binding,
                                     handlers: { method.path => handler }, response_limit: 1)
    response = limited.call(path: method.path, http_method: "POST",
                            headers: { "content-type" => ["application/proto"] },
                            input: StringIO.new(message.to_proto))
    expect(response.status).to eq(429)
    expect(response.body.bytesize).to be <= 1
  end

  it "bounds a deliberate oversized error body" do
    allow(handler).to receive(:call).and_raise(ConnectRPC::Error.new(:invalid_argument, "x" * 1000))
    limited = ConnectRPC::Server.new(services: [EchoExample::SERVICE], binding: binding,
                                     handlers: { method.path => handler }, response_limit: 100)
    response = limited.call(path: method.path, http_method: "POST", input: StringIO.new(message.to_proto),
                            headers: { "content-type" => ["application/proto"] })
    expect(response.status).to eq(429)
    expect(response.body.bytesize).to be <= 100
    expect(JSON.parse(response.body)["code"]).to eq("resource_exhausted")
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

  it "supports a custom invocation boundary without Google or Rack arguments" do
    custom = lambda do |declaration, request, context|
      expect(declaration).to equal(method)
      expect(request).to be_a(EchoExample::Message)
      expect(context).to be_a(ConnectRPC::Context)
      EchoExample::Message.new(name: "invoked")
    end
    custom_server = ConnectRPC::Server.new(services: [EchoExample::SERVICE], binding: binding, invoker: custom)
    response = custom_server.call(path: method.path, http_method: "POST",
                                  headers: { "content-type" => ["application/proto"] },
                                  input: StringIO.new(message.to_proto))
    expect(response.status).to eq(200)
    expect(EchoExample::Message.decode(response.body).name).to eq("invoked")
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
