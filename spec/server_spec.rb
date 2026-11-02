# frozen_string_literal: true

require "stringio"
require_relative "../examples/echo_schema"

RSpec.describe ConnectRPC::Server do
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

  [["identity", "identity"], ["identity, gzip"], ["identity, identity"]].each do |encodings|
    it "rejects multiple request encodings #{encodings}" do
      response = server.call(path: method.path, http_method: "POST", input: StringIO.new(message.to_proto),
                             headers: { "content-type" => ["application/proto"], "content-encoding" => encodings })
      expect(response.status).to eq(501)
      expect(JSON.parse(response.body)["code"]).to eq("unimplemented")
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
end
