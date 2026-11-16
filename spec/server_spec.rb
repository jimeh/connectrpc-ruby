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

RSpec.describe "Nil server responses" do
  # Encodes nil as an empty message, so only core can reject a nil response.
  let(:binding) do
    Object.new.tap do |codec|
      codec.define_singleton_method(:encode) { |message, **| message.to_s.b }
      codec.define_singleton_method(:decode) { |bytes, **| bytes }
    end
  end
  let(:declarations) do
    [ConnectRPC::Method, ConnectRPC::ServerStreamMethod, ConnectRPC::ClientStreamMethod].to_h do |kind|
      [kind, kind.new(service: "nil.Test", name: kind.name.split("::").last, request_type: String,
                      response_type: String)]
    end
  end
  let(:handlers) do
    { ConnectRPC::Method => ->(_request, _context) {},
      ConnectRPC::ServerStreamMethod => ->(_request, writer, _context) { writer.send_message(nil) },
      ConnectRPC::ClientStreamMethod => lambda { |reader, _context|
        reader.each { nil }
        nil
      } }
  end
  let(:server) do
    require "connectrpc/bindings/google_protobuf"
    ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: "nil.Test", methods: declarations.values)],
                           binding: binding, protocol: %i[connect grpc grpc_web],
                           status_codec: ConnectRPC::Bindings::GoogleProtobuf.new,
                           handlers: declarations.to_h { |kind, rpc| [rpc.path, handlers.fetch(kind)] })
  end

  def frame(bytes, flag = 0) = [flag, bytes.bytesize].pack("CN") + bytes

  def native_input(bytes)
    StringIO.new(bytes).tap { |source| source.define_singleton_method(:read) { |length, **| super(length) } }
  end

  def native_output(written, finished)
    Object.new.tap do |output|
      output.define_singleton_method(:write) { |chunk, **| written << chunk }
      output.define_singleton_method(:finish) { |headers:| finished.merge!(headers) }
      output.define_singleton_method(:abort) { nil }
    end
  end

  def terminal_frame(bytes)
    offset = 0
    loop do
      flag, length = bytes.byteslice(offset, 5).unpack("CN")
      return bytes.byteslice(offset + 5, length) if flag.anybits?(2) || flag.anybits?(128)

      offset += 5 + length
    end
  end

  it "rejects a nil Connect unary handler result as internal" do
    response = server.call(path: declarations[ConnectRPC::Method].path, http_method: "POST",
                           headers: { "content-type" => ["application/proto"] }, input: StringIO.new("hello"))
    expect(response.status).to eq(500)
    expect(JSON.parse(response.body)).to include("code" => "internal", "message" => "Nil response message")
  end

  [ConnectRPC::ServerStreamMethod, ConnectRPC::ClientStreamMethod].each do |kind|
    it "ends a Connect #{kind.name.split('::').last} with internal for a nil response" do
      written = +""
      server.call_stream(path: declarations[kind].path, http_method: "POST",
                         headers: { "content-type" => ["application/connect+proto"] },
                         input: native_input(frame("hello")), output: native_output(written, {}),
                         publish: ->(_) {}, http2: true)
      error = JSON.parse(terminal_frame(written)).fetch("error")
      expect(error).to include("code" => "internal", "message" => "Nil response message")
    end
  end

  it "ends a native gRPC unary call with status 13 for a nil response" do
    finished = {}
    server.call_grpc(path: declarations[ConnectRPC::Method].path, http_method: "POST",
                     headers: { "content-type" => ["application/grpc"] }, input: native_input(frame("hello")),
                     output: native_output(+"", finished), publish: ->(_) {}, http2: true)
    expect(finished).to include("grpc-status" => ["13"], "grpc-message" => ["Nil response message"])
  end

  it "ends a gRPC-Web unary call with status 13 for a nil response" do
    response = server.call(path: declarations[ConnectRPC::Method].path, http_method: "POST",
                           headers: { "content-type" => ["application/grpc-web"] }, input: StringIO.new(frame("hello")))
    expect(terminal_frame(response.body)).to include("grpc-status: 13\r\n", "grpc-message: Nil response message\r\n")
  end
end
