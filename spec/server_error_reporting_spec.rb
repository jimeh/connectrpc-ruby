# frozen_string_literal: true

require "stringio"
require_relative "../examples/echo_schema"

RSpec.describe ConnectRPC::Server, "unexpected error reporting" do
  let(:method) { EchoExample::ECHO }
  let(:failure) { RuntimeError.new("secret database password") }
  let(:handler) { ->(_request, _context) { raise failure } }
  let(:reports) { [] }

  def build(**)
    described_class.new(services: [EchoExample::SERVICE], binding: EchoExample::BINDING,
                        handlers: { method.path => handler }, **)
  end

  def post(server, body: "{}", **)
    server.call(path: method.path, http_method: "POST", headers: { "content-type" => ["application/json"] },
                input: StringIO.new(body), **)
  end

  def wire(response) = [response.status, response.headers, response.body]

  it "passes the original exception to on_error and keeps the peer response unchanged" do
    plain = post(build)
    response = post(build(on_error: ->(error) { reports << error }))
    expect(reports.size).to eq(1)
    expect(reports.first).to be(failure)
    expect(wire(response)).to eq(wire(plain))
    expect(JSON.parse(response.body)).to include("code" => "internal", "message" => "Internal error")
  end

  it "does not report deliberate errors or protocol rejections" do
    deliberate = ->(_request, _context) { raise ConnectRPC::Error.new(:not_found, "missing") }
    server = described_class.new(services: [EchoExample::SERVICE], binding: EchoExample::BINDING,
                                 handlers: { method.path => deliberate }, on_error: ->(error) { reports << error })
    expect(post(server).status).to eq(404)
    expect(post(server, body: "{").status).to eq(400)
    expect(reports).to be_empty
  end

  it "reports unexpected interceptor exceptions at the same boundary" do
    interceptor = Object.new
    interceptor.define_singleton_method(:server_unary) { |_call| raise ArgumentError, "interceptor bug" }
    response = post(build(interceptors: [interceptor], on_error: ->(error) { reports << error }))
    expect(response.status).to eq(500)
    expect(reports.map(&:message)).to eq(["interceptor bug"])
  end

  it "runs the server hook, then the host reporter, and ignores reporter failures" do
    order = []
    server = build(on_error: lambda { |error|
      order << [:server, error]
      raise "reporter failed"
    })
    response = post(server, on_error: lambda { |error|
      order << [:host, error]
      raise "host reporter failed"
    })
    expect(order).to eq([[:server, failure], [:host, failure]])
    expect(wire(response)).to eq(wire(post(build)))
  end

  it "reports inside the instrumented call scope" do
    attached = nil
    subscriber = Object.new
    subscriber.define_singleton_method(:start) { |_call| self }
    subscriber.define_singleton_method(:attach) { attached = true }
    subscriber.define_singleton_method(:detach) { |_token| attached = false }
    observed = []
    server = build(instrumentation: ConnectRPC::Instrumentation.new(subscribers: [subscriber]),
                   on_error: ->(_error) { observed << attached })
    expect(post(server).status).to eq(500)
    expect([observed, attached]).to eq([[true], false])
  end

  it "does not report an exception when the deadline outcome wins arbitration" do
    now = 100.0
    allow(Process).to receive(:clock_gettime).and_call_original
    allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { now }
    late = lambda do |_request, _context|
      now += 2
      raise failure
    end
    server = described_class.new(services: [EchoExample::SERVICE], binding: EchoExample::BINDING,
                                 handlers: { method.path => late }, on_error: ->(error) { reports << error })
    response = server.call(path: method.path, http_method: "POST", input: StringIO.new("{}"),
                           headers: { "content-type" => ["application/json"], "connect-timeout-ms" => ["1000"] },
                           on_error: ->(error) { reports << error })
    expect(JSON.parse(response.body)).to include("code" => "deadline_exceeded")
    expect(reports).to be_empty
  end

  it "requires a callable on_error" do
    expect { build(on_error: "log") }.to raise_error(ArgumentError, "on_error must be callable")
  end
end

RSpec.describe ConnectRPC::Server, "unexpected error reporting on gRPC, gRPC-Web and Connect streaming" do
  let(:binding) do
    Object.new.tap do |object|
      object.define_singleton_method(:encode) { |message, **| message.b }
      object.define_singleton_method(:decode) { |bytes, **| bytes }
    end
  end
  let(:status_codec) do
    require "connectrpc/bindings/google_protobuf"
    ConnectRPC::Bindings::GoogleProtobuf.new
  end
  let(:method) do
    ConnectRPC::Method.new(service: "report.Tokens", name: "Call", request_type: :request, response_type: :response)
  end
  let(:failure) { RuntimeError.new("secret database password") }
  let(:reports) { [] }

  def build(protocol, **)
    described_class.new(services: [ConnectRPC::Service.new(name: method.service, methods: [method])],
                        binding: binding, status_codec: status_codec, protocol: protocol,
                        handlers: { method.path => ->(_message, _context) { raise failure } }, **)
  end

  def frame(bytes) = [0, bytes.bytesize].pack("CN") + bytes

  def native_input
    StringIO.new(frame("request")).tap { |input| input.define_singleton_method(:read) { |length, **| super(length) } }
  end

  def web(server, **)
    server.call(path: method.path, http_method: "POST", headers: { "content-type" => ["application/grpc-web"] },
                input: StringIO.new(frame("request")), **)
  end

  it "reports gRPC-Web unary through the Server hook and the host reporter with an unchanged body" do
    plain = web(build(:grpc_web))
    response = web(build(:grpc_web, on_error: ->(error) { reports << [:server, error] }),
                   on_error: ->(error) { reports << [:host, error] })
    expect(reports).to eq([[:server, failure], [:host, failure]])
    expect([response.status, response.headers, response.body]).to eq([plain.status, plain.headers, plain.body])
    expect(response.body).to include("grpc-status: 13\r\n", "grpc-message: Internal error\r\n")
  end

  it "reports native gRPC handler exceptions through the Server hook" do
    output = Object.new
    trailers = []
    output.define_singleton_method(:write) { |_bytes, **| nil }
    output.define_singleton_method(:finish) do |headers:|
      trailers << headers
      nil
    end
    output.define_singleton_method(:abort) { nil }
    server = build(:grpc, on_error: ->(error) { reports << error })
    server.call_grpc(path: method.path, http_method: "POST", headers: { "content-type" => ["application/grpc"] },
                     input: native_input, output: output, publish: ->(_head) {}, http2: true)
    expect(reports).to eq([failure])
    expect(trailers.last).to include("grpc-status" => ["13"], "grpc-message" => ["Internal error"])
  end

  it "reports Connect streaming handler exceptions through the Server hook" do
    stream = ConnectRPC::ServerStreamMethod.new(service: method.service, name: "Watch", request_type: :request,
                                                response_type: :response)
    server = described_class.new(services: [ConnectRPC::Service.new(name: stream.service, methods: [stream])],
                                 binding: binding, on_error: ->(error) { reports << error },
                                 handlers: { stream.path => ->(_request, _writer, _context) { raise failure } })
    written = +""
    output = Object.new
    output.define_singleton_method(:write) { |chunk, **| written << chunk }
    server.call_stream(path: stream.path, http_method: "POST",
                       headers: { "content-type" => ["application/connect+proto"] },
                       input: native_input, output: output, publish: ->(_head) {}, http2: true)
    expect(reports).to eq([failure])
    expect(JSON.parse(written.byteslice(5..)).fetch("error")).to include("code" => "internal",
                                                                         "message" => "Internal error")
  end

  it "does not report a streaming exception when the deadline outcome wins arbitration" do
    now = 100.0
    allow(Process).to receive(:clock_gettime).and_call_original
    allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { now }
    stream = ConnectRPC::ServerStreamMethod.new(service: method.service, name: "Watch", request_type: :request,
                                                response_type: :response)
    late = lambda do |_request, _writer, _context|
      now += 2
      raise failure
    end
    server = described_class.new(services: [ConnectRPC::Service.new(name: stream.service, methods: [stream])],
                                 binding: binding, on_error: ->(error) { reports << error },
                                 handlers: { stream.path => late })
    written = +""
    output = Object.new
    output.define_singleton_method(:write) { |chunk, **| written << chunk }
    server.call_stream(path: stream.path, http_method: "POST", input: native_input, output: output,
                       headers: { "content-type" => ["application/connect+proto"], "connect-timeout-ms" => ["1000"] },
                       publish: ->(_head) {}, http2: true)
    expect(JSON.parse(written.byteslice(5..)).fetch("error")).to include("code" => "deadline_exceeded")
    expect(reports).to be_empty
  end
end
