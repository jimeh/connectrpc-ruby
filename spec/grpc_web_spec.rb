# frozen_string_literal: true

require "stringio"

RSpec.describe "gRPC-Web server" do
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
    ConnectRPC::Method.new(service: "web.Tokens", name: "Call", request_type: :request, response_type: :response)
  end

  def server(declaration = method, **, &handler)
    ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: declaration.service, methods: [declaration])],
                           binding: binding, status_codec: status_codec, protocol: :grpc_web,
                           handlers: { declaration.path => handler || ->(message, _) { message } }, **)
  end

  def frame(bytes, flag = 0) = [flag, bytes.bytesize].pack("CN") + bytes
  def text(bytes) = [bytes].pack("m0")

  def call(target = server, body: frame("request"), media: "application/grpc-web", headers: {}, verb: "POST")
    target.call(path: method.path, http_method: verb,
                headers: { "content-type" => [media] }.merge(headers), input: StringIO.new(body))
  end

  def terminal(response, representation = :binary)
    body = response.body
    body = body.scan(/.{4}/).map { |group| group.unpack1("m0") }.join if representation == :text
    offset = 0
    loop do
      flag, length = body.byteslice(offset, 5).unpack("CN")
      bytes = body.byteslice(offset + 5, length)
      return bytes if flag == 128

      offset += length + 5
    end
  end

  it "uses custom message binding tokens and a separate status codec for binary unary" do
    response = call
    expect(response.status).to eq(200)
    expect(response.headers["content-type"]).to eq(["application/grpc-web+proto"])
    expect(response.body).to eq(frame("request") + frame("grpc-status: 0\r\n", 128))
    expect(response.headers["vary"]).to include("Accept")
  end

  it "accepts arbitrary separately padded base64 groups and emits independently padded writes" do
    bytes = frame("q")
    response = call(body: bytes.bytes.map { |byte| text(byte.chr) }.join, media: "application/grpc-web-text+proto")
    expect(response.status).to eq(200)
    expect(terminal(response, :text)).to eq("grpc-status: 0\r\n")
  end

  it "negotiates output independently using specificity and request representation on ties" do
    response = call(headers: { "accept" => ["application/grpc-web;q=0, */*;q=1"] })
    expect(response.headers["content-type"]).to eq(["application/grpc-web-text+proto"])
    response = call(headers: { "accept" => ["application/*;q=0.5, application/grpc-web-text;q=0.5"] })
    expect(response.headers["content-type"]).to eq(["application/grpc-web+proto"])
  end

  ["application/grpc-web+json", "application/grpc-web; charset=utf-8"].each do |media|
    it "rejects unsupported media #{media} before reading" do
      input = Object.new
      expect(input).not_to receive(:read)
      response = server.call(path: method.path, http_method: "POST", headers: { "content-type" => [media] },
                             input: input)
      expect(response.status).to eq(415)
      expect(response.headers["vary"]).to include("Accept")
    end
  end

  it "rejects unacceptable output before input or binding" do
    expect(binding).not_to receive(:decode)
    response = call(headers: { "accept" => ["application/json"] }, body: "not framed")
    expect(response.status).to eq(406)
  end

  { "" => 12, "\x80\0\0\0\0".b => 3, "\x01\0\0\0\0".b => 13,
    "\x04\0\0\0\0".b => 3, "\0" => 3 }.each do |body, code|
    it "maps malformed or missing #{body.unpack1('H*')} input to status #{code}" do
      expect(terminal(call(body: body))).to include("grpc-status: #{code}\r\n")
    end
  end

  it "detects excess request framing without decoding another message" do
    expect(binding).to receive(:decode).once.and_call_original
    expect(terminal(call(body: frame("one") + frame("two")))).to include("grpc-status: 12\r\n")
  end

  %w[A A=== AB== AAB= !!!! AAA\n].each do |body|
    it "rejects invalid base64 #{body.inspect}" do
      expect(terminal(call(body: body, media: "application/grpc-web-text"), :text)).to include("grpc-status: 3\r\n")
    end
  end

  it "rejects portable server streaming before reading, binding or handler invocation" do
    declaration = ConnectRPC::ServerStreamMethod.new(service: method.service, name: method.name,
                                                     request_type: :request, response_type: :response)
    target = server(declaration) { raise "handler ran" }
    expect(binding).not_to receive(:decode)
    expect(terminal(call(target, body: "bad"))).to include("grpc-status: 12\r\n")
  end

  it "keeps auto exactly Connect and native gRPC, and rejects invalid protocol arrays" do
    expect(call(server(protocol: :auto)).status).to eq(415)
    [[], [:auto], %i[connect connect], [:unknown], [[:grpc_web]], ["grpc_web"], nil].each do |protocol|
      expect { server(protocol: protocol) }.to raise_error(ArgumentError)
    end
    protocols = %i[connect grpc_web]
    target = server(protocol: protocols)
    protocols.clear
    expect(call(target).status).to eq(200)
    expect { ConnectRPC::Client.new(base_url: "http://localhost", binding: binding, protocol: :grpc_web) }
      .to raise_error(ArgumentError)
  end

  it "keeps Connect GET query controls authoritative on a mixed Web server" do
    safe = ConnectRPC::Method.new(service: method.service, name: method.name, request_type: :request,
                                  response_type: :response, idempotency: :no_side_effects)
    calls = []
    subscriber = Object.new
    subscriber.define_singleton_method(:start) do |call|
      calls << call
      nil
    end
    instrumentation = ConnectRPC::Instrumentation.new(subscribers: [subscriber])
    response = server(safe, protocol: %i[connect grpc_web], instrumentation: instrumentation).call(
      path: safe.path, http_method: "GET", query: "encoding=proto&base64=1&message=cXVlcnk",
      headers: { "content-type" => ["application/grpc-web"] }, input: nil
    )
    expect(response.status).to eq(200)
    expect(response.body).to eq("query")
    expect(calls.map(&:protocol)).to eq([:connect])
    expect(calls.first.carrier).to eq("content-type" => ["application/grpc-web"])
    expect(calls.first.carrier).to be_frozen
    expect(calls.first.carrier["content-type"]).to be_frozen
  end

  it "rejects unsafe mixed-server Connect GET before inspecting headers" do
    response = server(protocol: %i[connect grpc_web]).call(path: method.path, http_method: "GET",
                                                           headers: nil, input: nil, query: nil)
    expect(response.status).to eq(405)
  end

  [nil, Object.new].each do |headers|
    it "rejects unsafe mixed GET before normalization with #{headers.nil? ? 'nil' : 'hostile'} headers" do
      expect(headers).not_to receive(:each_with_object) unless headers.nil?
      expect(binding).not_to receive(:decode)
      target = server(protocol: %i[connect grpc_web]) { raise "unsafe handler invoked" }
      response = target.call(path: method.path, http_method: "GET", headers: headers, input: nil, query: nil)
      expect(response.status).to eq(405)
      expect(response.headers["allow"]).to eq(["POST"])
    end
  end

  it "keeps the default start_call POST classification and uses the actual GET method" do
    calls = []
    subscriber = Object.new
    subscriber.define_singleton_method(:start) do |call|
      calls << call
      nil
    end
    target = server(protocol: %i[connect grpc_web],
                    instrumentation: ConnectRPC::Instrumentation.new(subscribers: [subscriber]))
    headers = { "content-type" => ["application/grpc-web"] }
    target.start_call(path: method.path, headers: headers)&.finish(delivery: :unobserved)
    target.start_call(path: method.path, headers: headers, http_method: "GET")&.finish(delivery: :unobserved)
    expect(calls.map(&:protocol)).to eq(%i[grpc_web connect])
  end

  it "uses Connect GET before Web origin policy and ignores POST encoding controls" do
    safe = ConnectRPC::Method.new(service: method.service, name: method.name, request_type: :request,
                                  response_type: :response, idempotency: :no_side_effects)
    calls = []
    subscriber = Object.new
    subscriber.define_singleton_method(:start) do |call|
      calls << call
      nil
    end
    target = server(safe, protocol: %i[connect grpc grpc_web],
                          cors: ConnectRPC::GRPCWebCORS.new(origins: ["https://allowed.example"]),
                          instrumentation: ConnectRPC::Instrumentation.new(subscribers: [subscriber]))
    %w[application/grpc-web application/grpc application/connect+proto].each do |media|
      headers = { "content-type" => [media], "content-encoding" => ["unsupported"],
                  "connect-protocol-version" => ["ignored"], "origin" => ["https://denied.example"] }
      expect(target.cors_response(path: safe.path, http_method: "GET", headers: headers)).to be_nil
      response = target.call(path: safe.path, http_method: "GET", query: "encoding=proto&base64=1&message=cXVlcnk",
                             headers: headers, input: nil)
      expect([response.status, response.body]).to eq([200, "query"])
      expect(response.headers).not_to have_key("access-control-allow-origin")
    end
    expect(calls.map(&:protocol)).to eq(%i[connect connect connect])
  end

  it "emits repeated lower-case trailer lines and canonical rich status" do
    target = server do |_, context|
      context.response_trailers["x-repeat"] = %w[one two]
      raise ConnectRPC::Error.new(:not_found, "missing % café", details: [
                                    ConnectRPC::ErrorDetail.new(type: "opaque.Type", value: "\0\xff".b)
                                  ])
    end
    wire = terminal(call(target))
    expect(wire).to include("x-repeat: one\r\nx-repeat: two\r\n", "grpc-status: 5\r\n",
                            "grpc-message: missing %25 caf%C3%A9\r\n")
    encoded = wire[/grpc-status-details-bin: ([^\r]+)/, 1]
    error = status_codec.decode_status(ConnectRPC::Metadata.decode_binary(encoded), code: 5)
    expect(error.details.first.value).to eq("\0\xff".b)
  end

  it "handles preflight before input, binding, handler and RPC observation" do
    subscriber = Object.new
    expect(subscriber).not_to receive(:start)
    instrumentation = ConnectRPC::Instrumentation.new(subscribers: [subscriber])
    policy = ConnectRPC::GRPCWebCORS.new(origins: ["https://app.example"], request_headers: ["authorization"])
    target = server(cors: policy, instrumentation: instrumentation) { raise "handler ran" }
    expect(binding).not_to receive(:decode)
    response = call(target, verb: "OPTIONS", body: "bad", headers: {
                      "origin" => ["https://app.example"], "access-control-request-method" => ["POST"],
                      "access-control-request-headers" => ["content-type,authorization"]
                    })
    expect(response.status).to eq(204)
    expect(response.body).to eq("")
    expect(response.headers["access-control-max-age"]).to eq(["0"])
    expect(response.headers["access-control-allow-origin"]).to eq(["https://app.example"])
    expect(response.headers).not_to have_key("access-control-allow-credentials")
  end

  [{ "origin" => ["https://denied.example"] }, { "origin" => ["https://app.example", "https://app.example"] },
   { "access-control-request-method" => ["GET"] },
   { "access-control-request-headers" => ["authorization"] }].each do |invalid|
    it "denies disallowed or ambiguous preflight #{invalid.inspect}" do
      policy = ConnectRPC::GRPCWebCORS.new(origins: ["https://app.example"])
      headers = { "origin" => ["https://app.example"], "access-control-request-method" => ["POST"] }.merge(invalid)
      response = call(server(cors: policy), verb: "OPTIONS", headers: headers)
      expect(response.status).to eq(403)
      expect(response.headers.keys.grep(/access-control-allow/)).to be_empty
    end
  end

  it "preserves allowed-origin CORS on negotiation errors and native admission responses" do
    policy = ConnectRPC::GRPCWebCORS.new(origins: ["https://app.example"], credentials: true)
    target = server(cors: policy)
    headers = { "origin" => ["https://app.example"] }
    response = call(target, headers: headers, verb: "GET")
    expect(response.status).to eq(405)
    expect(response.headers["vary"]).to include("Origin", "Accept")
    expect(response.headers["access-control-allow-credentials"]).to eq(["true"])
    admission = ConnectRPC::TransportResponse.new(status: 503, headers: { "vary" => ["Accept-Encoding"] }, body: "")
    response = target.cors_response(path: method.path, http_method: "POST", headers: {
                                      "content-type" => ["application/grpc-web"], **headers
                                    }, response: admission)
    expect(response.status).to eq(503)
    expect(response.headers["vary"]).to eq(%w[Accept-Encoding Origin Accept])
    preflight = target.cors_response(path: method.path, http_method: "OPTIONS", headers: {
                                       "origin" => ["https://app.example"], "access-control-request-method" => ["POST"]
                                     }, response: admission)
    expect(preflight.status).to eq(503)
    expect(preflight.headers["access-control-allow-origin"]).to eq(["https://app.example"])
    expect(preflight.headers["vary"]).to include("Accept-Encoding", "Origin", "Access-Control-Request-Method")
    expect(target.cors_response(path: method.path, http_method: "POST", headers: {
                                  "content-type" => ["application/grpc-web"], **headers
                                }, response: response).headers).to eq(response.headers)
  end

  it "fails closed on conflicting application CORS without replacing other security fields" do
    policy = ConnectRPC::GRPCWebCORS.new(origins: ["https://app.example"])
    target = server(cors: policy) do |message, context|
      context.response_metadata["access-control-allow-origin"] = ["*"]
      message
    end
    expect do
      call(target, headers: { "origin" => ["https://app.example"] })
    end.to raise_error(ConnectRPC::Error) { |error|
             expect(error.code).to eq("internal")
           }
  end

  it "validates and deep-freezes CORS configuration and requires explicit Web enablement" do
    origins = [+"https://app.example"]
    policy = ConnectRPC::GRPCWebCORS.new(origins: origins)
    origins.first.replace("https://changed.example")
    expect(policy.origins).to eq(["https://app.example"])
    expect(policy).to be_frozen
    expect(policy.origins.first).to be_frozen
    expect { server(protocol: :auto, cors: policy) }.to raise_error(ArgumentError)
    ["*", "null", "https://example/", "http://user@example", "https://example:443"].each do |origin|
      expect { ConnectRPC::GRPCWebCORS.new(origins: [origin]) }.to raise_error(ArgumentError)
    end
  end

  [12_288, 12_289].each do |size|
    it "bounds encoded text output fragments for #{size} raw bytes and flushes small prefixes" do
      output = Object.new
      fragments = []
      output.define_singleton_method(:write) do |bytes, **|
        fragments << bytes
        nil
      end
      wrapper = ConnectRPC.const_get(:GRPCWeb)::Output.new(output, :text)
      wrapper.write("first")
      expect(fragments).to eq([text("first")])
      wrapper.write("x" * size)
      expect(fragments.map(&:bytesize).max).to be <= 16_384
      expect(fragments.drop(1).map { |part| part.unpack1("m0") }.join).to eq("x" * size)
    end
  end

  it "accepts the exact fourfold padded request cap and rejects declared excess before input" do
    target = server(request_wire_limit: 1, request_limit: 1)
    bytes = frame("x")
    encoded = bytes.bytes.map { |byte| text(byte.chr) }.join
    expect(encoded.bytesize).to eq(24)
    expect(terminal(call(target, body: encoded, media: "application/grpc-web-text"),
                    :text)).to include("grpc-status: 0\r\n")
    input = Object.new
    expect(input).not_to receive(:read)
    response = target.call(path: method.path, http_method: "POST", headers: {
                             "content-type" => ["application/grpc-web-text"], "content-length" => ["25"]
                           }, input: input)
    expect(terminal(response, :text)).to include("grpc-status: 8\r\n")
  end

  it "keeps independent encoded, frame and decoded message limits" do
    target = server(request_wire_limit: 4, request_limit: 1)
    expect(terminal(call(target, body: frame("xx")))).to include("grpc-status: 8\r\n")
    expect(terminal(call(target, body: frame("x"),
                                 headers: { "content-length" => ["7"] }))).to include("grpc-status: 3\r\n")
    expect(terminal(call(target, body: frame("x"),
                                 headers: { "content-length" => ["bad"] }))).to include("grpc-status: 3\r\n")
    expect(terminal(call(target, headers: { "grpc-timeout" => ["0S"] }))).to include("grpc-status: 4\r\n")
    expect(terminal(call(target, headers: { "grpc-timeout" => ["1s"] }))).to include("grpc-status: 13\r\n")
  end

  it "rejects unsupported shape and capability declarations before input" do
    [ConnectRPC::ClientStreamMethod, ConnectRPC::BidiStreamMethod].each do |kind|
      declaration = kind.new(service: method.service, name: method.name, request_type: :request,
                             response_type: :response)
      expect(terminal(call(server(declaration), body: "bad"))).to include("grpc-status: 12\r\n")
    end
    [[], [:bidi_stream], %i[unary unary]].each do |capabilities|
      expect do
        server.call_grpc_web(path: method.path, http_method: "POST", headers: {}, input: StringIO.new,
                             output: Object.new, publish: ->(*) {}, capabilities: capabilities)
      end.to raise_error(ArgumentError)
    end
  end

  it "keeps Web identity-only on a mixed gzip server and distinguishes malformed encoding" do
    target = server(protocol: %i[connect grpc grpc_web], encoding: :gzip)
    expect(terminal(call(target, headers: { "grpc-encoding" => ["gzip"] }))).to include("grpc-status: 12\r\n")
    expect(terminal(call(target, headers: { "grpc-encoding" => [""] }))).to include("grpc-status: 13\r\n")
    expect(terminal(call(target,
                         headers: { "grpc-encoding" => %w[identity identity] }))).to include("grpc-status: 13\r\n")
    expect(terminal(call(target, headers: { "content-encoding" => ["gzip"] }))).to include("grpc-status: 12\r\n")
    response = call(target, headers: { "grpc-accept-encoding" => ["gzip"], "accept-encoding" => ["gzip"] })
    expect(response.body).to start_with(frame("request"))
    expect(response.headers).not_to have_key("content-encoding")
    expect(response.headers).not_to have_key("grpc-encoding")
  end

  it "replaces oversized status, aborts when even minimal status cannot fit, and retains codec failure" do
    target = server(grpc_status_wire_limit: 44) do
      raise ConnectRPC::Error.new(:not_found, "oversized" * 100)
    end
    expect(terminal(call(target))).to eq("grpc-status: 8\r\n")
    expect { call(server(grpc_status_wire_limit: 43)) }.to raise_error(ConnectRPC::Error) { |error|
      expect(error.code).to eq("resource_exhausted")
    }
    failure = RuntimeError.new("private status codec failure")
    expect(status_codec).to receive(:encode_status).and_raise(failure)
    target = server do
      raise ConnectRPC::Error.new(:not_found, "safe",
                                  details: [ConnectRPC::ErrorDetail.new(type: "opaque.Type", value: "x")])
    end
    expect(terminal(call(target))).to include("grpc-status: 13\r\n")
  end
end
