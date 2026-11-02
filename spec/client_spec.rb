# frozen_string_literal: true

require "stringio"

RSpec.describe ConnectRPC::Client do
  let(:method) do
    ConnectRPC::Method.new(service: "test.Service", name: "Echo", request_type: String, response_type: String)
  end
  let(:binding) do
    Class.new do
      def encode(message, type:, codec:)
        raise ConnectRPC::CodecError unless message.is_a?(type) && codec == :proto

        "wire:#{message}"
      end

      def decode(bytes, type:, codec:)
        raise ConnectRPC::CodecError unless type == String && codec == :proto && bytes.start_with?("wire:")

        bytes.delete_prefix("wire:")
      end
    end.new
  end
  let(:reply) do
    ConnectRPC::TransportResponse.new(status: 200, headers: { "content-type" => ["application/proto"],
                                                              "x-repeat" => ["a,b", "c"], "trailer-x-end" => %w[d e] },
                                      body: "wire:hello")
  end
  let(:transport) { ->(_request) { reply } }

  def client(**)
    described_class.new(base_url: "http://example.invalid/prefix/", binding: binding, transport: transport, **)
  end

  it "uses a real custom binding/transport contract with prefixes, metadata and the shared budget" do
    captured = nil
    transport = lambda do |request|
      captured = request
      reply
    end
    result = described_class.new(base_url: "http://example.invalid/prefix/", binding: binding, transport: transport)
                            .call(method, "hello", metadata: { "x-request" => %w[one two] }, timeout: 2)
    expect(result.message).to eq("hello")
    expect(result.metadata["x-repeat"]).to eq(["a,b", "c"])
    expect(result.trailers["x-end"]).to eq(%w[d e])
    expect(captured.url.to_s).to eq("http://example.invalid/prefix/test.Service/Echo")
    expect(captured.http_method).to eq("POST")
    expect(captured.headers).to include("accept-encoding" => ["identity"], "connect-protocol-version" => ["1"],
                                        "x-request" => %w[one two])
    expect(captured.headers.fetch("connect-timeout-ms").first.to_i).to be_between(1, 2000)
    expect(captured.deadline).to be_a(ConnectRPC::Deadline)
  end

  it "retains the default 30 second budget when timeout is omitted" do
    expect(transport).to receive(:call) do |request|
      expect(request.deadline.remaining).to be_between(29, 30)
      expect(request.headers.fetch("connect-timeout-ms").first.to_i).to be_between(29_000, 30_000)
      reply
    end
    expect(client.call(method, "hello").message).to eq("hello")
  end

  it "passes an absent deadline and no timeout header for explicit nil" do
    expect(transport).to receive(:call) do |request|
      expect(request.deadline).to be_nil
      expect(request.headers).not_to have_key("connect-timeout-ms")
      reply
    end
    expect(client.call(method, "hello", timeout: nil).message).to eq("hello")
  end

  it "runs all three custom seams through the host-neutral server" do
    service = ConnectRPC::Service.new(name: method.service, methods: [method])
    invoker = lambda do |declaration, request, context|
      expect(declaration).to equal(method)
      expect(request).to eq("hello")
      context.response_trailers.add("x-custom", "invoked")
      "custom #{request}"
    end
    server = ConnectRPC::Server.new(services: [service], binding: binding, invoker: invoker)
    transport = lambda do |request|
      server.call(path: request.url.path, http_method: request.http_method, headers: request.headers,
                  input: StringIO.new(request.body))
    end
    client = described_class.new(base_url: "http://example.invalid", binding: binding, transport: transport)
    result = client.call(method, "hello")
    expect(result.message).to eq("custom hello")
    expect(result.trailers["x-custom"]).to eq(["invoked"])
  end

  it "rejects oversized requests before dispatch and oversized responses before decode" do
    expect(transport).not_to receive(:call)
    expect { client(request_limit: 3).call(method, "hello") }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("resource_exhausted") }
    expect(binding).not_to receive(:decode)
    small = described_class.new(base_url: "http://example.invalid", binding: binding, transport: lambda { |_|
      reply
    }, response_limit: 3)
    expect { small.call(method, "hello") }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("resource_exhausted") }
  end

  it "includes encoding in the same deadline budget" do
    clock = 0.0
    allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { clock }
    allow(binding).to receive(:encode) {
      clock = 2.0
      "wire:hello"
    }
    expect(transport).not_to receive(:call)
    expect { client.call(method, "hello", timeout: 1) }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("deadline_exceeded") }
  end

  it "bounds custom transport responses and maps codec failures safely" do
    reply.body = "invalid"
    expect { client.call(method, "hello") }.to raise_error(ConnectRPC::Error, "Cannot decode response")
    expect { client.call(method, 12) }.to raise_error(ConnectRPC::Error, "Cannot encode request")
  end

  { "text/plain" => "unknown", "application/json" => "internal", "application/grpc" => "internal" }.each do |type, code|
    it "rejects a 200 #{type} response as #{code}" do
      reply.headers["content-type"] = [type]
      expect { client.call(method, "hello") }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq(code) }
    end
  end

  it "rejects compressed responses without decoding" do
    reply.headers["content-encoding"] = ["gzip"]
    expect(binding).not_to receive(:decode)
    expect { client.call(method, "hello") }.to raise_error(ConnectRPC::Error, "Unsupported response content encoding")
  end

  %w[Identity IDENTITY].each do |encoding|
    it "accepts case-insensitive #{encoding} response encoding" do
      reply.headers["content-encoding"] = [encoding]
      expect(client.call(method, "hello").message).to eq("hello")
    end
  end

  [["identity", "identity"], ["identity, gzip"], ["identity, identity"]].each do |encodings|
    it "rejects multiple response encodings #{encodings}" do
      reply.headers["content-encoding"] = encodings
      expect(binding).not_to receive(:decode)
      expect { client.call(method, "hello") }.to raise_error(ConnectRPC::Error, "Unsupported response content encoding")
    end
  end

  [200, 400].each do |status|
    it "filters unrepresentable received metadata on a #{status} response without losing adjacent fields" do
      reply.status = status
      if status == 400
        reply.headers["content-type"] = ["application/json"]
        reply.body = '{"code":"invalid_argument","message":"deliberate"}'
      end
      fields = { "server" => ["caf\xff".b], "x-tab" => ["a\tb"], "legal!" => ["value"],
                 "x-mixed" => ["valid", "bad\x00"], "x-injection" => ["bad\r\nfield: value"],
                 "x-valid" => ["one,two", "three"] }
      reply.headers.merge!(fields)
      fields.each { |key, values| reply.headers["trailer-#{key}"] = values }
      result = if status == 200
                 client.call(method, "hello")
               else
                 begin
                   client.call(method, "hello")
                 rescue ConnectRPC::Error => e
                   expect(e.code).to eq("invalid_argument")
                   e
                 end
               end
      [result.metadata, result.trailers].each do |metadata|
        expect(metadata["x-valid"]).to eq(["one,two", "three"])
        (fields.keys - ["x-valid"]).each { |key| expect(metadata[key]).to be_nil }
      end
    end
  end

  it "rejects unsupported profile, codecs, limits and URL components" do
    [{ protocol: :grpc }, { mode: :stream }, { encoding: :br }, { codec: :xml }, { request_limit: 0 },
     { response_limit: 1.5 }].each do |options|
      expect { client(**options) }.to raise_error(ArgumentError)
    end
    %w[ftp://host http://user:password@host http://host?query http://host#fragment].each do |url|
      expect do
        described_class.new(base_url: url, binding: binding, transport: transport)
      end.to raise_error(ArgumentError)
    end
  end
end

RSpec.describe "Connect errors" do
  it "uses every specified code to HTTP mapping" do
    expected = { canceled: 499, unknown: 500, invalid_argument: 400, deadline_exceeded: 504, not_found: 404,
                 already_exists: 409, permission_denied: 403, resource_exhausted: 429, failed_precondition: 400,
                 aborted: 409, out_of_range: 400, unimplemented: 501, internal: 500, unavailable: 503,
                 data_loss: 500, unauthenticated: 401 }
    expected.each { |code, status| expect(ConnectRPC::Error.new(code).http_status).to eq(status) }
    expect { ConnectRPC::Error.new(:made_up) }.to raise_error(ArgumentError)
  end

  it "uses the HTTP fallback for malformed or absent Connect errors" do
    mapping = { 400 => "internal", 401 => "unauthenticated", 403 => "permission_denied", 404 => "unimplemented",
                429 => "unavailable", 502 => "unavailable", 503 => "unavailable", 504 => "unavailable",
                418 => "unknown" }
    malformed = ["", "{", "null", "[]",
                 '{"code":"internal","details":{}}', '{"code":"internal","details":[null]}',
                 '{"code":"internal","details":[{"type":"test.Detail","value":"!"}]}',
                 '{"code":"internal","message":1}', "\xff".b]
    mapping.each do |status, code|
      malformed.each do |body|
        expect(ConnectRPC::Protocol.parse_error(status, { "content-type" => ["application/json"] },
                                                body).code).to eq(code)
      end
    end
    error = ConnectRPC::Protocol.parse_error(503, { "content-type" => ["text/plain"] }, '{"code":"internal"}')
    expect(error.code).to eq("unavailable")
  end

  it "uses HTTP fallback for unknown codes with JSON charset and repeated metadata" do
    error = ConnectRPC::Protocol.parse_error(400, { "content-type" => ["application/json; charset=utf-8"],
                                                    "x-tag" => %w[a b], "trailer-x-tag" => %w[c d] },
                                             '{"code":"future_code","details":[]}')
    expect(error.code).to eq("internal")
    expect(error.metadata["x-tag"]).to eq(%w[a b])
    expect(error.trailers["x-tag"]).to eq(%w[c d])
  end
end

RSpec.describe "Connect error code fallback" do
  [nil, "future_code", :missing].each do |code|
    it "preserves message and details with HTTP fallback for #{code.inspect} code" do
      wire = { "message" => "preserved", "details" => [{ "type" => "test.Detail", "value" => "aGk" }] }
      wire["code"] = code unless code == :missing
      error = ConnectRPC::Protocol.parse_error(503, { "content-type" => ["application/json"] }, JSON.generate(wire))
      expect(error.code).to eq("unavailable")
      expect(error.message).to eq("preserved")
      expect(error.details.map { |detail| [detail.type, detail.value] }).to eq([["test.Detail", "hi"]])
    end
  end
end
