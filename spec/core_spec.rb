# frozen_string_literal: true

RSpec.describe "unary core" do
  it "validates unary declarations and rejects duplicate methods" do
    method = ConnectRPC::Method.new(service: "example.v1.Echo", name: "Say", request_type: String,
                                    response_type: String)
    expect(method.path).to eq("/example.v1.Echo/Say")
    expect { ConnectRPC::Service.new(name: method.service, methods: [method, method]) }.to raise_error(ArgumentError)
    expect do
      ConnectRPC::Method.new(service: "bad/path", name: "Say", request_type: String, response_type: String)
    end.to raise_error(ArgumentError)
    expect do
      ConnectRPC::Method.new(service: method.service, name: "Say", request_type: String, response_type: String,
                             cardinality: :server_streaming)
    end.to raise_error(ArgumentError, /unary/)
  end

  it "preserves repeated metadata and strictly decodes binary metadata" do
    metadata = ConnectRPC::Metadata.new("X-Tag" => ["a,b", "c"])
    expect(metadata["x-tag"]).to eq(["a,b", "c"])
    metadata.add_binary("data-bin", "\x00\xff".b)
    expect(metadata.binary("data-bin")).to eq(["\x00\xff".b])
    metadata.add_binary("empty-bin", "")
    expect(metadata.binary("empty-bin")).to eq([""])
    expect(ConnectRPC::Metadata.decode_binary("Zg==")).to eq("f")
    expect(ConnectRPC::Metadata.decode_binary("Zg")).to eq("f")
    ["Z===", "A", "Zm=8", "Zg!!", "Zh=="].each do |bad|
      expect { ConnectRPC::Metadata.decode_binary(bad) }.to raise_error(ArgumentError)
    end
    %w[connect-timeout-ms trailer-tag host accept-encoding].each do |key|
      expect { metadata.add(key, "bad") }.to raise_error(ArgumentError)
    end
    expect { metadata.add("tag", "value\r\ninjected: yes") }.to raise_error(ArgumentError)
  end

  it "keeps a monotonic budget and derives a positive clamped wire timeout" do
    now = 10.0
    deadline = ConnectRPC::Deadline.new(0.0001, clock: -> { now })
    expect(deadline.timeout_ms).to eq("1")
    now += 0.0002
    expect { deadline.check! }.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("deadline_exceeded") }
    expect(ConnectRPC::Deadline.new(1e20).timeout_ms).to eq("9999999999")
    [0, -1, Float::INFINITY, Float::NAN, "1"].each do |timeout|
      expect { ConnectRPC::Deadline.new(timeout) }.to raise_error(ArgumentError)
    end
    expect(ConnectRPC::Context.new.cancelled?).to be(false)
  end
end

RSpec.describe "deadline and wire boundaries" do
  it "clamps finite extremely large timeouts without numeric overflow" do
    expect(ConnectRPC::Deadline.new(Float::MAX).timeout_ms).to eq("9999999999")
  end

  it "falls back safely for invalid UTF-8 inside otherwise valid JSON errors" do
    body = "{\"code\":\"internal\",\"message\":\"\xff\"}".b
    expect(ConnectRPC::Protocol.parse_error(503, { "content-type" => ["application/json"] },
                                            body).code).to eq("unavailable")
  end

  it "checks server expiry after a synchronous input read before invoking a binding" do
    now = 0.0
    allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { now }
    input = double("input")
    allow(input).to receive(:read) do
      now = 0.02
      "bytes"
    end
    binding = double("binding")
    expect(binding).not_to receive(:decode)
    method = ConnectRPC::Method.new(service: "test.Service", name: "Read", request_type: String, response_type: String)
    service = ConnectRPC::Service.new(name: method.service, methods: [method])
    invoker = double("invoker")
    expect(invoker).not_to receive(:call)
    server = ConnectRPC::Server.new(services: [service], binding: binding, invoker: invoker)
    response = server.call(path: method.path, http_method: "POST", input: input,
                           headers: { "content-type" => ["application/proto"], "connect-timeout-ms" => ["10"] })
    expect(response.status).to eq(504)
    expect(JSON.parse(response.body)["code"]).to eq("deadline_exceeded")
  end
end
