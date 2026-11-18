# frozen_string_literal: true

require "open3"
require "rbconfig"

RSpec.describe "unary core" do
  it "validates unary declarations and rejects duplicate methods" do
    method = ConnectRPC::Method.new(service: "example.v1.Echo", name: "Say", request_type: String,
                                    response_type: String)
    expect(method.path).to eq("/example.v1.Echo/Say")
    expect { ConnectRPC::Service.new(name: method.service, rpcs: [method, method]) }
      .to raise_error(ArgumentError, "Duplicate RPCs")
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
    ["a\tb", "caf\xff".b, "bad\x00", "bad\n", "bad\r"].each do |value|
      expect { metadata.add("tag", value) }.to raise_error(ArgumentError)
    end
    expect { metadata.add("legal!", "value") }.to raise_error(ArgumentError)
  end

  it "keeps a monotonic budget and derives a positive clamped wire timeout" do
    now = 10.0
    deadline = ConnectRPC::Deadline.new(0.0001, clock: -> { now })
    expect(deadline.timeout_ms).to eq("1")
    now += 0.0002
    expect { deadline.check! }.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("deadline_exceeded") }
    expect(ConnectRPC::Deadline.new(1e20).timeout_ms).to eq("9999999999")
    [Float::INFINITY, -Float::INFINITY, Float::NAN, "1", nil, Complex(1, 0)].each do |timeout|
      expect { ConnectRPC::Deadline.new(timeout) }.to raise_error(ArgumentError, "Timeout must be finite real seconds")
    end
    expect(ConnectRPC::Context.new.cancelled?).to be(false)
  end
end

RSpec.describe "deadline and wire boundaries" do
  it "clamps finite extremely large timeouts without numeric overflow" do
    deadline = ConnectRPC::Deadline.new(Float::MAX, clock: -> { 0.0 })
    expect(deadline.timeout_ms).to eq("9999999999")
    expect(deadline.native_timeout).to eq(9_999_999.999)
    expect(deadline.remaining).to eq(Float::MAX)
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
    service = ConnectRPC::Service.new(name: method.service, rpcs: [method])
    invoker = double("invoker")
    expect(invoker).not_to receive(:call)
    server = ConnectRPC::Server.new(services: [service], binding: binding, invoker: invoker)
    response = server.call(path: method.path, http_method: "POST", input: input,
                           headers: { "content-type" => ["application/proto"], "connect-timeout-ms" => ["10"] })
    expect(response.status).to eq(504)
    expect(JSON.parse(response.body)["code"]).to eq("deadline_exceeded")
  end
end

RSpec.describe ConnectRPC::Service do
  let(:rpc) { ConnectRPC::Method.new(service: "example.v1.Echo", name: "Say", request_type: String, response_type: String) }

  it "exposes its declarations as rpcs and leaves Object#methods to Ruby reflection" do
    service = described_class.new(name: rpc.service, rpcs: [rpc])
    expect(service.rpcs).to eq([rpc])
    expect(service.rpcs).to be_frozen
    expect(service.methods).to include(:rpcs, :name)
    expect(service.methods).not_to include(rpc)
    expect { described_class.new(name: rpc.service, methods: [rpc]) }
      .to raise_error(ArgumentError, /missing keyword: :rpcs/)
    other = ConnectRPC::Method.new(service: "example.v1.Other", name: "Say", request_type: String,
                                   response_type: String)
    expect { described_class.new(name: rpc.service, rpcs: [other]) }
      .to raise_error(ArgumentError, "RPCs must belong to this service")
  end
end

RSpec.describe ConnectRPC::Deadline do
  [0, 0.0, -0.0, -1, -0.5, Rational(-1, 3)].each do |timeout|
    it "treats a #{timeout.inspect} second timeout as already expired" do
      deadline = described_class.new(timeout, clock: -> { 5.0 })
      expect(deadline.remaining).to eq(0.0)
      expect(deadline).to be_expired
      expect { deadline.check! }.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("deadline_exceeded") }
    end
  end
end

RSpec.describe "ConnectRPC::Protocol.call_deadline" do
  # Every monotonic read advances one second, so sampling both deadlines'
  # remaining time at different instants would favour the one read last.
  before do
    ticks = 100.0
    allow(Process).to receive(:clock_gettime).and_call_original
    allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { ticks += 1.0 }
  end

  it "compares absolute expiries of default-clock deadlines regardless of read order" do
    supplied = ConnectRPC::Deadline.new(10.5) # expires 0.5 s before a 10 s budget created on the next read
    expect(ConnectRPC::Protocol.call_deadline(10, supplied)).to equal(supplied)
    tied = ConnectRPC::Deadline.new(11)
    selected = ConnectRPC::Protocol.call_deadline(10, tied)
    expect(selected).not_to equal(tied)
    expect(selected).to be_a(ConnectRPC::Deadline)
  end

  it "builds the budget on a custom-clock deadline's own clock and compares absolute expiries" do
    ticks = 0.0
    clock = -> { ticks += 1.0 } # every read advances one second, like the stubbed monotonic clock
    earlier = ConnectRPC::Deadline.new(10.5, clock: clock) # expires 0.5 s before a 10 s budget created on the next read
    expect(ConnectRPC::Protocol.call_deadline(10, earlier)).to equal(earlier)
    tied = ConnectRPC::Deadline.new(11, clock: clock)
    expect(ConnectRPC::Protocol.call_deadline(10, tied)).not_to equal(tied)
    later = ConnectRPC::Deadline.new(12, clock: clock)
    selected = ConnectRPC::Protocol.call_deadline(10, later)
    expect(selected).not_to equal(later)
    expect(selected.remaining).to eq(9.0) # a 10 s budget on the custom clock, read one tick after creation
  end
end

RSpec.describe "binding resolution without the Google binding loaded" do
  it "names the binding require for omitted Client, Server and ErrorDetail bindings" do
    code = <<~RUBY
      require "connectrpc"
      abort "binding preloaded" if defined?(ConnectRPC::Bindings::GoogleProtobuf) || defined?(Google::Protobuf)
      # A top-level decoy must never stand in for the library's default binding.
      module Bindings
        class GoogleProtobuf; end
      end
      rpc = ConnectRPC::Method.new(service: "x.S", name: "M", request_type: String, response_type: String)
      service = ConnectRPC::Service.new(name: rpc.service, rpcs: [rpc])
      explicit = Object.new
      def explicit.encode(message, **) = message
      def explicit.decode(bytes, **) = bytes
      [-> { ConnectRPC::Client.new(base_url: "http://example.invalid") },
       -> { ConnectRPC::Server.new(services: [service]) },
       -> { ConnectRPC::ErrorDetail.new(type: "x.T", value: "").decode(type: String) }].each do |attempt|
        attempt.call
        puts "no error"
      rescue ArgumentError => e
        puts e.message
      end
      ConnectRPC::Client.new(base_url: "http://example.invalid", binding: explicit)
      ConnectRPC::Server.new(services: [service], binding: explicit)
      # Only nil selects the default; an explicit false is kept as given.
      client = ConnectRPC::Client.new(base_url: "http://example.invalid", binding: false)
      server = ConnectRPC::Server.new(services: [service], binding: false)
      puts [client, server].map { |owner| owner.instance_variable_get(:@binding).inspect }.join(",")
      puts ConnectRPC::ErrorDetail.new(type: "x.T", value: "raw").decode(binding: explicit, type: String)
      abort "binding loaded" if defined?(Google::Protobuf)
    RUBY
    output, error, status = Open3.capture3(RbConfig.ruby, "-I", File.expand_path("../lib", __dir__), "-e", code)
    expect(status.success?).to be(true), error
    lines = output.lines.map(&:chomp)
    expect(lines.first(3)).to all(include('require "connectrpc/bindings/google_protobuf"'))
    expect(lines.last(2)).to eq(["false,false", "raw"])
  end
end
