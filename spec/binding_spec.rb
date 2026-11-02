# frozen_string_literal: true

require_relative "../examples/echo_schema"

RSpec.describe ConnectRPC::Bindings::GoogleProtobuf do
  let(:binding) { described_class.new }
  let(:type) { EchoExample::Message }

  it "round trips optional presence, enum, int64, bytes and WKT in both codecs" do
    message = type.new(name: "", count: 9_007_199_254_740_993, mood: :HAPPY,
                       at: Google::Protobuf::Timestamp.new(seconds: 1_704_067_200), data: "\x00\xff".b)
    %i[proto json].each do |codec|
      decoded = binding.decode(binding.encode(message, type: type, codec: codec), type: type, codec: codec)
      expect(decoded).to eq(message)
      expect(decoded.has_name?).to be_truthy
      expect(binding.decode(binding.encode(type.new, type: type, codec: codec), type: type,
                                                                                codec: codec).has_name?).to be_falsey
    end
    json = JSON.parse(binding.encode(message, type: type, codec: :json))
    expect(json).to include("name" => "", "count" => "9007199254740993", "mood" => "HAPPY",
                            "at" => "2024-01-01T00:00:00Z", "data" => "AP8=")
  end

  it "ignores unknown ProtoJSON fields and preserves null-as-unset" do
    decoded = binding.decode('{"name":null,"futureField":true}', type: type, codec: :json)
    expect(decoded.has_name?).to be_falsey
  end

  it "rejects malformed binary/JSON and incorrect message types through the binding error seam" do
    [[:proto, "\xff".b], [:json, "{"], [:json, '{"count":"not an integer"}']].each do |codec, bytes|
      expect { binding.decode(bytes, type: type, codec: codec) }.to raise_error(ConnectRPC::CodecError)
    end
    expect { binding.encode("wrong", type: type, codec: :proto) }.to raise_error(ConnectRPC::CodecError)
  end

  it "decodes typed binary details rather than treating them as JSON Any" do
    message = type.new(name: "detail")
    detail = ConnectRPC::ErrorDetail.new(type: "example.v1.EchoMessage",
                                         value: binding.encode(message,
                                                               type: type, codec: :proto))
    error = ConnectRPC::Error.new(:invalid_argument, "safe", details: [detail])
    parsed = ConnectRPC::Protocol.parse_error(400, { "content-type" => ["application/json"] },
                                              JSON.generate(error.to_h))
    expect(parsed.details.first.type).to eq("example.v1.EchoMessage")
    expect(parsed.details.first.decode(binding: binding, type: type)).to eq(message)
    expect(parsed.details.first.to_h).not_to have_key("debug")
  end
end

RSpec.describe "top-level protobuf wrappers" do
  it "uses native ProtoJSON mapping for scalar wrapper roots" do
    require "google/protobuf/wrappers_pb"
    binding = ConnectRPC::Bindings::GoogleProtobuf.new
    [Google::Protobuf::StringValue.new(value: "typed"),
     Google::Protobuf::Int64Value.new(value: 9_007_199_254_740_993),
     Google::Protobuf::BytesValue.new(value: "\x00\xff".b)].each do |message|
      bytes = binding.encode(message, type: message.class, codec: :json)
      expect(binding.decode(bytes, type: message.class, codec: :json)).to eq(message)
    end
  end
end
