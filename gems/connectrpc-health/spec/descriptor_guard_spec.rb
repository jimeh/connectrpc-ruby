# frozen_string_literal: true

require "open3"
require "rbconfig"

RSpec.describe "Health descriptor load order" do
  # A pre-List grpc.health.v1 file, as older grpc gem releases register it.
  let(:older_descriptor) { <<~RUBY }
    require "google/protobuf"
    require "google/protobuf/descriptor_pb"
    proto = Google::Protobuf
    service = proto::FieldDescriptorProto.new(name: "service", number: 1, type: :TYPE_STRING,
                                              label: :LABEL_OPTIONAL, json_name: "service")
    file = proto::FileDescriptorProto.new(
      name: "grpc/health/v1/health.proto", package: "grpc.health.v1", syntax: "proto3",
      message_type: [proto::DescriptorProto.new(name: "HealthCheckRequest", field: [service]),
                     proto::DescriptorProto.new(name: "HealthCheckResponse")]
    )
    proto::DescriptorPool.generated_pool.add_serialized_file(proto::FileDescriptorProto.encode(file))
  RUBY

  def run_ruby(code)
    output, status = Open3.capture2e(RbConfig.ruby, "-e", code)
    [output, status.exitstatus]
  end

  it "refuses to load after an older descriptor without List" do
    output, status = run_ruby(<<~RUBY)
      #{older_descriptor}
      begin
        require "connectrpc/health"
      rescue LoadError => e
        puts e.message
        exit 3
      end
    RUBY
    expect(status).to eq(3), output
    expect(output).to include("without List").and include('require "connectrpc/health" before')
  end

  it "loads normally when it is first and satisfies the shared feature name" do
    output, status = run_ruby(<<~RUBY)
      require "connectrpc/health"
      abort "feature reloaded" if require "grpc/health/v1/health_pb"
      pool = Google::Protobuf::DescriptorPool.generated_pool
      abort "List missing" unless pool.lookup("grpc.health.v1.HealthListRequest")
      puts ConnectRPC::Health::SERVICE.methods.map(&:name).join(",")
    RUBY
    expect([output.strip, status]).to eq(["Check,List,Watch", 0])
  end
end
