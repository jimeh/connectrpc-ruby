# frozen_string_literal: true

require "spec_helper"
require "google/protobuf/plugin_pb"

RSpec.describe "Service generation" do
  def request_for(rpcs: ["GetHTTP"], service: "EchoService", package: "generation.v1")
    methods = rpcs.map do |name|
      Google::Protobuf::MethodDescriptorProto.new(name: name, input_type: ".#{package}.Message",
                                                  output_type: ".#{package}.Message")
    end
    file = Google::Protobuf::FileDescriptorProto.new(
      name: "sample.proto", package: package, syntax: "proto3",
      message_type: [Google::Protobuf::DescriptorProto.new(name: "Message")],
      service: [Google::Protobuf::ServiceDescriptorProto.new(name: service, method: methods)]
    )
    Google::Protobuf::Compiler::CodeGeneratorRequest.new(file_to_generate: [file.name], proto_file: [file])
  end

  def generate(request)
    require "connectrpc/generator"
    ConnectRPC::Generator.generate(request)
  end

  it "emits reusable unary metadata, acronym methods and thin forwarding" do
    response = generate(request_for)
    expect(response.error).to eq("")
    expect(response.file.map(&:name)).to eq(["sample_connect.rb"])
    content = response.file.first.content
    expect(content).to include('require "sample_pb"', "module EchoService", "GET_HTTP = ::ConnectRPC::Method.new",
                               "def get_http(request, **options)", "@client.call(GET_HTTP, request, **options)",
                               "def self.handlers(get_http:)", "SERVICE = ::ConnectRPC::Service.new")
    expect(response.supported_features).to eq(1)
  end

  it "returns no partial files for target streaming methods" do
    request = request_for
    %w[client_streaming server_streaming].each do |flag|
      method = request.proto_file.first.service.first["method"].first
      method[flag] = true
      response = generate(request)
      expect(response.error).to include("sample.proto", "EchoService", "GetHTTP", "streaming")
      expect(response.file).to be_empty
      method[flag] = false
    end
  end

  it "rejects names that normalize to the same Ruby method" do
    response = generate(request_for(rpcs: %w[GetHTTP GetHttp]))
    expect(response.error).to include("GetHTTP", "GetHttp", "collision")
    expect(response.file).to be_empty
  end

  it "rejects every nonempty plugin parameter" do
    request = request_for
    request.parameter = "unknown=true"
    expect(generate(request).error).to include("parameter")
  end

  it "uses fixed reserved sets covering inherited methods on the running Ruby" do
    generate(request_for)
    inherited = Object.public_instance_methods.map(&:to_s).grep(/\A[a-z_][a-z_0-9]*\z/)
    reserved = ConnectRPC::Generator::Names::CLIENT_RESERVED + ConnectRPC::Generator::Names::KEYWORDS
    expect(inherited - reserved).to be_empty
  end

  %w[_Ping End Begin Method Send Initialize Handlers Service Client class].each do |name|
    it "rejects unsupported/reserved RPC #{name}" do
      response = generate(request_for(rpcs: [name]))
      expect(response.error).to include(name)
      expect(response.file).to be_empty
    end
  end

  it "ignores imported streaming services and only emits targets" do
    request = request_for
    imported = Google::Protobuf.deep_copy(request.proto_file.first)
    imported.name = "imported.proto"
    imported.package = "unrelated"
    imported.service.first["method"].first.client_streaming = true
    request.proto_file << imported
    response = generate(request)
    expect(response.error).to eq("")
    expect(response.file.map(&:name)).to eq(["sample_connect.rb"])
  end

  it "generates empty service declarations and handlers without messages" do
    request = request_for(rpcs: [])
    response = generate(request)
    expect(response.error).to eq("")
    expect(response.file.first.content).to include("methods: []", "def self.handlers\n", "{}")
    request.proto_file.first.service.clear
    expect(generate(request).file).to be_empty
  end

  it "ignores compiler version and source comments for deterministic output" do
    request = request_for
    first = generate(request).file.first.content
    request.compiler_version = Google::Protobuf::Compiler::Version.new(major: 99)
    request.proto_file.first.source_code_info = Google::Protobuf::SourceCodeInfo.new
    expect(generate(request).file.first.content).to eq(first)
  end

  it "rejects unknown targets, duplicate files, targets and proto symbols" do
    request = request_for
    request.file_to_generate << "absent.proto"
    expect(generate(request).error).to include("Missing generation target", "absent.proto")
    request.file_to_generate.pop
    request.file_to_generate << "sample.proto"
    expect(generate(request).error).to include("Duplicate generation target")
    request.file_to_generate.pop
    request.proto_file << request.proto_file.first
    expect(generate(request).error).to include("Duplicate proto file")
    request.proto_file.pop
    request.proto_file.first.message_type << request.proto_file.first.message_type.first
    expect(generate(request).error).to include("Duplicate proto symbol", "Message")
  end

  it "rejects unresolved, enum and synthetic map-entry RPC types" do
    request = request_for
    method = request.proto_file.first.service.first["method"].first
    method.input_type = ".missing.Type"
    expect(generate(request).error).to include("non-message type", ".missing.Type", "GetHTTP")
    method.input_type = ".generation.v1.Message"
    request.proto_file.first.message_type.first.options = Google::Protobuf::MessageOptions.new(map_entry: true)
    expect(generate(request).error).to include("non-message type")
  end

  ["../evil.proto", "/evil.proto", "a//b.proto", "a/../b.proto", "a\#{evil}.proto", "a\n.proto"].each do |name|
    it "rejects unsafe output path #{name.inspect}" do
      request = request_for
      request.proto_file.first.name = name
      request.file_to_generate[0] = name
      expect(generate(request).error).to include("Unsafe relative proto path")
    end
  end

  it "rejects output stem collisions between proto and protodevel" do
    request = request_for
    other = Google::Protobuf::FileDescriptorProto.new(name: "sample.protodevel", package: "other")
    request.proto_file << other
    expect(generate(request).error).to include("Output collision", "sample.proto", "sample.protodevel")
  end

  it "rejects unsupported syntax and editions without partial output" do
    %w[editions bad].each do |syntax|
      request = request_for
      request.proto_file.first.syntax = syntax
      response = generate(request)
      expect(response.error).to include("Unsupported syntax")
      expect(response.file).to be_empty
    end
  end

  it "rejects namespace injection and invalid constant tokens" do
    ["Safe::Bad;puts 1", "Safe::", "lower::Valid", "BEGIN", "a..b"].each do |namespace|
      request = request_for
      request.proto_file.first.options = Google::Protobuf::FileOptions.new(ruby_package: namespace)
      expect(generate(request).error).to include("Invalid Ruby constant")
    end
  end

  it "rejects service/message and alias namespace collisions with both proto names" do
    request = request_for(service: "message")
    expect(generate(request).error).to include("namespace collision", "generation.v1.message", "generation.v1.Message")
    request = request_for
    other = Google::Protobuf::FileDescriptorProto.new(
      name: "other.proto", package: "other",
      options: Google::Protobuf::FileOptions.new(ruby_package: "Generation::V1"),
      message_type: [Google::Protobuf::DescriptorProto.new(name: "EchoService")]
    )
    request.proto_file << other
    expect(generate(request).error).to include("namespace collision", "other.EchoService", "generation.v1.EchoService")
  end

  it "rejects package/class and service/package merges" do
    request = request_for
    other = Google::Protobuf::FileDescriptorProto.new(
      name: "other.proto", package: "generation.v1.echo_service",
      message_type: [Google::Protobuf::DescriptorProto.new(name: "Client")]
    )
    request.proto_file << other
    expect(generate(request).error).to include("namespace collision", "EchoService")
    other.package = "generation.v1.message"
    expect(generate(request).error).to include("namespace collision", "Message")
  end

  %w[File Process Object ConnectRPC Google].each do |name|
    it "rejects reserved top-level service #{name}" do
      request = request_for(rpcs: [], service: name, package: "")
      expect(generate(request).error).to include("Reserved top-level Ruby constant", name)
    end
  end

  it "rejects Ruby collisions in imported message namespaces required by a target" do
    request = request_for
    %w[first second].each do |name|
      imported = Google::Protobuf::FileDescriptorProto.new(
        name: "#{name}.proto", package: name,
        options: Google::Protobuf::FileOptions.new(ruby_package: "ImportedCollision"),
        message_type: [Google::Protobuf::DescriptorProto.new(name: "Message")]
      )
      request.proto_file << imported
      request.proto_file.first.dependency << imported.name
    end
    request.proto_file.first.service.first["method"].first.input_type = ".first.Message"
    response = generate(request)
    expect(response.error).to include("namespace collision", "first.Message", "second.Message")
    expect(response.file).to be_empty
  end

  it "allows RPC names distinct from inherited predicate methods" do
    response = generate(request_for(rpcs: %w[Equal Frozen RespondTo]))
    expect(response.error).to eq("")
    expect(response.file.first.content).to include("def equal(request", "def frozen(request", "def respond_to(request")
  end
end
