# frozen_string_literal: true

require "spec_helper"
require "connectrpc/generator"

RSpec.describe "Opt-in service RBS generation" do
  def rbs_request(parameter = "rbs=true")
    request = Google::Protobuf::DescriptorProto.new(name: "Request")
    reply = Google::Protobuf::DescriptorProto.new(name: "Reply")
    container = Google::Protobuf::DescriptorProto.new(name: "Container", nested_type: [request, reply])
    messages = Google::Protobuf::FileDescriptorProto.new(
      name: "messages.proto", package: "wire.types", syntax: "proto3",
      options: Google::Protobuf::FileOptions.new(ruby_package: "RbsSpec::Messages"), message_type: [container]
    )
    rpc = Google::Protobuf::MethodDescriptorProto.new(
      name: "GetHTTP", input_type: ".wire.types.Container.Request", output_type: ".wire.types.Container.Reply"
    )
    service = Google::Protobuf::FileDescriptorProto.new(
      name: "api.proto", package: "wire.api", syntax: "proto3", dependency: [messages.name],
      options: Google::Protobuf::FileOptions.new(ruby_package: "rbs_spec.api"),
      service: [Google::Protobuf::ServiceDescriptorProto.new(name: "Echo", method: [rpc])]
    )
    Google::Protobuf::Compiler::CodeGeneratorRequest.new(
      parameter: parameter, file_to_generate: [service.name], proto_file: [messages, service]
    )
  end

  it "adds signatures while preserving every default Ruby byte" do
    original = ConnectRPC::Generator.generate(rbs_request(""))
    response = ConnectRPC::Generator.generate(rbs_request)
    expect(response.error).to eq("")
    expect(response.file.map(&:name)).to eq(%w[api_connect.rb api_connect.rbs])
    expect(response.file.first.content).to eq(original.file.first.content)
    expect(response.file.last.content).to include(
      "::ConnectRPC::Method[::RbsSpec::Messages::Container::Request, ::RbsSpec::Messages::Container::Reply]",
      "module RbsSpec", "module Api", "def get_http:", "::ConnectRPC::Response[::RbsSpec::Messages::Container::Reply]"
    )
  end

  ["unknown=true", "rbs=false", "rbs=TRUE", "rbs", "rbs=", "rbs=true,rbs=true", "rbs=true,", ",rbs=true",
   "rbs=true,,rbs=true"].each do |parameter|
    it "rejects parameter #{parameter.inspect} without partial output" do
      response = ConnectRPC::Generator.generate(rbs_request(parameter))
      expect(response.error).to match(/parameter/i)
      expect(response.file).to be_empty
    end
  end

  it "validates every target before returning Ruby or RBS" do
    request = rbs_request
    other = Google::Protobuf.deep_copy(request.proto_file.last)
    other.name = "bad.proto"
    other.package = "bad"
    other.options = Google::Protobuf::FileOptions.new(ruby_package: "RbsSpec::Bad")
    other.service.first["method"].first.input_type = ".missing.Message"
    request.proto_file << other
    request.file_to_generate << other.name
    response = ConnectRPC::Generator.generate(request)
    expect(response.error).to include("Unresolved")
    expect(response.file).to be_empty
  end

  it "retains collision and editions checks with the option enabled" do
    request = rbs_request
    request.proto_file.last.service.first["method"] << Google::Protobuf::MethodDescriptorProto.new(
      name: "GetHttp", input_type: ".wire.types.Container.Request", output_type: ".wire.types.Container.Reply"
    )
    expect(ConnectRPC::Generator.generate(request).error).to include("collision")
    expect(ConnectRPC::Generator.generate(request).file).to be_empty
    request.proto_file.last.syntax = "editions"
    expect(ConnectRPC::Generator.generate(request).error).to include("Unsupported syntax")
    expect(ConnectRPC::Generator.generate(request).file).to be_empty
  end

  it "emits callable empty handlers and skips files with no services" do
    request = rbs_request
    request.proto_file.last.service.first["method"].clear
    response = ConnectRPC::Generator.generate(request)
    expect(response.error).to eq("")
    expect(response.file.last.content).to include("def self.handlers: () -> ::Hash[::String, bot]")
    request.proto_file.last.service.clear
    expect(ConnectRPC::Generator.generate(request).file).to be_empty
  end
end
