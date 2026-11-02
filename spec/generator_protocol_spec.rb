# frozen_string_literal: true

require "google/protobuf/plugin_pb"
require "open3"
require "rbconfig"
require "tmpdir"

RSpec.describe "protoc-gen-connectrpc-ruby protocol" do
  let(:plugin) { File.expand_path("../exe/protoc-gen-connectrpc-ruby", __dir__) }
  let(:request_type) { Google::Protobuf::Compiler::CodeGeneratorRequest }
  let(:response_type) { Google::Protobuf::Compiler::CodeGeneratorResponse }

  def invoke(bytes, *)
    Open3.capture3(RbConfig.ruby, "-I", File.expand_path("../lib", __dir__), plugin, *,
                   stdin_data: bytes, binmode: true)
  end

  it "reads raw protobuf through EOF and emits exactly one unframed response" do
    bytes = request_type.encode(request_type.new)
    output, error, status = invoke(bytes)
    expect(status.success?).to be(true), error
    expect(error).to eq("")
    response = response_type.decode(output)
    expect(response.supported_features).to eq(1)
    expect(output).to eq(response_type.encode(response))
    expect(output.bytes.first).to eq(16) # supported_features field, not a length prefix.
  end

  it "reports malformed and truncated protobuf only on stderr with nonzero status" do
    ["\xff".b, "\x0a\x05x".b].each do |bytes|
      output, error, status = invoke(bytes)
      expect(status.success?).to be(false)
      expect(output).to eq("")
      expect(error).to include("Invalid CodeGeneratorRequest protobuf")
    end
  end

  it "rejects CLI options without writing binary stdout" do
    output, error, status = invoke("", "--help")
    expect(status.success?).to be(false)
    expect(output).to eq("")
    expect(error).to include("accepts no CLI arguments", "protoc or Buf")
  end

  it "uses response.error and exit zero for supported protocol with rejected descriptors" do
    bytes = request_type.encode(request_type.new(parameter: "unknown"))
    output, error, status = invoke(bytes)
    expect(status.success?).to be(true), error
    expect(error).to eq("")
    response = response_type.decode(output)
    expect(response.error).to include("parameters", "unknown")
    expect(response.file).to be_empty
  end

  it "explains the incompatible optional Google dependency without binary stdout" do
    Dir.mktmpdir("generator-gate-") do |temporary|
      prelude = File.join(temporary, "incompatible.rb")
      File.write(prelude, <<~CODE)
        module Kernel
          def gem(name, *)
            raise Gem::LoadError, "incompatible installed google-protobuf" if name == "google-protobuf"
          end
        end
      CODE
      output, error, status = Open3.capture3(RbConfig.ruby, "-r", prelude, "-I", File.expand_path("../lib", __dir__),
                                             plugin, stdin_data: "", binmode: true)
      expect(status.success?).to be(false)
      expect(output).to eq("")
      expect(error).to include("generator requires google-protobuf >= 4.36.2, < 5", "incompatible")
    end
  end
end
