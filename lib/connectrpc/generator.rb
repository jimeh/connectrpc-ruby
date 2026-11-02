# frozen_string_literal: true

begin
  gem "google-protobuf", ">= 4.36.2", "< 5"
  require "google/protobuf/plugin_pb"
rescue LoadError => e
  raise LoadError, "ConnectRPC generator requires google-protobuf >= 4.36.2, < 5: #{e.message}"
end

require_relative "generator/names"
require_relative "generator/runtime_names"
require_relative "generator/schema"
require_relative "generator/renderer"

module ConnectRPC
  module Generator
    class InvalidDescriptor < StandardError; end

    # Pure request -> response. Expected descriptor errors use the protoc error
    # field; unexpected implementation failures must remain visible to callers.
    def self.generate(request)
      schema = Schema.new(request)
      files = schema.targets.filter_map do |file|
        next if file.service.empty?

        Google::Protobuf::Compiler::CodeGeneratorResponse::File.new(
          name: "#{Names.stem(file.name)}_connect.rb", content: Renderer.new(schema, file).render
        )
      end
      Google::Protobuf::Compiler::CodeGeneratorResponse.new(supported_features: 1, file: files)
    rescue InvalidDescriptor => e
      Google::Protobuf::Compiler::CodeGeneratorResponse.new(error: e.message)
    end
  end
end
