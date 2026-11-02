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
require_relative "generator/rbs_renderer"

module ConnectRPC
  module Generator
    class InvalidDescriptor < StandardError; end

    # Pure request -> response. Expected descriptor errors use the protoc error
    # field; unexpected implementation failures must remain visible to callers.
    def self.generate(request)
      schema = Schema.new(request)
      files = schema.targets.reject { |file| file.service.empty? }.flat_map { |file| render_files(schema, file) }
      Google::Protobuf::Compiler::CodeGeneratorResponse.new(supported_features: 1, file: files)
    rescue InvalidDescriptor => e
      Google::Protobuf::Compiler::CodeGeneratorResponse.new(error: e.message)
    end

    def self.render_files(schema, file)
      renderers = [[Renderer, "rb"]]
      renderers << [RBSRenderer, "rbs"] if schema.rbs
      renderers.map do |renderer, extension|
        Google::Protobuf::Compiler::CodeGeneratorResponse::File.new(
          name: "#{Names.stem(file.name)}_connect.#{extension}", content: renderer.new(schema, file).render
        )
      end
    end
    private_class_method :render_files
  end
end
