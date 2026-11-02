# frozen_string_literal: true

# This explicit descriptor builder mirrors spec/fixtures/echo.proto. It is not
# protoc output or a parser. Both peers consume these same FileDescriptorProto
# bytes; the schema is checked by Ruby mapping specs and Go reflection checks.
require "google/protobuf/descriptor_pb"

field = Google::Protobuf::FieldDescriptorProto
message = Google::Protobuf::DescriptorProto.new(
  name: "EchoMessage",
  oneof_decl: [Google::Protobuf::OneofDescriptorProto.new(name: "_name")],
  field: [
    field.new(name: "name", number: 1, type: :TYPE_STRING, label: :LABEL_OPTIONAL, proto3_optional: true,
              oneof_index: 0),
    field.new(name: "count", number: 2, type: :TYPE_INT64, label: :LABEL_OPTIONAL),
    field.new(name: "mood", number: 3, type: :TYPE_ENUM, label: :LABEL_OPTIONAL,
              type_name: ".example.v1.EchoMessage.Mood"),
    field.new(name: "at", number: 4, type: :TYPE_MESSAGE, label: :LABEL_OPTIONAL,
              type_name: ".google.protobuf.Timestamp"),
    field.new(name: "data", number: 5, type: :TYPE_BYTES, label: :LABEL_OPTIONAL)
  ],
  enum_type: [Google::Protobuf::EnumDescriptorProto.new(name: "Mood", value: [
                                                          Google::Protobuf::EnumValueDescriptorProto.new(
                                                            name: "MOOD_UNSPECIFIED", number: 0
                                                          ),
                                                          Google::Protobuf::EnumValueDescriptorProto.new(name: "HAPPY",
                                                                                                         number: 1)
                                                        ])]
)
methods = %w[Echo Fail].map do |name|
  Google::Protobuf::MethodDescriptorProto.new(name: name, input_type: ".example.v1.EchoMessage",
                                              output_type: ".example.v1.EchoMessage")
end
file = Google::Protobuf::FileDescriptorProto.new(
  name: "echo.proto", package: "example.v1", syntax: "proto3", dependency: ["google/protobuf/timestamp.proto"],
  message_type: [message], service: [Google::Protobuf::ServiceDescriptorProto.new(name: "EchoService", method: methods)]
)
bytes = Google::Protobuf::FileDescriptorProto.encode(file)
fixture = File.expand_path("../spec/fixtures/echo.bin", __dir__)
if ARGV == ["--check"]
  abort "Descriptor fixture differs; run mise run fixture:build" unless File.binread(fixture) == bytes
  puts "Shared descriptor fixture matches builder (#{bytes.bytesize} bytes)"
else
  File.binwrite(fixture, bytes)
end
