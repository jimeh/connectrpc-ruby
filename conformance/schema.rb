# frozen_string_literal: true

require "connectrpc/bindings/google_protobuf"
require "google/protobuf/descriptor_pb"
require "google/protobuf/any_pb"
require "google/protobuf/empty_pb"
require "google/protobuf/struct_pb"

module Conformance
  PREFIX = "connectrpc.conformance.v1."
  FILES = %w[config service client_compat server_compat].map { |name| "connectrpc/conformance/v1/#{name}.proto" }.freeze
  DESCRIPTORS = Google::Protobuf::FileDescriptorSet.decode(File.binread(File.join(__dir__, "descriptor.bin"))).file
  pool = Google::Protobuf::DescriptorPool.generated_pool
  loaded = []
  DESCRIPTORS.each do |file|
    if FILES.include?(file.name)
      raise "Descriptor dependency order: #{file.name}" unless file.dependency.all? { |name| loaded.include?(name) }

      pool.add_serialized_file(Google::Protobuf::FileDescriptorProto.encode(file))
    else
      raise "Unexpected descriptor #{file.name}" unless %w[google/protobuf/any.proto google/protobuf/empty.proto
                                                           google/protobuf/struct.proto].include?(file.name)
    end
    loaded << file.name
  end
  raise "Missing conformance descriptors" unless loaded.select { |name| FILES.include?(name) } == FILES

  def self.register_messages(messages, scope, prefix)
    messages.each do |message|
      type_name = prefix + message.name
      type = Google::Protobuf::DescriptorPool.generated_pool.lookup(type_name).msgclass
      scope.const_set(message.name, type)
      register_messages(message.nested_type, type, "#{type_name}.")
    end
  end

  DESCRIPTORS.select { |file| FILES.include?(file.name) }.each do |file|
    register_messages(file.message_type, self, PREFIX)
  end
  RequestInfo = ConformancePayload::RequestInfo
  BINDING = ConnectRPC::Bindings::GoogleProtobuf.new
  SERVICE_NAME = "#{PREFIX}ConformanceService".freeze
  UNARY = ConnectRPC::Method.new(service: SERVICE_NAME, name: "Unary", request_type: UnaryRequest,
                                 response_type: UnaryResponse)
  IDEMPOTENT_UNARY = ConnectRPC::Method.new(service: SERVICE_NAME, name: "IdempotentUnary",
                                            request_type: IdempotentUnaryRequest,
                                            response_type: IdempotentUnaryResponse,
                                            idempotency: :no_side_effects)
  UNIMPLEMENTED = ConnectRPC::Method.new(service: SERVICE_NAME, name: "Unimplemented",
                                         request_type: UnimplementedRequest,
                                         response_type: UnimplementedResponse)
  SERVICE = ConnectRPC::Service.new(name: SERVICE_NAME, methods: [UNARY, IDEMPOTENT_UNARY, UNIMPLEMENTED])

  def self.pack(message)
    Google::Protobuf::Any.new(type_url: "type.googleapis.com/#{message.class.descriptor.name}", value: message.to_proto)
  end

  def self.headers(metadata)
    metadata.map { |name, values| Header.new(name: name, value: values) }
  end

  def self.metadata(headers)
    headers.each_with_object(ConnectRPC::Metadata.new) do |header, result|
      header.value.each { |value| result.add(header.name, value) }
    end
  end

  def self.limit(value)
    value.zero? ? ConnectRPC::Protocol::DEFAULT_LIMIT : value
  end
end
