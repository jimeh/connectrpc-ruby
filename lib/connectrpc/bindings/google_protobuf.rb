# frozen_string_literal: true

require_relative "../../connectrpc"

begin
  gem "google-protobuf", ">= 4.36.2", "< 5"
  require "google/protobuf"
  require "google/protobuf/any_pb"
  require "google/protobuf/descriptor_pb"
rescue LoadError => e
  raise LoadError, "ConnectRPC GoogleProtobuf binding requires google-protobuf >= 4.36.2, < 5: #{e.message}"
end

module ConnectRPC
  module Bindings
    class GoogleProtobuf
      WRAPPERS = %w[
        google.protobuf.DoubleValue google.protobuf.FloatValue google.protobuf.Int64Value google.protobuf.UInt64Value
        google.protobuf.Int32Value google.protobuf.UInt32Value google.protobuf.BoolValue google.protobuf.StringValue
        google.protobuf.BytesValue
      ].freeze

      def encode(message, type:, codec:)
        Protocol.codec!(codec)
        raise CodecError, "Wrong protobuf message type" unless message.is_a?(type)

        codec == :proto ? type.encode(message) : type.encode_json(message)
      rescue Google::Protobuf::Error, TypeError, ArgumentError, RuntimeError, JSON::JSONError => e
        raise CodecError, e.message
      end

      def decode(bytes, type:, codec:)
        Protocol.codec!(codec)
        return type.decode(bytes) if codec == :proto
        return decode_wrapper(bytes, type) if WRAPPERS.include?(type.descriptor.name)

        type.decode_json(bytes, ignore_unknown_fields: true)
      rescue Google::Protobuf::Error, TypeError, ArgumentError, RuntimeError, JSON::JSONError => e
        raise CodecError, e.message
      end

      def encode_status(error)
        status, any = status_types
        details = error.details.map do |detail|
          any.new(type_url: "type.googleapis.com/#{detail.type}", value: detail.value)
        end
        status.encode(status.new(code: GRPCProtocol::CODES.index(error.code), message: error.message, details: details))
      rescue Google::Protobuf::Error, TypeError, ArgumentError, RuntimeError => e
        raise CodecError, e.message
      end

      def decode_status(bytes, code:) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
        unless code.positive? && code <= GRPCProtocol::UINT32_MAX
          raise CodecError, "Expected a nonzero uint32 gRPC status"
        end

        status, = status_types
        decoded = status.decode(bytes)
        raise CodecError, "gRPC status code mismatch" unless decoded.code == code

        details = decoded.details.map do |detail|
          name = detail.type_url.split("/", -1).last
          raise CodecError, "Invalid gRPC detail type URL" unless detail.type_url.include?("/") && name && !name.empty?

          ErrorDetail.new(type: name, value: detail.value)
        end
        Error.new(GRPCProtocol::CODES[code] || :unknown, decoded.message, details: details)
      rescue Google::Protobuf::Error, TypeError, ArgumentError, RuntimeError => e
        raise CodecError, e.message
      end

      private

      def status_types
        # This pool is intentionally detached from generated_pool and public constants.
        pool = Google::Protobuf::DescriptorPool.new
        set = Google::Protobuf::FileDescriptorSet.decode(File.binread(File.join(__dir__, "descriptors/status.bin")))
        set.file.each { |file| pool.add_serialized_file(Google::Protobuf::FileDescriptorProto.encode(file)) }
        [pool.lookup("google.rpc.Status").msgclass, pool.lookup("google.protobuf.Any").msgclass]
      end

      def decode_wrapper(bytes, type)
        # Native protobuf 4.36 cannot parse a scalar wrapper at the JSON root.
        # Its native Any decoder can. A standard Any envelope keeps all protobuf
        # value validation in the backend, then supplies binary bytes to type.
        envelope = JSON.generate("@type" => "type.googleapis.com/#{type.descriptor.name}", "value" => JSON.parse(bytes))
        any = Google::Protobuf::Any.decode_json(envelope, ignore_unknown_fields: true)
        type.decode(any.value)
      end
    end
  end
end
