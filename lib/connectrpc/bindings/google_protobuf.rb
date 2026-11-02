# frozen_string_literal: true

require_relative "../../connectrpc"

begin
  gem "google-protobuf", ">= 4.36.2", "< 5"
  require "google/protobuf"
  require "google/protobuf/any_pb"
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

      private

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
