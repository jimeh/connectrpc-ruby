# frozen_string_literal: true

module ConnectRPC
  module Streaming
    def self.parse_terminal(bytes, metadata: Metadata.new)
      object = JSON.parse(bytes, max_nesting: 64)
      raise ArgumentError unless object.is_a?(Hash)

      trailers = terminal_metadata(object)
      [terminal_error(object["error"], metadata, trailers), trailers]
    rescue JSON::ParserError, ArgumentError, TypeError, EncodingError
      raise Error.new(:internal, "Malformed streaming terminal"), cause: nil
    end

    def self.terminal_metadata(object)
      empty = {} #: headers
      trailers = object.fetch("metadata", empty)
      raise ArgumentError unless trailers.is_a?(Hash)

      Protocol.metadata(trailers).freeze
    end

    def self.terminal_error(error, metadata, trailers)
      return if error.nil?
      raise ArgumentError unless Protocol.valid_wire_error?(error)

      empty = [] #: Array[ErrorDetail]
      details = Protocol.wire_details(error.fetch("details", empty))
      code = Error::HTTP_STATUS.key?(error["code"]) ? error["code"] : "unknown"
      Error.new(code, error.fetch("message", ""), details: details, metadata: metadata, trailers: trailers)
    end

    def self.terminal_bytes(error, trailers, wire_limit:, limit:)
      # @type var object: Hash[String, headers | Hash[String, String | Array[Hash[String, String]]]]
      object = { "metadata" => trailers.to_h }
      object["error"] = error.to_h if error
      bytes = JSON.generate(object, max_nesting: 64)
      return [bytes, error] if bytes.bytesize <= wire_limit && bytes.bytesize <= limit

      minimal_terminal(wire_limit, limit)
    end

    def self.minimal_terminal(wire_limit, limit)
      replacement = Error.new(:resource_exhausted, "Terminal exceeds byte limit")
      bytes = JSON.generate({ "error" => { "code" => replacement.code } })
      raise replacement if bytes.bytesize > wire_limit || bytes.bytesize > limit

      [bytes, replacement]
    end
  end
end
