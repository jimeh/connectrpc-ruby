# frozen_string_literal: true

require "json"

module ConnectRPC
  TransportRequest = Struct.new(:url, :http_method, :headers, :body, :response_limit, :deadline, keyword_init: true)
  TransportResponse = Struct.new(:status, :headers, :body, keyword_init: true)
  Response = Struct.new(:message, :metadata, :trailers, keyword_init: true)

  module Protocol
    DEFAULT_LIMIT = 4 * 1024 * 1024
    CONTENT_TYPES = { proto: "application/proto", json: "application/json" }.freeze

    def self.limit!(value)
      raise ArgumentError, "Message limits must be positive integers" unless value.is_a?(Integer) && value.positive?

      value
    end

    def self.codec!(codec)
      raise ArgumentError, "Supported codecs: proto, json" unless CONTENT_TYPES.key?(codec)

      codec
    end

    def self.profile!(protocol, mode, encoding)
      return if protocol == :connect && mode == :unary && encoding == :identity

      raise ArgumentError, "Only Connect unary identity is supported"
    end

    def self.content_codec(value)
      return :proto if value&.match?(%r{\Aapplication/proto\z}i)
      return :json if value&.match?(%r{\Aapplication/json(?:\s*;\s*charset=(?:utf-8|"utf-8"))?\z}i)

      nil
    end

    def self.headers(values)
      values.each_with_object({}) { |(key, entries), result| (result[key.downcase] ||= []).concat(Array(entries)) }
    end

    def self.single(headers, key)
      entries = headers[key]
      entries&.size == 1 ? entries.first : nil
    end

    def self.identity_encoding?(headers)
      !headers.key?("content-encoding") || single(headers, "content-encoding")&.casecmp?("identity")
    end

    def self.metadata(headers, trailers: false)
      headers.each_with_object(Metadata.new) do |(key, entries), result|
        if trailers
          next unless key.start_with?("trailer-")

          key = key.delete_prefix("trailer-")
        end
        next if Metadata.reserved?(key)

        result[key] = entries
      rescue ArgumentError
        # HTTP permits names and values outside Connect's metadata contract.
        # Omit the whole field while retaining adjacent valid metadata.
        next
      end
    end

    def self.check_size!(bytes, limit)
      raise Error.new(:resource_exhausted, "Message exceeds byte limit") if bytes.bytesize > limit
    end

    def self.content_length!(entries, invalid_code:)
      return unless entries
      unless entries.size == 1 && /\A[0-9]{1,20}\z/.match?(entries.first)
        raise Error.new(invalid_code, "Invalid Content-Length")
      end

      entries.first.to_i
    end

    def self.error_body(error, limit)
      body = JSON.generate(error.to_h)
      return [body, error.http_status] if body.bytesize <= limit

      error = Error.new(:resource_exhausted, "Error exceeds byte limit")
      body = JSON.generate(error.to_h)
      [body.bytesize <= limit ? body : "", error.http_status]
    end

    def self.parse_error(status, headers, body)
      values = { metadata: metadata(headers), trailers: metadata(headers, trailers: true) }
      if content_codec(single(headers, "content-type")) == :json
        wire = wire_error(body)
        return Error.new(wire.fetch(:code), wire.fetch(:message), details: wire.fetch(:details), **values) if wire
      end
      Error.new(Error::HTTP_FALLBACK.fetch(status, "unknown"), "HTTP #{status}", **values)
    end

    def self.wire_error(body)
      object = JSON.parse(body)
      return unless valid_wire_error?(object)

      code = Error::HTTP_STATUS.key?(object["code"]) ? object["code"] : "unknown"
      { code: code, message: object.fetch("message", ""), details: wire_details(object.fetch("details", [])) }
    rescue JSON::ParserError, ArgumentError, TypeError, EncodingError
      # Invalid Connect error bodies use the protocol's HTTP fallback table.
      nil
    end

    def self.valid_wire_error?(object)
      return false unless object.is_a?(Hash)

      code = object["code"]
      message = object.fetch("message", "")
      code.is_a?(String) && code.valid_encoding? && message.is_a?(String) && message.valid_encoding?
    end

    def self.wire_details(details)
      raise ArgumentError unless details.is_a?(Array)

      details.map do |detail|
        raise ArgumentError unless detail.is_a?(Hash)

        ErrorDetail.new(type: detail["type"], value: Metadata.decode_binary(detail["value"]))
      end
    end
  end
end
