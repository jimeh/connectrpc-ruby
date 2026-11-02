# frozen_string_literal: true

require "json"

module ConnectRPC
  TransportRequest = Struct.new(:url, :http_method, :headers, :body, :response_limit, :deadline, :lifecycle,
                                keyword_init: true)
  TransportResponse = Struct.new(:status, :headers, :body, keyword_init: true)
  Response = Struct.new(:message, :metadata, :trailers, keyword_init: true)

  module Protocol # rubocop:disable Metrics/ModuleLength
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
      return if protocol == :connect && mode == :unary && %i[identity gzip].include?(encoding)

      raise ArgumentError, "Only Connect unary identity/gzip is supported"
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

    def self.content_encoding(headers)
      return :identity unless headers.key?("content-encoding")

      value = single(headers, "content-encoding")
      return :identity if value&.casecmp?("identity")
      return :gzip if value&.casecmp?("gzip")

      nil
    end

    def self.accept_encoding!(value, encoding)
      return [encoding, :identity].uniq if value.nil?
      unless value.is_a?(Array) && !value.empty? && value.uniq.size == value.size &&
             value.all? { |entry| %i[identity gzip].include?(entry) }
        raise ArgumentError, "Response encodings must be distinct identity/gzip symbols"
      end

      (value - [:identity]) + [:identity]
    end

    def self.response_encoding(headers, request_encoding, enabled)
      return :identity unless enabled == :gzip
      return request_encoding unless headers.key?("accept-encoding")

      tokens = headers.fetch("accept-encoding").flat_map { |entry| entry.split(",") }
      tokens.any? { |token| token.strip.casecmp?("gzip") } ? :gzip : :identity
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

    def self.error_body(error, limit) # rubocop:disable Metrics/AbcSize
      body = JSON.generate(error.to_h)
      if body.bytesize <= limit
        yield error, error.code if block_given?
        return [body, error.http_status]
      end

      error = Error.new(:resource_exhausted, "Error exceeds byte limit")
      body = JSON.generate(error.to_h)
      emitted = body.bytesize <= limit
      yield error, (error.code if emitted) if block_given?
      [emitted ? body : "", error.http_status]
    end

    def self.parse_error(status, headers, body)
      values = { metadata: metadata(headers), trailers: metadata(headers, trailers: true) }
      if content_codec(single(headers, "content-type")) == :json
        wire = wire_error(body, fallback: Error::HTTP_FALLBACK.fetch(status, "unknown"))
        return Error.new(wire.fetch(:code), wire.fetch(:message), details: wire.fetch(:details), **values) if wire
      end
      Error.new(Error::HTTP_FALLBACK.fetch(status, "unknown"), "HTTP #{status}", **values)
    end

    def self.wire_error(body, fallback:)
      object = JSON.parse(body)
      return unless valid_wire_error?(object)

      code = Error::HTTP_STATUS.key?(object["code"]) ? object["code"] : fallback
      { code: code, message: object.fetch("message", ""), details: wire_details(object.fetch("details", [])) }
    rescue JSON::ParserError, ArgumentError, TypeError, EncodingError
      # Invalid Connect error bodies use the protocol's HTTP fallback table.
      nil
    end

    def self.valid_wire_error?(object)
      return false unless object.is_a?(Hash)

      code = object["code"]
      message = object.fetch("message", "")
      (code.nil? || (code.is_a?(String) && code.valid_encoding?)) &&
        message.is_a?(String) && message.valid_encoding?
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
