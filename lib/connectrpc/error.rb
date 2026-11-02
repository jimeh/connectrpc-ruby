# frozen_string_literal: true

module ConnectRPC
  class CodecError < StandardError; end

  class ErrorDetail
    attr_reader :type, :value

    def initialize(type:, value:)
      raise ArgumentError, "Invalid detail type" unless type.is_a?(String) && Method::QUALIFIED_NAME.match?(type)
      raise ArgumentError, "Detail value must be binary bytes" unless value.is_a?(String)

      @type = type.dup.freeze
      @value = value.b.freeze
      freeze
    end

    def decode(binding:, type:)
      binding.decode(value, type: type, codec: :proto)
    end

    def to_h
      { "type" => type, "value" => Metadata.encode_binary(value) }
    end
  end

  class Error < StandardError
    HTTP_STATUS = {
      "canceled" => 499, "unknown" => 500, "invalid_argument" => 400, "deadline_exceeded" => 504,
      "not_found" => 404, "already_exists" => 409, "permission_denied" => 403, "resource_exhausted" => 429,
      "failed_precondition" => 400, "aborted" => 409, "out_of_range" => 400, "unimplemented" => 501,
      "internal" => 500, "unavailable" => 503, "data_loss" => 500, "unauthenticated" => 401
    }.freeze
    HTTP_FALLBACK = {
      400 => "internal", 401 => "unauthenticated", 403 => "permission_denied", 404 => "unimplemented",
      429 => "unavailable", 502 => "unavailable", 503 => "unavailable", 504 => "unavailable"
    }.freeze

    attr_reader :code, :details, :metadata, :trailers

    def initialize(code, message = "", details: [], metadata: {}, trailers: {})
      code = code.to_s
      raise ArgumentError, "Unknown Connect error code" unless HTTP_STATUS.key?(code)
      unless details.is_a?(Array) && details.all?(ErrorDetail)
        raise ArgumentError, "Details must be ErrorDetail objects"
      end

      super(safe_message(message))
      @code = code.dup.freeze
      @details = details.dup.freeze
      @metadata = Metadata.new(metadata)
      @trailers = Metadata.new(trailers)
    end

    def safe_message(message)
      raise ArgumentError, "Error message must be UTF-8 text" unless message.is_a?(String) && message.valid_encoding?

      message.encode(Encoding::UTF_8)
    rescue EncodingError
      raise ArgumentError, "Error message must be UTF-8 text"
    end
    private :safe_message

    def http_status
      HTTP_STATUS.fetch(code)
    end

    def to_h
      { "code" => code, "message" => message, "details" => details.map(&:to_h) }
    end
  end
end
