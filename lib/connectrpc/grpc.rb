# frozen_string_literal: true

module ConnectRPC
  # Separate from StreamHead: HTTP/2 END_STREAM is a protocol fact, not body state.
  class GRPCHead
    attr_reader :status, :headers, :end_stream

    def initialize(status:, headers:, end_stream:)
      @status = status
      @headers = GRPCProtocol.snapshot(headers)
      @end_stream = end_stream
      freeze
    end
  end

  module GRPCProtocol # rubocop:disable Metrics/ModuleLength
    CONTENT_TYPE = "application/grpc+proto"
    FIELD_LIMIT = 65_536
    UINT32_MAX = 4_294_967_295
    CODES = ["OK", *Error::HTTP_STATUS.keys].freeze
    TIME_UNITS = { "n" => 1e-9, "u" => 1e-6, "m" => 0.001, "S" => 1, "M" => 60, "H" => 3600 }.freeze

    def self.media?(value)
      !!value&.match?(%r{\Aapplication/grpc(?:\+proto)?\z}i)
    end

    def self.intent?(headers)
      (headers["content-type"] || []).any? { |value| value.downcase.start_with?("application/grpc") }
    end

    def self.snapshot(headers)
      headers.to_h { |key, values| [key.dup.freeze, values.map { |value| value.dup.freeze }.freeze] }.freeze
    end

    def self.field_size(headers)
      headers.sum { |key, values| values.sum { |value| key.bytesize + value.bytesize + 32 } }
    end

    def self.check_fields!(headers, limit = FIELD_LIMIT)
      return unless field_size(headers) > limit

      raise Error.new(:resource_exhausted, "gRPC field block exceeds byte limit"),
            cause: nil
    end

    # Parse incrementally: the budget bounds spelling length, uint32 bounds value.
    def self.status_number(value)
      return if value.empty?

      number = 0
      value.each_byte do |byte|
        return nil unless byte.between?(48, 57)

        digit = byte - 48
        return nil if number > (UINT32_MAX - digit) / 10

        number = (number * 10) + digit
      end
      number
    end

    def self.status_value(headers)
      values = headers["grpc-status"]
      return unless values
      raise Error.new(:internal, "Duplicate grpc-status"), cause: nil unless values.size == 1

      status_number(values.first)
    end

    def self.encode_message(message)
      message.b.each_byte.map { |byte| byte.between?(32, 126) && byte != 37 ? byte.chr : format("%%%02X", byte) }.join
    end

    def self.decode_message(value)
      value.b.gsub(/%([0-9a-fA-F]{2})/n) { [(Regexp.last_match(1) || "").to_i(16)].pack("C") }.force_encoding(Encoding::UTF_8).scrub
    end

    def self.parse_status(headers, codec:, http_status: 200, metadata: Metadata.new, # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
                          wire_limit: FIELD_LIMIT, limit: FIELD_LIMIT, on_error: nil)
      check_fields!(headers, wire_limit)
      trailers = Protocol.metadata(headers).freeze
      number = status_value(headers)
      unless headers.key?("grpc-status")
        failure = status_error(Error::HTTP_FALLBACK.fetch(http_status, "unknown"), "Missing grpc-status", [], metadata,
                               trailers)
        return [failure, trailers]
      end
      message_fields = headers["grpc-message"] || []
      raise Error.new(:internal, "Duplicate grpc-message"), cause: nil if message_fields.size > 1

      details = headers["grpc-status-details-bin"]
      rich = decode_status(details, number, codec, limit, on_error) if details
      return [nil, trailers] if number&.zero?

      code = (number && CODES[number]) || "unknown"
      message = rich ? rich.message : decode_message(message_fields.first || "")
      [status_error(code, message, rich ? rich.details : [], metadata, trailers), trailers]
    end

    def self.status_error(code, message, details, metadata, trailers)
      error = Error.new(code, message, details: details, metadata: metadata, trailers: trailers)
      error.metadata.freeze
      error.trailers.freeze
      error
    end

    def self.decode_status(values, number, codec, limit, on_error)
      unless number && number != 0 && values.size == 1 && codec
        raise Error.new(:internal, "Invalid gRPC status details"), cause: nil
      end

      bytes = Metadata.decode_binary(values.first)
      Protocol.check_size!(bytes, limit)
      codec.decode_status(bytes, code: number)
    rescue CodecError, ArgumentError => e
      on_error&.call(e)
      raise Error.new(:internal, "Invalid gRPC status details"), cause: nil
    end

    def self.status_fields(error, trailers, codec, limit) # rubocop:disable Metrics/AbcSize
      fields = trailers.to_h.merge("grpc-status" => [(error ? CODES.index(error.code) : 0).to_s])
      fields["grpc-message"] = [encode_message(error.message)] if error && !error.message.empty?
      if error && !error.details.empty?
        bytes = codec.encode_status(error)
        Protocol.check_size!(bytes, limit)
        fields["grpc-status-details-bin"] = [Metadata.encode_binary(bytes)]
      end
      fields
    end

    def self.terminal(error, trailers, codec:, wire_limit:, limit:, on_error: nil) # rubocop:disable Metrics/MethodLength
      fields = status_fields(error, trailers, codec, limit)
      check_fields!(fields, [wire_limit, FIELD_LIMIT].min)
      [snapshot(fields), error]
    rescue CodecError => e
      on_error&.call(e)
      terminal(Error.new(:internal, "Cannot encode gRPC status"), Metadata.new, codec: codec,
                                                                                wire_limit: wire_limit, limit: limit)
    rescue Error => e
      raise unless e.code == "resource_exhausted"

      replacement = Error.new(:resource_exhausted, "Status exceeds byte limit")
      fields = { "grpc-status" => ["8"] }
      check_fields!(fields, [wire_limit, FIELD_LIMIT].min)
      [snapshot(fields), replacement]
    end

    def self.request_deadline(headers) # rubocop:disable Metrics/AbcSize
      values = headers["grpc-timeout"]
      return unless values
      unless values.size == 1 && /\A[0-9]{1,8}[HMSmun]\z/.match?(values.first)
        raise Error.new(:internal, "Invalid grpc-timeout"), cause: nil
      end

      value = values.first
      seconds = value[0...-1].to_i.to_f * TIME_UNITS.fetch(value[-1] || "").to_f
      seconds.zero? ? Deadline.send(:expired) : Deadline.new(seconds)
    end

    def self.timeout(deadline)
      deadline.check!
      seconds = Float(deadline.remaining) #: Float
      TIME_UNITS.each do |unit, scale|
        scaled = seconds / scale
        next unless scaled.finite? && scaled <= 99_999_999

        amount = scaled.ceil
        return "#{[amount, 1].max}#{unit}" if amount <= 99_999_999
      end
      "99999999H"
    end

    def self.encoding(headers, request: false, enabled: :gzip) # rubocop:disable Metrics/CyclomaticComplexity
      values = headers["grpc-encoding"]
      return :identity unless values
      unless values.size == 1 && /\A[!#$%&'*+.^_`|~0-9A-Za-z-]+\z/.match?(values.first)
        raise Error.new(:internal, "Invalid grpc-encoding"), cause: nil
      end
      return :identity if values.first == "identity"
      return :gzip if values.first == "gzip" && enabled == :gzip

      raise Error.new(request ? :unimplemented : :internal, "Unsupported grpc-encoding"), cause: nil
    end

    def self.response_encoding(headers, enabled)
      tokens = (headers["grpc-accept-encoding"] || []).flat_map { |value| value.split(",").map(&:strip) }
      enabled == :gzip && tokens.include?("gzip") ? :gzip : :identity
    end

    def self.status_codec!(binding, explicit)
      candidate = explicit || binding #: (_Binding | _GRPCStatusCodec) & _GRPCCapabilityProbe
      if candidate.respond_to?(:encode_status) && candidate.respond_to?(:decode_status)
        return explicit || binding #: _GRPCStatusCodec
      end

      raise ArgumentError, "gRPC requires a status codec"
    end
  end
  private_constant :GRPCProtocol
end
