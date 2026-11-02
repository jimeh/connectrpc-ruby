# frozen_string_literal: true

module ConnectRPC
  class Metadata
    include Enumerable

    RESERVED = %w[accept accept-encoding accept-post allow connection content-encoding content-length content-type
                  expect host keep-alive proxy-authenticate proxy-authorization proxy-connection te trailer
                  transfer-encoding upgrade].freeze

    def self.reserved?(key)
      RESERVED.include?(key) || key.start_with?("connect-", "trailer-", "grpc-")
    end

    def self.encode_binary(bytes)
      [bytes].pack("m0").delete_suffix("==").delete_suffix("=")
    end

    def self.decode_binary(value)
      raw = base64_payload(value)
      bytes = (raw + ("=" * ((4 - (raw.size % 4)) % 4))).unpack1("m0")
      raise ArgumentError, "Noncanonical base64" unless encode_binary(bytes) == raw

      bytes
    end

    def self.base64_payload(value)
      raise ArgumentError, "Malformed base64" unless value.is_a?(String) && %r{\A[A-Za-z0-9+/]*={0,2}\z}.match?(value)

      raw = value.delete_suffix("==").delete_suffix("=")
      raise ArgumentError, "Malformed base64" if raw.size % 4 == 1
      raise ArgumentError, "Malformed base64 padding" if value.include?("=") && value.size % 4 != 0

      raw
    end
    private_class_method :base64_payload

    def initialize(values = {})
      @values = {}
      values.each { |key, entries| self[key] = entries }
    end

    def [](key)
      @values[key.downcase]&.dup
    end

    def []=(key, entries)
      key = validate_key(key)
      @values[key] = Array(entries).map { |value| validate_value(value) }.freeze
    end

    def add(key, value)
      self[key] = (self[key] || []) + [value]
      self
    end

    def each(&)
      @values.each(&)
    end

    def to_h
      @values.dup
    end

    def add_binary(key, bytes)
      raise ArgumentError, "Binary metadata keys must end in -bin" unless key.end_with?("-bin")

      add(key, self.class.encode_binary(bytes))
    end

    def binary(key)
      (self[key] || []).flat_map do |value|
        parts = value.empty? ? [""] : value.split(",", -1)
        parts.map { |part| self.class.decode_binary(part.strip) }
      end
    end

    private

    def validate_key(key)
      unless key.is_a?(String) && /\A[0-9A-Za-z_.-]+\z/.match?(key) && !self.class.reserved?(key.downcase)
        raise ArgumentError, "Invalid or reserved metadata key"
      end

      key.downcase.freeze
    end

    def validate_value(value)
      unless value.is_a?(String) && value.ascii_only? && /\A[\x20-\x7e]*\z/.match?(value)
        raise ArgumentError, "Metadata values must be printable ASCII header strings"
      end

      value.dup.freeze
    end
  end
end
