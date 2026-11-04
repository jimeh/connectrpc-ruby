# frozen_string_literal: true

module ConnectRPC
  # Server-only wire policy. Native gRPC's final HEADERS and client completion
  # rules do not apply to a Web body terminal.
  module GRPCWeb
    # @type var types: Hash[representation, String]
    types = { binary: "application/grpc-web+proto", text: "application/grpc-web-text+proto" }
    CONTENT_TYPES = types.freeze
    ACCEPT_LIMIT = 8192
    ACCEPT_ENTRIES = 32

    def self.intent?(headers)
      (headers["content-type"] || []).any? { |value| value.downcase.start_with?("application/grpc-web") }
    end

    def self.media(value)
      return :binary if value&.match?(%r{\Aapplication/grpc-web(?:\+proto)?\z}i)
      return :text if value&.match?(%r{\Aapplication/grpc-web-text(?:\+proto)?\z}i)

      nil
    end

    def self.representation(headers, request) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
      values = headers.fetch("accept", [])
      return if values.sum(&:bytesize) > ACCEPT_LIMIT
      return request if values.empty?

      entries = values.flat_map { |value| value.empty? ? [""] : value.split(",", -1) }
      return if entries.size > ACCEPT_ENTRIES

      entries.reject! { |entry| entry.strip.empty? }
      return request if entries.empty?

      parsed = entries.map { |entry| accept_entry(entry) }
      return if parsed.any?(&:nil?)

      entries = parsed.compact
      other = (request == :text ? :binary : :text) #: representation
      selected = quality(entries, request) >= quality(entries, other) ? request : other
      selected if quality(entries, selected).positive?
    end

    def self.accept_entry(entry) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
      token, *parameters = entry.strip.downcase.split(/\s*;\s*/, -1)
      return unless token && %r{\A[!#$%&'*+.^_`|~0-9a-z-]+/[!#$%&'*+.^_`|~0-9a-z-]+\z}.match?(token)
      return [token, 0] unless media(token) || %w[application/* */*].include?(token)

      weights = parameters.grep(/\Aq(?:=|\s|\z)/)
      return if weights.size > 1

      weight = weights.first
      match = /\Aq=(0(?:\.[0-9]{0,3})?|1(?:\.0{0,3})?)\z/.match(weight) if weight
      return if weight && !match
      # Our representations have no media parameters. Other parameters cannot
      # match them, but do not invalidate a separate satisfiable list element.
      return ["", 0] unless parameters.size == weights.size

      [token, match ? (match[1].to_f * 1000).round : 1000]
    end

    def self.quality(entries, candidate) # rubocop:disable Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
      matches = entries.filter_map do |entry|
        token, quality = entry
        specificity = if media(token) == candidate
                        2
                      elsif token == "application/*"
                        1
                      elsif token == "*/*"
                        0
                      end
        [specificity, quality] if specificity
      end
      matches.max&.last || 0
    end

    def self.vary(headers, *names)
      tokens = headers.fetch("vary", []).flat_map { |value| value.split(",").map(&:strip) }
      return headers if tokens.include?("*")

      names.each { |name| tokens << name unless tokens.any? { |token| token.casecmp?(name) } }
      headers["vary"] = tokens
      headers
    end

    def self.capabilities!(capabilities)
      return if capabilities.is_a?(Array) && !capabilities.empty? && capabilities.uniq.size == capabilities.size &&
                capabilities.all? { |value| %i[unary server_stream].include?(value) }

      raise ArgumentError, "Web capabilities must be distinct unary/server_stream symbols"
    end

    def self.terminal(error, trailers, codec:, wire_limit:, limit:, on_error:) # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      fields, effective = GRPCProtocol.terminal(error, trailers, codec: codec, wire_limit: wire_limit, limit: limit,
                                                                 on_error: on_error)
      bytes = fields.flat_map { |key, values| values.map { |value| "#{key}: #{value}\r\n" } }.join.b
      if bytes.bytesize > wire_limit
        fields, effective = GRPCProtocol.terminal(Error.new(:resource_exhausted, "Status exceeds byte limit"),
                                                  Metadata.new, codec: codec, wire_limit: wire_limit, limit: limit)
        bytes = "grpc-status: #{fields.fetch('grpc-status').first}\r\n".b
        Protocol.check_size!(bytes, wire_limit)
      end
      [[128, bytes.bytesize].pack("CN") + bytes, effective]
    rescue Error
      raise
    rescue StandardError => e
      on_error&.call(e)
      terminal(Error.new(:internal, "Cannot encode gRPC status"), Metadata.new, codec: codec,
                                                                                wire_limit: wire_limit, limit: limit,
                                                                                on_error: nil)
    end

    class Framing < Streaming::Framing
      private

      def validate_flags(flags)
        protocol_error("Invalid Web envelope flags") unless [0, 1].include?(flags)
        raise Error.new(:internal, "Unnegotiated compression"), cause: nil if flags == 1
      end
    end

    class Strategy
      def grpc? = false
      def framing = Framing
      def wait(timeout) = [timeout || Deadline::MAX_NATIVE_WAIT, Deadline::MAX_NATIVE_WAIT].min
      def single_response?(method) = method.is_a?(Method)
      def decode_error(_server) = :invalid_argument
    end
  end
  private_constant :GRPCWeb
end
