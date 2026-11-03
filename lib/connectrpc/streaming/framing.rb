# frozen_string_literal: true

module ConnectRPC
  module Streaming
    # @type var types: Hash[codec, String]
    types = { proto: "application/connect+proto", json: "application/connect+json" }
    CONTENT_TYPES = types.freeze
    TERMINAL_LIMIT = 65_536
    FRAGMENT_SIZE = 16_384
    RECOVERY_LIMIT = 262_144
    RECOVERY_SECONDS = 0.1
    TAIL_SECONDS = 1.0

    def self.codec(content_type)
      return :proto if content_type&.match?(%r{\Aapplication/connect\+proto\z}i)
      return :json if content_type&.match?(%r{\Aapplication/connect\+json\z}i)

      nil
    end

    # Streaming content coding uses distinct protocol headers; reuse only the
    # validated unary negotiation rules, never whole-stream compression.
    def self.coding_headers(headers)
      result = headers.except("content-encoding", "accept-encoding")
      result["content-encoding"] = headers["connect-content-encoding"] if headers.key?("connect-content-encoding")
      result["accept-encoding"] = headers["connect-accept-encoding"] if headers.key?("connect-accept-encoding")
      result
    end

    class Framing
      attr_reader :messages, :terminal
      attr_writer :encoding

      def initialize(input:, request:, wire_limit:, limit:, output: nil, encoding: :identity,
                     terminal_wire_limit: TERMINAL_LIMIT, terminal_limit: TERMINAL_LIMIT, state: nil)
        @input = input
        @output = output
        @request = request
        @wire_limit = wire_limit
        @limit = limit
        @encoding = encoding
        @terminal_wire_limit = terminal_wire_limit
        @terminal_limit = terminal_limit
        initialize_state(state)
      end

      def read_frame(timeout: nil, skip_data: false)
        prefix = exact(5, eof: true, timeout: timeout)
        return if prefix.nil?

        flags, length = prefix.unpack("CN") #: [Integer, Integer]
        validate_envelope(flags, length)
        bytes = payload(flags, length, timeout: timeout, skip_data: skip_data)
        @terminal = true if flags.anybits?(2)
        @messages += 1 unless @terminal
        [flags, bytes]
      end

      def emit(bytes, terminal: false)
        flags, bytes = prepare_payload(bytes, terminal)
        @state&.check!
        output = @output #: _StreamingWriter
        output.write([flags, bytes.bytesize].pack("CN"))
        emit_fragments(bytes, output)
        nil
      end

      private

      def initialize_state(state)
        @state = state
        @messages = 0
        @terminal = false
      end

      def validate_envelope(flags, length)
        validate_flags(flags)
        bound = flags.anybits?(2) ? @terminal_wire_limit : @wire_limit
        raise Error.new(:resource_exhausted, "Envelope exceeds wire byte limit"), cause: nil if length > bound
      end

      def validate_flags(flags)
        protocol_error("Invalid envelope flags") unless [0, 1, 2, 3].include?(flags)
        protocol_error("Request terminal is forbidden") if flags.anybits?(2) && @request
        protocol_error("Envelope after terminal") if @terminal
        raise Error.new(:internal, "Unnegotiated compression"), cause: nil if flags.anybits?(1) && @encoding != :gzip
      end

      def payload(flags, length, timeout:, skip_data:)
        return skip_payload(length, timeout) if skip_data && !flags.anybits?(2)

        bytes = exact(length, timeout: timeout) #: String
        bound = flags.anybits?(2) ? @terminal_limit : @limit
        bytes = inflate(bytes, bound) if flags.anybits?(1)
        Protocol.check_size!(bytes, bound)
        bytes
      end

      def inflate(bytes, bound)
        protocol_error("Empty compressed envelope") if bytes.empty?
        Compression.decode(bytes, limit: bound, deadline: @state)
      rescue Compression::Malformed, Zlib::Error
        protocol_error("Malformed compressed envelope")
      end

      def skip_payload(length, timeout)
        remaining = length
        while remaining.positive?
          chunk = exact([remaining, FRAGMENT_SIZE].min, timeout: timeout) #: String
          remaining -= chunk.bytesize
        end
        ""
      end

      def protocol_error(message)
        raise Error.new(@request ? :invalid_argument : :internal, message), cause: nil
      end
    end
  end
  private_constant :Streaming
end
