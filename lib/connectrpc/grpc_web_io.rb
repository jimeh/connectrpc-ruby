# frozen_string_literal: true

module ConnectRPC
  module GRPCWeb
    # Decode each complete quartet independently: padding is permitted at every
    # flush boundary, including inside a frame prefix or protobuf message.
    class Input
      def initialize(input, representation:, wire_limit:, headers:) # rubocop:disable Metrics/MethodLength
        @input = input
        @text = representation == :text
        @encoded_limit = (@text ? 4 : 1) * (5 + wire_limit)
        @decoded_limit = 5 + wire_limit
        length = @length = Protocol.content_length!(headers["content-length"], invalid_code: :invalid_argument)
        if length && length > @encoded_limit
          raise Error.new(:resource_exhausted,
                          "Encoded request exceeds byte limit")
        end

        @encoded = @decoded = 0
        @carry = +"".b
        @remainder = +"".b
        @eof = false
      end

      def read(length, timeout: nil) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
        while @remainder.empty? && !@eof
          amount = @text ? [((length + 2) / 3) * 4, 16_384].min : [length, 16_384].min
          bytes = @input&.read([amount, @encoded_limit - @encoded + 1].min, timeout: timeout)
          if bytes.nil? || bytes.empty?
            finish_input
          else
            @encoded += bytes.bytesize
            Protocol.check_size!(bytes, amount)
            if @encoded > @encoded_limit
              raise Error.new(:resource_exhausted, "Encoded request exceeds byte limit"), cause: nil
            end

            @remainder = @text ? decode(bytes) : bytes
            @decoded += @remainder.bytesize
            if @decoded > @decoded_limit
              raise Error.new(:resource_exhausted, "Framed request exceeds byte limit"), cause: nil
            end
          end
        end
        return if @remainder.empty?

        result = @remainder.byteslice(0, length) #: String
        @remainder = @remainder.byteslice(result.bytesize..) #: String
        result
      end

      private

      def decode(bytes) # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
        bytes = @carry + bytes
        count = bytes.bytesize - (bytes.bytesize % 4)
        @carry = bytes.byteslice(count..) #: String
        decoded = +"".b
        offset = 0
        while offset < count
          group = bytes.byteslice(offset, 4) #: String
          unless %r{\A(?:[A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)\z}.match?(group)
            raise Error.new(:invalid_argument, "Malformed Web base64"), cause: nil
          end

          value = group.unpack1("m0") #: String
          raise Error.new(:invalid_argument, "Noncanonical Web base64"), cause: nil unless [value].pack("m0") == group

          decoded << value
          offset += 4
        end
        decoded
      rescue ArgumentError
        raise Error.new(:invalid_argument, "Malformed Web base64"), cause: nil
      end

      def finish_input
        unless @carry.empty? && (@length.nil? || @length == @encoded)
          raise Error.new(:invalid_argument, "Truncated Web request"), cause: nil
        end

        @eof = true
      end
    end

    class Output
      def initialize(output, representation)
        @output = output
        @text = representation == :text
      end

      def write(bytes, timeout: nil)
        size = @text ? 12_288 : 16_384
        offset = 0
        while offset < bytes.bytesize
          fragment = bytes.byteslice(offset, size) #: String
          @output.write(@text ? [fragment].pack("m0") : fragment, timeout: timeout)
          offset += fragment.bytesize
        end
        nil
      end

      def finish(terminal:)
        @output.finish(terminal: @text ? [terminal].pack("m0") : terminal)
      end

      def abort = @output.abort
    end

    # Portable unary materializes one bounded response, never a native stream.
    class Aggregate
      attr_reader :body

      def initialize(limit)
        @limit = limit
        @body = +"".b
      end

      def write(bytes, timeout: nil) # rubocop:disable Lint/UnusedMethodArgument -- synchronous bounded append
        if @body.bytesize + bytes.bytesize > @limit
          raise Error.new(:resource_exhausted,
                          "Aggregate Web response exceeds byte limit")
        end

        @body << bytes
        nil
      end

      def finish(terminal:)
        @terminal = terminal
        nil
      end

      def complete
        terminal = @terminal
        write(terminal) if terminal
      end

      def abort
        @body.clear
        @terminal = nil
        nil
      end
    end

    class PortableInput
      def initialize(input)
        @input = input
      end

      def read(length, timeout: nil) # rubocop:disable Lint/UnusedMethodArgument -- portable cooperative I/O
        @input&.read(length)
      end
    end
  end
end
