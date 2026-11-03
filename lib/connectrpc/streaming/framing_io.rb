# frozen_string_literal: true

module ConnectRPC
  module Streaming
    class Framing
      private

      def prepare_payload(bytes, terminal)
        bound = terminal ? @terminal_wire_limit : @wire_limit
        Protocol.check_size!(bytes, terminal ? @terminal_limit : @limit)
        flags = terminal ? 2 : 0
        if !terminal && @encoding == :gzip
          bytes = Compression.encode(bytes, limit: bound, deadline: @state)
          flags = 1
        end
        check_output_size(bytes, bound)
        [flags, bytes]
      end

      def check_output_size(bytes, bound)
        Protocol.check_size!(bytes, bound)
        return if bytes.bytesize <= 0xffff_ffff

        raise Error.new(:resource_exhausted, "Envelope exceeds uint32 length"), cause: nil
      end

      def emit_fragments(bytes, output)
        offset = 0
        while offset < bytes.bytesize
          @state&.check!
          fragment = bytes.byteslice(offset, FRAGMENT_SIZE) #: String
          output.write(fragment)
          offset += fragment.bytesize
        end
      end

      def exact(length, eof: false, timeout: nil)
        bytes = +"".b
        while bytes.bytesize < length
          chunk = read_chunk(length - bytes.bytesize, timeout)
          if chunk.nil? || chunk.empty?
            return if eof && bytes.empty?

            protocol_error("Truncated envelope")
          end
          bytes << chunk
        end
        bytes
      end

      def read_chunk(length, timeout)
        @state&.check!
        input = @input #: _StreamingInput
        chunk = input.read([length, FRAGMENT_SIZE].min, timeout: timeout)
        @state&.check!
        protocol_error("Input exceeded requested fragment") if chunk && chunk.bytesize > length
        chunk
      end
    end
  end
end
