# frozen_string_literal: true

require "zlib"

module ConnectRPC
  # Retains bounded output from native gzip streams. Each native output callback
  # yields at most 16 KiB on the supported default Zlib versions.
  class Compression
    class Malformed < StandardError; end

    CHUNK_SIZE = 16_384

    def self.encode(bytes, limit:, deadline: nil)
      new(limit, deadline).encode(bytes)
    end

    def self.decode(bytes, limit:, deadline: nil)
      new(limit, deadline).decode(bytes)
    end

    def initialize(limit, deadline)
      @limit = limit
      @deadline = deadline
      @output = +"".b
    end

    def encode(bytes)
      stream = Zlib::Deflate.new(Zlib::DEFAULT_COMPRESSION, Zlib::MAX_WBITS + 16)
      chunks(bytes) { |chunk| step { stream.deflate(chunk) { |output| append(output) } } }
      step { stream.finish { |output| append(output) } }
      @output
    ensure
      stream&.close
    end

    def decode(bytes)
      cursor = 0
      while cursor < bytes.bytesize
        @deadline&.check!
        cursor = member(bytes, cursor)
      end
      @deadline&.check!
      @output
    rescue Zlib::DataError, Zlib::BufError
      raise Malformed, "Malformed gzip content"
    end

    private

    def chunks(bytes)
      cursor = 0
      while cursor < bytes.bytesize
        chunk = bytes.byteslice(cursor, CHUNK_SIZE)
        yield chunk
        cursor += chunk.bytesize
      end
    end

    def member(bytes, cursor)
      stream = Zlib::Inflate.new(Zlib::MAX_WBITS + 16)
      until stream.finished?
        raise Malformed, "Malformed gzip content" if cursor == bytes.bytesize

        cursor += inflate_chunk(stream, bytes.byteslice(cursor, CHUNK_SIZE))
      end
      # finish may return raw trailing input. Only inflate's validated block
      # output belongs in the payload; the cursor starts the next gzip member.
      cursor
    ensure
      stream&.close
    end

    def inflate_chunk(stream, chunk)
      before = stream.total_in
      step { stream.inflate(chunk) { |output| append(output) } }
      consumed = stream.total_in - before
      raise Malformed, "Malformed gzip content" unless consumed.positive?

      consumed
    end

    def step
      @deadline&.check!
      begin
        yield
      ensure
        @deadline&.check!
      end
    end

    def append(chunk)
      @deadline&.check!
      raise Error.new(:resource_exhausted, "Message exceeds byte limit") if @output.bytesize + chunk.bytesize > @limit

      @output << chunk
      @deadline&.check!
    end
  end
end
