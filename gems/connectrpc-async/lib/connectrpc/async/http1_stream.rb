# frozen_string_literal: true

require "io/stream"
require "openssl"

module ConnectRPC
  module Async
    module Internal
      # A buffered HTTP/1 stream over a socket's public IO API. Unlike
      # IO::Stream's coupled read path, reads never flush or take the writer's
      # mutex, so a response stays readable while a request writer is blocked
      # mid-upload.
      class HTTP1Stream
        READ_SIZE = 16_384

        # `resource`, when given, reports a local raw failure through
        # `aborted?`; the descriptor stays open until its final close.
        def initialize(io, resource = nil)
          @io = io
          @resource = resource
          @finished = false
          @read_buffer = String.new(encoding: Encoding::BINARY)
          # Scratch destination for reads appended to a non-empty read buffer.
          @input_buffer = String.new(encoding: Encoding::BINARY)
          @write_buffer = String.new(encoding: Encoding::BINARY)
          @writing = Thread::Mutex.new
        end

        def closed? = @io.closed?
        def failed? = closed? || !!@resource&.aborted?

        # Like IO::Stream 0.14, the limit is compared with the separator's
        # start, so a returned line may exceed it by the separator.
        def gets(separator, limit = nil)
          offset = 0
          until (index = @read_buffer.index(separator, offset))
            # Resume just before the tail so a separator split across reads matches.
            offset = [@read_buffer.bytesize - separator.bytesize + 1, 0].max
            return consume(limit) if limit && offset >= limit
            return consume unless fill_read_buffer
          end
          return consume(limit) if limit && index >= limit

          consume(index + separator.bytesize)
        end

        def read(size)
          return String.new(encoding: Encoding::BINARY) if size.zero?

          fill_read_buffer until @finished || @read_buffer.bytesize >= size
          consume(size)
        end

        def readpartial(length)
          return String.new(encoding: Encoding::BINARY) if length.zero?

          fill_read_buffer if !@finished && @read_buffer.empty?
          consume([length, READ_SIZE].min) or raise EOFError, "Stream finished before reading enough data!"
        end

        def write(string, flush: false)
          @writing.synchronize do
            @write_buffer << (string.encoding == Encoding::BINARY ? string : string.b)
            drain if flush || @write_buffer.bytesize >= ::IO::Stream::MINIMUM_WRITE_SIZE
          end
          string.bytesize
        end

        def flush
          return if @write_buffer.empty?

          @writing.synchronize { drain }
        end

        # Best effort flush; interruptions still propagate after the close.
        def close
          return if closed?

          begin
            flush
          rescue StandardError
            nil
          ensure
            @io.close
          end
        end

        def close_write
          flush
        ensure
          @io.close_write
        end

        def close_read
          # Destructive retirement fails raw IO before unfinished body cleanup
          # and closes it only after every task has joined.
          @io.close_read unless failed?
        end

        def viable?
          return false if failed? || @finished || !@read_buffer.empty?

          @io.read_nonblock(1, @input_buffer, exception: false) == :wait_readable
        rescue IOError, SystemCallError, OpenSSL::SSL::SSLError
          false
        end

        private

        # Returns the bytes read, or nil once the peer has finished.
        def fill_read_buffer
          if @read_buffer.empty?
            chunk = sysread(@read_buffer)
          elsif (chunk = sysread(@input_buffer))
            @read_buffer << chunk
          end
          @finished = true unless chunk
          chunk
        end

        # Returns at most size buffered bytes, or nil once finished and empty.
        def consume(size = nil)
          return if @finished && @read_buffer.empty?

          if size.nil? || size >= @read_buffer.bytesize
            result = @read_buffer
            @read_buffer = String.new(encoding: Encoding::BINARY)
          else
            # Freezing first lets byteslice share instead of copying.
            @read_buffer.freeze
            result = @read_buffer.byteslice(0, size)
            @read_buffer = @read_buffer.byteslice(size, @read_buffer.bytesize)
          end
          result
        end

        def drain
          @io.write(@write_buffer)
        ensure
          # A failed write loses the buffered bytes, as in IO::Stream.
          @write_buffer.clear
        end

        # Raise every native read failure except a TLS peer's unexpected EOF,
        # which maps to a connection reset like ECONNRESET. IO::Stream 0.14
        # swallows other SSL errors, which would read as a clean EOF and
        # truncate a close-delimited body.
        def sysread(buffer)
          loop do
            result = @io.read_nonblock(READ_SIZE, buffer, exception: false)
            case result
            when :wait_readable
              @io.wait_readable(@io.timeout) or raise ::IO::TimeoutError, "read timeout"
            when :wait_writable
              @io.wait_writable(@io.timeout) or raise ::IO::TimeoutError, "write timeout"
            else
              return result
            end
          end
        rescue OpenSSL::SSL::SSLError => e
          raise ::IO::Stream::ConnectionResetError, "Connection reset by peer!" if
            e.message.include?("unexpected eof while reading")

          raise
        rescue Errno::ECONNRESET
          raise ::IO::Stream::ConnectionResetError, "Connection reset by peer!"
        rescue Errno::EBADF
          raise IOError, "stream closed"
        end
      end
    end
  end
end
