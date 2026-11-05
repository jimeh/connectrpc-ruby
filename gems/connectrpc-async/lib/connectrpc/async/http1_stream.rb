# frozen_string_literal: true

require "io/stream/buffered"

module ConnectRPC
  module Async
    module Internal
      # Unlike IO::Stream's coupled read path, an H1 response must remain
      # readable while the request writer holds its output mutex.
      class HTTP1Stream < ::IO::Stream::Buffered
        def initialize(io)
          super(io, minimum_read_size: 16_384, maximum_read_size: 16_384)
        end

        def readpartial(length, buffer = nil)
          super([length, 16_384].min, buffer)
        end

        def close_read
          # Destructive retirement closes raw IO before unfinished body cleanup.
          super unless closed?
        end

        def viable?
          return false if closed? || @finished || !@read_buffer.empty?

          result = sysread_nonblock(1, @input_buffer)
          result == :wait_readable
        rescue IOError, SystemCallError, OpenSSL::SSL::SSLError
          false
        end

        private

        # Keep pinned buffer/EOF behavior without acquiring the writer's mutex.
        def fill_read_buffer(size = @minimum_read_size) # rubocop:disable Naming/PredicateMethod -- Pinned upstream override.
          size = [size, @maximum_read_size].min
          if @read_buffer.empty?
            return true if sysread(size, @read_buffer)
          elsif (chunk = sysread(size, @input_buffer))
            @read_buffer << chunk
            return true
          end
          @finished = true
          false
        end

        # Buffered#sysread in io-stream 0.14 swallows SSL errors other than
        # unexpected EOF. Preserve those native failures rather than returning
        # a clean close-delimited HTTP EOF.
        def sysread(size, buffer)
          loop do
            result = @io.read_nonblock(size, buffer, exception: false)
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
