# frozen_string_literal: true

module ConnectRPC
  module Async
    module Internal
      # A producer never waits for the consumer. Stream credit bounds payload;
      # coalescing bounds entries independently of the peer's frame sizes.
      class IngressQueue
        CHUNK_SIZE = 16_384

        def initialize
          @chunks = []
          @closed = false
          @changed = ::Async::Condition.new
        end

        def push(bytes)
          raise ::Protocol::HTTP::Body::Writable::Closed if @closed

          offset = 0
          while offset < bytes.bytesize
            tail = @chunks.last
            if !tail || tail.bytesize == CHUNK_SIZE
              tail = +"".b
              @chunks << tail
            end
            size = [CHUNK_SIZE - tail.bytesize, bytes.bytesize - offset].min
            tail << bytes.byteslice(offset, size)
            offset += size
          end
          @changed.signal
        end

        def pop
          @changed.wait while @chunks.empty? && !@closed
          @chunks.shift
        end

        def clear = @chunks.clear
        def empty? = @chunks.empty?
        def closed? = @closed

        def close
          @closed = true
          @changed.signal
        end
      end

      class Ingress < ::Protocol::HTTP::Body::Writable
        def initialize(stream, length)
          super(length, queue: IngressQueue.new)
          @stream = stream
          @remaining = length
          @retained = 0
        end

        def write(bytes)
          @retained += bytes.bytesize
          super
        end

        def read
          chunk = super
          if chunk
            @retained -= chunk.bytesize
            @remaining -= chunk.bytesize if @remaining
            @stream&.reclaim_input(chunk.bytesize)
          elsif @remaining && !@remaining.zero?
            direction = @remaining.positive? ? "short" : "over"
            raise EOFError, "Expected #{length} bytes, #{@remaining} bytes #{direction}!"
          end
          chunk
        end

        def close(error = nil)
          stream = @stream
          @stream = nil
          discarded = @retained
          @retained = 0
          super
          return unless stream

          stream.finish_input(self, error)
          stream.reclaim_input(discarded) unless error
        end

        # A normal producer EOF preserves preceding data. A failed producer
        # releases it immediately, and both paths detach the retired stream.
        def retire(error = nil, discard: false)
          @stream = nil
          @error ||= error
          return unless error || discard

          @retained = 0
          @queue.clear
        end
      end
    end
  end
end
