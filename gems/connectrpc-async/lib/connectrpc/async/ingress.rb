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

      # A server request body made no progress within request_body_timeout.
      class RequestBodyTimeout < IOError; end

      class Ingress < ::Protocol::HTTP::Body::Writable
        def initialize(stream, length)
          super(length, queue: IngressQueue.new)
          @stream = stream
          @remaining = length
          @retained = 0
          @progress_clock = nil
          @progress_seconds = nil
        end

        # Bounds each read that waits for body bytes to `seconds` on `clock`.
        # Only body bytes or the end of the body end a wait, so empty or
        # padding-only DATA does not extend it. On expiry the stream is reset
        # with CANCEL and the read raises RequestBodyTimeout.
        def bound_progress(clock, seconds)
          @progress_clock = clock
          @progress_seconds = seconds
        end

        def write(bytes)
          @retained += bytes.bytesize
          super
        end

        def read
          timer = progress_timer if @progress_seconds && !@error && @queue.empty? && !@queue.closed?
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
        ensure
          Internal.cancel(timer)
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

        # Lives only inside one blocked read, so EOF, reset, close and
        # retirement all end it with that read.
        def progress_timer
          timer = ::Async::Task.new(::Async::Task.current, finished: false) do
            @progress_clock.sleep(@progress_seconds)
            stalled
          end
          timer.run
          timer
        end

        # Never yields, so the waiting read cannot cancel it part way.
        def stalled
          stream = @stream
          return unless stream

          @error ||= RequestBodyTimeout.new("HTTP/2 request body made no progress within request_body_timeout")
          stream.input_stalled
          @retained = 0
          @queue.clear
          @queue.close
        end

        private :progress_timer, :stalled

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
