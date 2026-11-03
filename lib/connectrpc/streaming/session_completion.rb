# frozen_string_literal: true

module ConnectRPC
  module Streaming
    class Session
      # Recovery runs only after the scope relinquishes normal consumption.
      # It cannot steal a live reader, decode skipped data or resume the endpoint.
      def recover_send_failure
        return @remote_error if @eof
        return unless recovery_available?

        @recovering = true
        @recovery_expires ||= now + RECOVERY_SECONDS
        @recovery_remaining ||= RECOVERY_LIMIT
        operation(:reading) { recover_terminal }
      rescue StandardError => e
        @lifecycle&.record_failure(e)
        nil
      end

      private

      def recovery_available? = !@reading.busy && !@server && !@abandoned

      def recover_terminal
        leading
        loop do
          frame = @reader.read_frame(skip_data: true)
          return unless frame
          return recovered_error(frame.last) if frame.first.anybits?(2)

          @received += 1
          return if @method.is_a?(ClientStreamMethod) && @received > 1
        end
      end

      def recovered_error(bytes)
        accept_terminal(bytes)
        nil
      rescue Error => e
        return e if e.equal?(@remote_error) && @eof

        raise
      end

      def accept_terminal(bytes)
        @remote_error, @trailers = parse_terminal(bytes)
        validate_terminal_count
        expires = @recovery_expires || (now + TAIL_SECONDS)
        verify_tail(expires)
        finish_transport(expires)
        @eof = true
        error = @remote_error
        raise error, cause: nil if error

        nil
      end

      def parse_terminal(bytes)
        Streaming.parse_terminal(bytes, metadata: @headers || Metadata.new,
                                        on_error: ->(error) { @lifecycle&.record_failure(error) })
      end

      def validate_terminal_count
        return unless !@remote_error && @method.is_a?(ClientStreamMethod) && @received != 1

        raise Error.new(:unimplemented, "Expected exactly one response"), cause: nil
      end

      def verify_tail(expires)
        extra = read(1, timeout: completion_timeout(expires))
        return unless extra && !extra.empty?

        raise Error.new(:internal, "Body bytes after streaming terminal"), cause: nil
      end

      def finish_transport(expires)
        exchange = @input #: _StreamExchange
        output = @output #: _StreamExchange
        output.close_send
        @send_closed = true
        exchange.finish(timeout: completion_timeout(expires))
        @state.check!
        completion_timeout(expires)
        nil
      end

      def completion_timeout(expires)
        remaining = expires - now
        raise Error.new(:unavailable, "Streaming completion policy expired"), cause: nil unless remaining.positive?

        remaining
      end

      def recovery_timeout
        expires = @recovery_expires #: Float
        completion_timeout(expires)
      end

      def now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
end
