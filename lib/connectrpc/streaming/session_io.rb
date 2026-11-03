# frozen_string_literal: true

module ConnectRPC
  module Streaming
    class Session
      def write(bytes)
        check_open!
        write_chunk(bytes)
        check_open!
        nil
      rescue StandardError => e
        record_send_failure(e)
      end

      def read(length, timeout: nil)
        check_open!
        timeout = recovery_read_timeout(length) if @recovery_expires
        deadline = @context&.deadline
        timeout = deadline.remaining if @server && deadline
        bytes = @input.read(length, timeout: @strategy.wait(timeout))
        check_open!
        count_recovery_bytes(bytes)
        bytes
      end

      private

      def write_chunk(bytes)
        if @server
          output = @output #: _StreamingOutput
          output.write(bytes, timeout: @strategy.wait(@context&.deadline&.remaining))
        else
          @output.write(bytes)
        end
      end

      def record_send_failure(error)
        @lifecycle&.record_failure(error)
        @state.check!
        if error.is_a?(IOError) || error.is_a?(SystemCallError)
          failure = @send_failure = Error.new(:unavailable, "Streaming write failed")
          raise failure, cause: nil
        end
        @send_failure = error if error.is_a?(Error) && error.code == "unavailable"
        raise error
      end

      def recovery_read_timeout(length)
        remaining = @recovery_remaining #: Integer
        raise Error.new(:resource_exhausted, "Streaming recovery byte limit"), cause: nil if length > remaining

        recovery_timeout
      end

      def count_recovery_bytes(bytes)
        remaining = @recovery_remaining
        @recovery_remaining = remaining - bytes.bytesize if remaining && bytes
      end

      def leading
        snapshot = @headers
        return snapshot if snapshot

        @headers = if @server
                     @context #: StreamingContext
                       .request_metadata.freeze
                   else
                     response_metadata
                   end
      end

      def response_metadata # rubocop:disable Metrics/MethodLength
        exchange = @input #: _StreamExchange
        strategy = @strategy
        if strategy.is_a?(GRPCProtocol::Strategy)
          grpc = @input #: _GRPCStreamExchange
          return strategy.leading(grpc, @reader, @accept_encoding, @recovery_expires ? recovery_timeout : nil,
                                  input: self)
        end
        head = @recovery_expires ? exchange.response_head(timeout: recovery_timeout) : exchange.response_head
        validate_response_status(head.status)
        values = Protocol.headers(head.headers)
        validate_response_headers(values)
        Protocol.metadata(values).freeze
      end

      def validate_response_status(status)
        return if status == 200

        raise Error.new(Error::HTTP_FALLBACK.fetch(status, "unknown"), "HTTP #{status}"), cause: nil
      end

      def validate_response_headers(values)
        unless Streaming.codec(Protocol.single(values, "content-type")) == @codec
          raise Error.new(:internal, "Unexpected streaming response content type"), cause: nil
        end

        encoding = Protocol.content_encoding(Streaming.coding_headers(values))
        unless encoding && @accept_encoding.include?(encoding)
          raise Error.new(:internal, "Unsupported streaming response encoding"), cause: nil
        end

        @reader.encoding = encoding
      end
    end
  end
end
