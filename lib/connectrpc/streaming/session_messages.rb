# frozen_string_literal: true

module ConnectRPC
  module Streaming
    class Session
      def headers
        @headers || operation(:reading) { leading }
      end

      def receive
        operation(:reading) { receive_message }
      end

      def send_message(message)
        operation(:writing) do
          validate_send(message)
          encode_message(message)
        end
      end

      def close_send
        operation(:writing) do
          return nil if @send_closed

          @send_closed = true
          output = @output #: _StreamExchange
          output.close_send
          nil
        end
      end

      def abandon
        check_open!
        @abandoned = true
        @output.abort
        nil
      rescue StandardError => e
        record_operation_failure(e, :abandon)
        raise
      end

      private

      def receive_message
        raise StreamStateError, "Normal response consumption was relinquished" if @recovering

        return completed_receive if @eof

        leading
        frame = @reader.read_frame
        return receive_eof unless frame
        return accept_terminal(frame.last) if frame.first.anybits?(2)

        decode_message(frame.last)
      end

      def completed_receive
        error = @remote_error
        raise error, cause: nil if error

        nil
      end

      def receive_eof
        raise Error.new(:internal, "Missing streaming terminal"), cause: nil unless @server

        @eof = true
        nil
      end

      def decode_message(bytes)
        @received += 1
        validate_response_count
        message = @binding.decode(bytes, type: @read_type, codec: @codec)
        @state.check!
        raise Error.new(:internal, "Binding decoded a nil streaming message"), cause: nil if message.nil?

        message
      rescue CodecError => e
        @lifecycle&.record_failure(e)
        raise Error.new(@server ? :invalid_argument : :internal, "Cannot decode streaming message"), cause: nil
      end

      def validate_response_count
        return unless !@server && @method.is_a?(ClientStreamMethod) && @received > 1

        raise Error.new(:unimplemented, "Expected exactly one response"), cause: nil
      end

      def validate_send(message)
        raise StreamStateError, "Request sending is closed" if @send_closed
        raise Error.new(:invalid_argument, "Nil is not a streaming message"), cause: nil if message.nil?
        return unless !@server && @method.is_a?(ServerStreamMethod) && @sent.positive?

        raise StreamStateError, "Server streaming accepts one request"
      end

      def encode_message(message)
        bytes = @binding.encode(message, type: @write_type, codec: @codec)
        @state.check!
        Protocol.check_size!(bytes, @write_limit)
        @context&.send(:publish_headers)
        @writer.emit(bytes)
        @sent += 1
        nil
      rescue CodecError => e
        @lifecycle&.record_failure(e)
        raise Error.new(@server ? :internal : :invalid_argument, "Cannot encode streaming message"), cause: nil
      end
    end
  end
end
