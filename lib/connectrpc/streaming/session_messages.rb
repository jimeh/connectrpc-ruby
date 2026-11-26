# frozen_string_literal: true

module ConnectRPC
  module Streaming
    class Session # rubocop:disable Metrics/ClassLength
      def remote_completed?(error)
        @strategy.grpc? && @eof && error.equal?(@remote_error)
      end

      def headers
        @headers || operation(:reading) { leading }
      end

      # Leading metadata already received and accepted; nil before then. Never reads.
      def cached_headers = @headers

      # skip_data counts a message without decoding, inflating or observing it.
      # Envelope bounds still apply, and the terminal still decides the outcome.
      def receive(skip_data: false)
        operation(:reading) { receive_message(skip_data) }
      end

      # Server single-request methods: true only at a clean end of input.
      # A surplus envelope is rejected from its header, before its payload.
      def request_complete?
        operation(:reading) do
          next true if @eof

          leading
          next false if @reader.next_envelope?

          receive_eof
          true
        end
      end

      def send_message(message)
        operation(:writing) do
          validate_send(message)
          encode_message(message)
        end
      end

      def close_send
        operation(:writing) do
          close_exchange_send
          nil
        end
      end

      # Sends the one request of a unary or server-streaming call, which core
      # writes itself. As in grpc-go and connect-go, a send failure leaves the
      # outcome to the response; other failures still end the call.
      def send_request(message)
        send_message(message)
      rescue Error => e
        raise unless send_failure?(e)

        forgive_send(e, @raw_send_failure)
        close_forgiven_send
      else
        close_send
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

      # The lifecycle drops only the failures this send produced, so failures
      # recorded independently, before or during it, still count.
      def forgive_send(*errors)
        @send_failure = @raw_send_failure = @writing_error = nil
        @lifecycle&.forgive(*errors)
        nil
      end

      # After a forgiven send, closing may fail for the same reason the write
      # did. Other failures, such as cancellation or a closed scope, end the call.
      def close_forgiven_send
        close_send
      rescue IOError, SystemCallError, Error => e
        raise if e.is_a?(Error) && e.code != "unavailable"

        forgive_send(e)
      end

      def receive_message(skip_data)
        raise StreamStateError, "Normal response consumption was relinquished" if @recovering

        return completed_receive if @eof

        leading
        frame = @reader.read_frame(skip_data: skip_data)
        return receive_eof unless frame

        flags, bytes, length = frame
        return accept_terminal(bytes) if flags.anybits?(2)
        return decode_message(bytes, length) unless skip_data

        @received += 1
        nil
      end

      def completed_receive
        error = @remote_error
        raise error, cause: nil if error

        nil
      end

      def receive_eof
        return accept_grpc_terminal unless @server || !@strategy.grpc?

        raise Error.new(:internal, "Missing streaming terminal"), cause: nil unless @server

        @eof = true
        nil
      end

      def decode_message(bytes, length)
        @received += 1
        validate_response_count unless @strategy.grpc?
        message = @binding.decode(bytes, type: @read_type, codec: @codec)
        @state.check!
        raise Error.new(:internal, "Binding decoded a nil streaming message"), cause: nil if message.nil?

        @lifecycle&.message_received(length)
        message
      rescue CodecError => e
        @lifecycle&.record_failure(e)
        raise Error.new(@strategy.decode_error(@server), "Cannot decode streaming message"), cause: nil
      end

      def validate_response_count
        return unless !@server && @method.is_a?(ClientStreamMethod) && @received > 1

        raise Error.new(:unimplemented, "Expected exactly one response"), cause: nil
      end

      def validate_send(message)
        raise StreamStateError, "Request sending is closed" if @send_closed
        raise nil_message_error, cause: nil if message.nil?
        return unless !@server && @method.is_a?(ServerStreamMethod) && @sent.positive?

        raise StreamStateError, "Server streaming accepts one request"
      end

      def encode_message(message) # rubocop:disable Metrics/MethodLength
        bytes = @binding.encode(message, type: @write_type, codec: @codec)
        @state.check!
        Protocol.check_size!(bytes, @write_limit)
        publisher = @publish
        publisher ? publisher.call : @context&.send(:publish_headers)
        written = @writer.emit(bytes)
        @sent += 1
        @lifecycle&.message_sent(written)
        nil
      rescue CodecError => e
        @lifecycle&.record_failure(e)
        raise Error.new(@server ? :internal : :invalid_argument, "Cannot encode streaming message"), cause: nil
      end
    end
  end
end
