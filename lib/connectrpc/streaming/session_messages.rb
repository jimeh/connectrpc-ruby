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
        forgiven = begin
          send_message(message)
          nil
        rescue Error => e
          raise unless send_failure?(e)

          [e, @raw_send_failure].compact
        end
        # Closing outside the rescue keeps the forgiven failure from becoming
        # the implicit cause of anything close_send raises.
        forgiven ? close_forgiven_send(forgiven) : close_send
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

      # After a forgiven send, closing may fail again with the write's failure,
      # or with a send failure caused by it, which is forgiven too. Any other
      # failure ends the call: the half-close may not have reached the peer.
      def close_forgiven_send(forgiven)
        forgive_send(*forgiven)
        close_send
      rescue IOError, SystemCallError, Error => e
        raise if e.is_a?(Error) && e.code != "unavailable"
        raise unless caused_by?(e, forgiven)

        forgive_send(e)
      end

      def caused_by?(error, causes)
        seen = [] #: Array[Exception]
        # @type var current: Exception?
        current = error
        while current && seen.none? { |failure| failure.equal?(current) }
          return true if causes.any? { |cause| cause.equal?(current) }

          seen << current
          current = current.cause
        end
        false
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
