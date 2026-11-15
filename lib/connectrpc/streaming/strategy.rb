# frozen_string_literal: true

module ConnectRPC
  module Streaming
    # Protocol policy alone varies; Session owns all scope and I/O arbitration.
    class ConnectStrategy
      def grpc? = false
      def framing = Framing
      def wait(timeout) = timeout
      def single_response?(method) = method.is_a?(ClientStreamMethod)
      def decode_error(server) = server ? :invalid_argument : :internal
    end
  end

  module GRPCProtocol
    class Framing < Streaming::Framing
      private

      def validate_flags(flags)
        protocol_error("Invalid gRPC envelope flags") unless [0, 1].include?(flags)
        protocol_error("Unnegotiated compression") if flags == 1 && @encoding != :gzip
      end

      def protocol_error(message)
        raise Error.new(:internal, message), cause: nil
      end
    end

    class Strategy
      def initialize(codec:, wire_limit:, limit:, on_error: nil)
        @on_error = on_error
        @codec = codec
        @wire_limit = wire_limit
        @limit = limit
      end

      def grpc? = true
      def framing = Framing
      def wait(timeout) = [timeout || Deadline::MAX_NATIVE_WAIT, Deadline::MAX_NATIVE_WAIT].min
      def single_response?(method) = method.is_a?(Method) || method.is_a?(ClientStreamMethod)
      def decode_error(_server) = :internal

      def leading(exchange, reader, accept_encoding, timeout, input:) # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
        head = @head = exchange.response_head(timeout: wait(timeout))
        values = Protocol.headers(head.headers)
        GRPCProtocol.check_fields!(values)
        reject_media(input, exchange, head, timeout) unless GRPCProtocol.media?(Protocol.single(values, "content-type"))
        if !head.end_stream && values.key?("grpc-status") && GRPCProtocol.status_value(values).nil?
          raise Error.new(:unknown, "Malformed initial grpc-status"), cause: nil
        end

        coding = GRPCProtocol.encoding(values)
        raise Error.new(:internal, "Unadvertised gRPC encoding"), cause: nil unless accept_encoding.include?(coding)

        reader.encoding = coding
        head.end_stream ? Metadata.new.freeze : Protocol.metadata(values).freeze
      end

      def reject_media(input, exchange, head, timeout)
        terminal = head.end_stream ? head.headers : drain_rejected_media(input, exchange, timeout)
        GRPCProtocol.check_fields!(terminal, @wire_limit)
        grpc = terminal.key?("grpc-status") || head.headers.key?("grpc-status")
        code = grpc ? "internal" : Error::HTTP_FALLBACK.fetch(head.status, "unknown")
        raise Error.new(code, "Unexpected gRPC response content type"), cause: nil
      end

      def recoverable?(exchange)
        head = @head #: GRPCHead
        fields = head.end_stream ? head.headers : exchange.response_trailers
        return false unless fields

        number = GRPCProtocol.status_value(fields)
        !!number&.positive?
      end

      def terminal(exchange, metadata)
        head = @head #: GRPCHead
        fields = head.end_stream ? head.headers : exchange.response_trailers
        GRPCProtocol.parse_status(fields || {}, codec: @codec, http_status: head.status, metadata: metadata,
                                                wire_limit: @wire_limit, limit: @limit, on_error: @on_error)
      end

      private

      # A body of up to RECOVERY_LIMIT bytes reaches EOF and the trailers. The
      # read stops at the first byte past that, so a larger body yields no
      # fields: the caller classifies from the head alone and call cleanup
      # abandons the unread stream.
      def drain_rejected_media(input, exchange, timeout)
        remaining = Streaming::RECOVERY_LIMIT
        loop do
          bytes = input.read([Streaming::FRAGMENT_SIZE, remaining + 1].min, timeout: wait(timeout))
          return exchange.response_trailers || {} if bytes.nil? || bytes.empty?

          remaining -= bytes.bytesize
          return {} if remaining.negative?
        end
      end
    end
  end
end
