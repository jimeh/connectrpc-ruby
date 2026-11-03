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

      def reject_media(input, exchange, head, timeout) # rubocop:disable Metrics/AbcSize, Metrics/MethodLength, Metrics/PerceivedComplexity
        if head.end_stream
          terminal = head.headers
        else
          loop do
            bytes = input.read(Streaming::FRAGMENT_SIZE, timeout: wait(timeout))
            break if bytes.nil? || bytes.empty?
          end
          terminal = exchange.response_trailers || {}
        end
        GRPCProtocol.check_fields!(terminal, @wire_limit)
        code = if terminal.key?("grpc-status") || head.headers.key?("grpc-status")
                 "internal"
               else
                 Error::HTTP_FALLBACK.fetch(head.status, "unknown")
               end
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
    end
  end
end
