# frozen_string_literal: true

module ConnectRPC
  module Hyper
    # One streaming call for core's exchange SPI. Operations run on the
    # calling thread and block in Rust with the GVL released; each wait is
    # bounded by the call deadline and the per-wait timeout.
    class Exchange
      def initialize(native, request, grpc:)
        @native = native
        @request = request
        @grpc = grpc
        @head = nil
        @trailers = nil
        @read_eof = false
      end

      def response_head(timeout: nil)
        @response_head ||= begin
          status, headers, end_stream = wait(timeout) { |limit| @native.head(limit) }
          if @grpc
            # h2 accepts blocks up to 16 MiB; core's field limit applies here.
            GRPCProtocol.check_fields!(headers)
            GRPCHead.new(status: status, headers: headers, end_stream: end_stream)
          else
            StreamHead.new(status: status, headers: headers)
          end
        end
      end

      def read(length, timeout: nil)
        bytes = wait(timeout) { |limit| @native.read(length, limit) }
        receive_trailers if bytes.nil? && @grpc
        bytes
      end

      # h2 refuses a larger chunk outright, which would drop the request body
      # while the response waits for it.
      MAX_WRITE = 1 << 31
      private_constant :MAX_WRITE

      def write(bytes)
        raise ArgumentError, "A streaming write must be smaller than 2 GiB" if bytes.bytesize >= MAX_WRITE

        wait(nil) { |limit| @native.write(bytes, limit) }
        nil
      end

      def close_send
        @native.close_send
        nil
      end

      def response_trailers
        raise StreamStateError, "Trailers require response EOF" unless @read_eof

        @trailers
      end

      def finish(timeout: nil)
        wait(timeout) { |limit| @native.finish(limit) }
        return unless @grpc

        receive_trailers
        head = @response_head
        raise Error.new(:internal, "Missing final gRPC HEADERS"), cause: nil unless head&.end_stream || @trailers

        nil
      end

      def abort
        @native.abort
        nil
      end

      private

      def receive_trailers
        return if @read_eof

        trailers = @native.trailers
        GRPCProtocol.check_fields!(trailers) if trailers
        @trailers = trailers
        @read_eof = true
      end

      def wait(timeout)
        Errors.native(@request, grpc: @grpc) do
          @request.cancellation ? @request.cancellation.check! : @request.deadline&.check!
          remaining = @request.deadline&.remaining
          yield [timeout, remaining].compact.min
        end
      end
    end
  end
end
