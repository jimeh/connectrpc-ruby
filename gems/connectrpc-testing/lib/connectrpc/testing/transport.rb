# frozen_string_literal: true

module ConnectRPC
  module Testing
    class Transport
      def initialize(server:)
        raise ArgumentError, "server must be a ConnectRPC::Server" unless server.is_a?(Server)

        @server = server
      end

      def call(request)
        validate!(request)
        request.deadline&.check!
        response = dispatch(request)
        request.deadline&.check!
        Protocol.check_size!(response.body, request.response_limit)
        response
      end

      private

      def dispatch(request)
        headers = request.headers.dup
        headers["connect-timeout-ms"] = [request.deadline.timeout_ms] if request.deadline
        @server.call(path: request.url.path, http_method: request.http_method,
                     headers: headers, input: BytesReader.new(request.body))
      end

      def validate!(request)
        raise ArgumentError, "request must be a ConnectRPC::TransportRequest" unless request.is_a?(TransportRequest)
        unless valid_url?(request.url)
          raise ArgumentError,
                "url must be an absolute HTTP(S) URI without credentials or fragment"
        end

        validate_fields!(request)
        unless request.deadline.nil? || request.deadline.is_a?(Deadline)
          raise ArgumentError, "deadline must be a ConnectRPC::Deadline or nil"
        end

        Protocol.limit!(request.response_limit)
      end

      def validate_fields!(request)
        unless request.http_method.is_a?(String) && request.body.is_a?(String)
          raise ArgumentError, "method and body must be Strings"
        end
        return if valid_headers?(request.headers)

        raise ArgumentError, "headers must contain lower-case String keys and String arrays"
      end

      def valid_url?(url)
        url.is_a?(URI::HTTP) && %w[http https].include?(url.scheme) && url.host && !url.host.empty? &&
          !url.userinfo && !url.fragment
      end

      def valid_headers?(headers)
        headers.is_a?(Hash) && headers.all? do |key, values|
          key.is_a?(String) && key == key.downcase && values.is_a?(Array) && values.all?(String)
        end
      end

      # MessageIO controls read sizes. Offsets are bytes even for UTF-8 JSON.
      class BytesReader
        def initialize(bytes)
          @bytes = bytes
          @offset = 0
        end

        def read(length)
          return if @offset >= @bytes.bytesize

          chunk = @bytes.byteslice(@offset, length)
          @offset += chunk.bytesize
          chunk
        end
      end
      private_constant :BytesReader
    end
  end
end
