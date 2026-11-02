# frozen_string_literal: true

require "uri"

module ConnectRPC
  class Client # rubocop:disable Metrics/ClassLength
    # Configuration validates each independent public bound eagerly.
    # rubocop:disable Metrics/MethodLength
    def initialize(base_url:, binding:, transport: nil, codec: :proto, request_limit: Protocol::DEFAULT_LIMIT,
                   response_limit: Protocol::DEFAULT_LIMIT, request_wire_limit: request_limit,
                   response_wire_limit: response_limit, accept_encoding: nil,
                   protocol: :connect, mode: :unary, encoding: :identity)
      Protocol.profile!(protocol, mode, encoding)
      @base_url = parse_base_url(base_url)
      @binding = binding
      @transport = transport || default_transport
      @codec = Protocol.codec!(codec)
      @encoding = encoding
      @accept_encoding = Protocol.accept_encoding!(accept_encoding, encoding)
      @request_wire_limit = Protocol.limit!(request_wire_limit)
      @response_wire_limit = Protocol.limit!(response_wire_limit)
      @request_limit = Protocol.limit!(request_limit)
      @response_limit = Protocol.limit!(response_limit)
    end

    # rubocop:enable Metrics/MethodLength

    def call(method, request, metadata: {}, timeout: 30)
      deadline = Deadline.new(timeout) unless timeout.nil?
      outgoing = build_request(method, request, Metadata.new(metadata), deadline)
      response = @transport.call(outgoing)
      deadline&.check!
      Protocol.check_size!(response.body, @response_wire_limit)
      read_response(response, method, deadline)
    end

    private

    def parse_base_url(base_url)
      uri = URI(base_url)
      unless %w[http https].include?(uri.scheme) && uri.host && !uri.userinfo && !uri.query && !uri.fragment
        raise ArgumentError, "Base URL must be an HTTP(S) URL without credentials, query or fragment"
      end

      uri
    end

    def default_transport
      require_relative "transports/net_http"
      Transports::NetHTTP.new
    end

    def encode_request(method, request, deadline)
      deadline&.check!
      body = @binding.encode(request, type: method.request_type, codec: @codec)
      deadline&.check!
      Protocol.check_size!(body, @request_limit)
      body
    rescue CodecError
      raise Error.new(:invalid_argument, "Cannot encode request")
    end

    def build_request(method, request, metadata, deadline)
      body = encode_request(method, request, deadline)
      body = compress_request(body, deadline) if @encoding == :gzip
      Protocol.check_size!(body, @request_wire_limit)
      url = @base_url.dup
      url.path = @base_url.path.delete_suffix("/") + method.path
      TransportRequest.new(url: url, http_method: "POST", headers: request_headers(metadata, deadline), body: body,
                           response_limit: @response_wire_limit, deadline: deadline)
    end

    def request_headers(metadata, deadline)
      headers = metadata.to_h.merge("content-type" => [Protocol::CONTENT_TYPES.fetch(@codec)],
                                    "accept-encoding" => [@accept_encoding.join(", ")],
                                    "connect-protocol-version" => ["1"])
      headers["content-encoding"] = ["gzip"] if @encoding == :gzip
      headers["connect-timeout-ms"] = [deadline.timeout_ms] if deadline
      headers
    end

    def compress_request(body, deadline)
      Compression.encode(body, limit: @request_wire_limit, deadline: deadline)
    rescue Zlib::Error
      raise Error.new(:internal, "Cannot compress request")
    end

    def response_body(response, headers, deadline)
      encoding = response_encoding!(headers)

      body = response.body
      body = inflate_response(body, response.status, deadline) if encoding == :gzip && !body.empty?
      Protocol.check_size!(body, @response_limit)
      deadline&.check!
      body
    end

    def inflate_response(body, status, deadline)
      Compression.decode(body, limit: @response_limit, deadline: deadline)
    rescue Compression::Malformed
      raise Error.new(:internal, "Cannot decompress response") if status == 200

      # Malformed error Bare-Messages use HTTP fallback, preserving metadata.
      ""
    rescue Zlib::Error
      raise Error.new(:internal, "Cannot decompress response")
    end

    def response_encoding!(headers)
      encoding = Protocol.content_encoding(headers)
      return encoding if encoding && @accept_encoding.include?(encoding)

      raise Error.new(:internal, "Unsupported response content encoding")
    end

    def validate_response!(status, headers, body)
      raise Protocol.parse_error(status, headers, body) unless status == 200

      content_type = Protocol.single(headers, "content-type")
      return if Protocol.content_codec(content_type) == @codec

      code = content_type&.downcase&.start_with?("application/") ? :internal : :unknown
      raise Error.new(code, "Unexpected response content type")
    end

    def read_response(response, method, deadline)
      headers = Protocol.headers(response.headers)
      body = response_body(response, headers, deadline)
      validate_response!(response.status, headers, body)
      message = decode_response(body, method)
      Response.new(message: message, metadata: Protocol.metadata(headers),
                   trailers: Protocol.metadata(headers, trailers: true))
    rescue ArgumentError
      raise Error.new(:internal, "Malformed response metadata")
    ensure
      deadline&.check!
    end

    def decode_response(bytes, method)
      @binding.decode(bytes, type: method.response_type, codec: @codec)
    rescue CodecError
      raise Error.new(:internal, "Cannot decode response")
    end
  end
end
