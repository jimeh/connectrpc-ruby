# frozen_string_literal: true

require "uri"

module ConnectRPC
  class Client
    def initialize(base_url:, binding:, transport: nil, codec: :proto, request_limit: Protocol::DEFAULT_LIMIT,
                   response_limit: Protocol::DEFAULT_LIMIT, protocol: :connect, mode: :unary, encoding: :identity)
      Protocol.profile!(protocol, mode, encoding)
      @base_url = parse_base_url(base_url)
      @binding = binding
      @transport = transport || default_transport
      @codec = Protocol.codec!(codec)
      @request_limit = Protocol.limit!(request_limit)
      @response_limit = Protocol.limit!(response_limit)
    end

    def call(method, request, metadata: {}, timeout: 30)
      deadline = Deadline.new(timeout)
      outgoing = build_request(method, request, Metadata.new(metadata), deadline)
      response = @transport.call(outgoing)
      deadline.check!
      Protocol.check_size!(response.body, @response_limit)
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
      deadline.check!
      body = @binding.encode(request, type: method.request_type, codec: @codec)
      deadline.check!
      Protocol.check_size!(body, @request_limit)
      body
    rescue CodecError
      raise Error.new(:invalid_argument, "Cannot encode request")
    end

    def build_request(method, request, metadata, deadline)
      body = encode_request(method, request, deadline)
      url = @base_url.dup
      url.path = @base_url.path.delete_suffix("/") + method.path
      headers = metadata.to_h.merge("content-type" => [Protocol::CONTENT_TYPES.fetch(@codec)],
                                    "accept-encoding" => ["identity"], "connect-protocol-version" => ["1"],
                                    "connect-timeout-ms" => [deadline.timeout_ms])
      TransportRequest.new(url: url, http_method: "POST", headers: headers, body: body,
                           response_limit: @response_limit, deadline: deadline)
    end

    def validate_encoding!(headers)
      return if Protocol.identity_encoding?(headers)

      raise Error.new(:internal, "Unsupported response content encoding")
    end

    def validate_response!(status, headers, body)
      validate_encoding!(headers)
      raise Protocol.parse_error(status, headers, body) unless status == 200

      content_type = Protocol.single(headers, "content-type")
      return if Protocol.content_codec(content_type) == @codec

      code = content_type&.downcase&.start_with?("application/") ? :internal : :unknown
      raise Error.new(code, "Unexpected response content type")
    end

    def read_response(response, method, deadline)
      headers = Protocol.headers(response.headers)
      validate_response!(response.status, headers, response.body)
      message = decode_response(response.body, method)
      Response.new(message: message, metadata: Protocol.metadata(headers),
                   trailers: Protocol.metadata(headers, trailers: true))
    rescue ArgumentError
      raise Error.new(:internal, "Malformed response metadata")
    ensure
      deadline.check!
    end

    def decode_response(bytes, method)
      @binding.decode(bytes, type: method.response_type, codec: @codec)
    rescue CodecError
      raise Error.new(:internal, "Cannot decode response")
    end
  end
end
