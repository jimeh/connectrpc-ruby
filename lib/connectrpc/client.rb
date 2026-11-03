# frozen_string_literal: true

require "English"

require "uri"

module ConnectRPC
  class Client # rubocop:disable Metrics/ClassLength
    # Configuration validates each independent public bound eagerly.
    # rubocop:disable Metrics/MethodLength, Metrics/AbcSize
    def initialize(base_url:, binding:, transport: nil, codec: :proto, request_limit: Protocol::DEFAULT_LIMIT,
                   response_limit: Protocol::DEFAULT_LIMIT, request_wire_limit: request_limit,
                   response_wire_limit: response_limit, accept_encoding: nil, use_get: false, get_url_limit: 8192,
                   protocol: :connect, mode: :unary, encoding: :identity, instrumentation: nil,
                   terminal_wire_limit: 65_536, terminal_limit: 65_536)
      Protocol.profile!(protocol, mode, encoding)
      raise ArgumentError, "use_get must be true or false" unless [true, false].include?(use_get)

      @terminal_wire_limit = Protocol.limit!(terminal_wire_limit)
      @terminal_limit = Protocol.limit!(terminal_limit)
      @use_get = use_get
      @get_url_limit = Protocol.limit!(get_url_limit)
      @base_url = parse_base_url(base_url)
      @instrumentation = instrumentation
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

    # rubocop:enable Metrics/MethodLength, Metrics/AbcSize

    def call(method, request, metadata: {}, timeout: 30, cancellation: nil)
      raise ArgumentError, "call requires a unary Method" unless method.is_a?(Method)

      unless cancellation.nil? || cancellation.is_a?(CancellationToken)
        raise ArgumentError, "cancellation must be a CancellationToken or nil"
      end
      return instrumented_call(method, request, metadata, timeout, cancellation) if @instrumentation

      process_call(method, request, metadata, timeout, cancellation)
    end

    private

    # rubocop:disable Metrics/MethodLength, Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
    def process_call(method, request, metadata, timeout, cancellation, lifecycle: nil, injection: nil)
      # @type var state: CallCancellation?
      state = nil
      deadline = Deadline.new(timeout) unless timeout.nil?
      active = state = CallCancellation.new(deadline: deadline, cancellation: cancellation)
      active.check!
      metadata = Metadata.new(metadata)
      if lifecycle
        metadata = injected_metadata(metadata, injection)
        lifecycle.request_metadata = Instrumentation.carrier(metadata)
      end
      outgoing = build_request(method, request, metadata, active, lifecycle)
      outgoing.deadline = deadline
      outgoing.cancellation = state
      outgoing.headers["connect-timeout-ms"] = [deadline.timeout_ms] if deadline
      outgoing.lifecycle = lifecycle if lifecycle
      active.check!
      lifecycle.request_bytes = outgoing.body.bytesize if lifecycle && outgoing.http_method != "GET"
      transport = @transport #: _Transport
      response = transport.call(outgoing)
      yield response if block_given?
      active.check!
      Protocol.check_size!(response.body, @response_wire_limit)
      result = read_response(response, method, active, lifecycle)
      active.commit!
      result
    rescue StandardError => e
      lifecycle&.record_failure(e)
      state&.commit!
      raise
    ensure
      state&.close
    end

    def instrumented_call(method, request, metadata, timeout, cancellation)
      # @type var injection: Hash[String, String | Array[String]]
      injection = {}
      instrumentation = @instrumentation #: Instrumentation
      lifecycle = instrumentation.start(owner: self, role: :client, method: method.is_a?(Method) ? method : nil,
                                        carrier: injection, server_address: @base_url.hostname.freeze,
                                        server_port: @base_url.port)
      lifecycle.begin_processing(self)
      # @type var response: TransportResponse?
      response = nil
      begin
        result = lifecycle.activate do
          process_call(method, request, metadata, timeout, cancellation, lifecycle: lifecycle,
                                                                         injection: injection) do |received|
            response = received
          end
        end
        result
      ensure
        error = $ERROR_INFO
        lifecycle.processing_finished(code: error.is_a?(Error) ? error.code : ("OK" unless error),
                                      error: error, normalized_error: error, http_status: response&.status,
                                      response_bytes: response&.body&.bytesize,
                                      response_metadata: response ? Instrumentation.carrier(response.headers) : {})
        lifecycle.finish(delivery: :unobserved)
      end
    end

    # rubocop:enable Metrics/MethodLength, Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity

    def injected_metadata(metadata, injection)
      instrumentation = @instrumentation #: Instrumentation
      carrier = injection #: metadata_input
      instrumentation.safely do
        validated = Metadata.new(carrier)
        return Metadata.new(metadata.to_h.merge(validated.to_h))
      end
      metadata
    end

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

    def encode_request(method, request, deadline, lifecycle = nil)
      deadline&.check!
      body = @binding.encode(request, type: method.request_type, codec: @codec)
      deadline&.check!
      Protocol.check_size!(body, @request_limit)
      body
    rescue CodecError => e
      lifecycle&.record_failure(e)
      raise Error.new(:invalid_argument, "Cannot encode request")
    end

    def build_request(method, request, metadata, deadline, lifecycle = nil) # rubocop:disable Metrics/MethodLength, Metrics/AbcSize, Metrics/CyclomaticComplexity
      body = encode_request(method, request, deadline, lifecycle)
      body = compress_request(body, deadline, lifecycle) if @encoding == :gzip
      Protocol.check_size!(body, @request_wire_limit)
      url = @base_url.dup
      base_path = @base_url.path #: String
      url.path = base_path.delete_suffix("/") + method.path
      get = @use_get && method.idempotency == :no_side_effects
      lifecycle.request_bytes = body.bytesize if lifecycle && get
      GetQuery.build(url, body, codec: @codec, encoding: @encoding, limit: @get_url_limit, deadline: deadline) if get
      TransportRequest.new(url: url, http_method: get ? "GET" : "POST",
                           headers: request_headers(metadata, nil, get: get), body: get ? "" : body,
                           response_limit: @response_wire_limit, deadline: nil)
    end

    def request_headers(metadata, deadline, get: false)
      headers = metadata.to_h.merge("content-type" => [Protocol::CONTENT_TYPES.fetch(@codec)],
                                    "accept-encoding" => [@accept_encoding.join(", ")],
                                    "connect-protocol-version" => ["1"])
      headers["content-encoding"] = ["gzip"] if @encoding == :gzip
      %w[content-type content-encoding connect-protocol-version].each { |key| headers.delete(key) } if get
      headers["connect-timeout-ms"] = [deadline.timeout_ms] if deadline
      headers
    end

    def compress_request(body, deadline, lifecycle = nil)
      Compression.encode(body, limit: @request_wire_limit, deadline: deadline,
                               on_error: ->(error) { lifecycle&.record_failure(error) })
    rescue Zlib::Error => e
      lifecycle&.record_failure(e)
      raise Error.new(:internal, "Cannot compress request")
    end

    def response_body(response, headers, deadline, lifecycle = nil)
      encoding = response_encoding!(headers)

      body = response.body
      body = inflate_response(body, response.status, deadline, lifecycle) if encoding == :gzip && !body.empty?
      Protocol.check_size!(body, @response_limit)
      deadline&.check!
      body
    end

    def inflate_response(body, status, deadline, lifecycle = nil)
      Compression.decode(body, limit: @response_limit, deadline: deadline,
                               on_error: ->(error) { lifecycle&.record_failure(error) })
    rescue Compression::Malformed => e
      lifecycle&.record_failure(e)
      raise Error.new(:internal, "Cannot decompress response") if status == 200

      # Malformed error Bare-Messages use HTTP fallback, preserving metadata.
      ""
    rescue Zlib::Error => e
      lifecycle&.record_failure(e)
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

    def read_response(response, method, deadline, lifecycle = nil)
      headers = Protocol.headers(response.headers)
      body = response_body(response, headers, deadline, lifecycle)
      validate_response!(response.status, headers, body)
      message = decode_response(body, method, lifecycle)
      Response.new(message: message, metadata: Protocol.metadata(headers),
                   trailers: Protocol.metadata(headers, trailers: true))
    rescue ArgumentError => e
      lifecycle&.record_failure(e)
      raise Error.new(:internal, "Malformed response metadata")
    end

    def decode_response(bytes, method, lifecycle = nil)
      @binding.decode(bytes, type: method.response_type, codec: @codec)
    rescue CodecError => e
      lifecycle&.record_failure(e)
      raise Error.new(:internal, "Cannot decode response")
    end
  end
end
