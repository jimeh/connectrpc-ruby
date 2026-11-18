# frozen_string_literal: true

require "uri"

module ConnectRPC
  class Client # rubocop:disable Metrics/ClassLength
    # Configuration validates each independent public bound eagerly.
    # rubocop:disable Metrics/MethodLength, Metrics/AbcSize
    def initialize(base_url:, binding: nil, transport: nil, codec: :proto, request_limit: Protocol::DEFAULT_LIMIT,
                   response_limit: Protocol::DEFAULT_LIMIT, request_wire_limit: request_limit,
                   response_wire_limit: response_limit, accept_encoding: nil, use_get: false, get_url_limit: 8192,
                   protocol: :connect, mode: :unary, encoding: :identity, instrumentation: nil,
                   terminal_wire_limit: 65_536, terminal_limit: 65_536, status_codec: nil,
                   grpc_status_wire_limit: 65_536, grpc_status_limit: 65_536, interceptors: [])
      Protocol.profile!(protocol, mode, encoding)
      @protocol = protocol
      @unary_interceptors, @stream_interceptors = Interceptors.client(interceptors)
      @grpc_status_wire_limit = Protocol.limit!(grpc_status_wire_limit)
      @grpc_status_limit = Protocol.limit!(grpc_status_limit)
      binding = Protocol.binding!(binding)
      @status_codec = GRPCProtocol.status_codec!(binding, status_codec) if protocol == :grpc
      raise ArgumentError, "gRPC requires protobuf POST" if protocol == :grpc && (codec != :proto || use_get)
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

    def call(method, request, metadata: {}, timeout: 30, deadline: nil, cancellation: nil)
      raise ArgumentError, "call requires a unary Method" unless method.is_a?(Method)

      validate_call_options!(deadline, cancellation)
      limit = [timeout, deadline] #: [Numeric?, Deadline?]
      if @protocol == :grpc
        raise ArgumentError, "Transport does not support unary gRPC" unless stream_capability?(method)

        return stream_call(method, request, metadata, limit, cancellation) { |_| nil }
      end
      return instrumented_call(method, request, metadata, limit, cancellation) if @instrumentation

      process_call(method, request, metadata, limit, cancellation)
    end

    private

    def validate_call_options!(deadline, cancellation)
      raise ArgumentError, "deadline must be a Deadline or nil" unless deadline.nil? || deadline.is_a?(Deadline)
      return if cancellation.nil? || cancellation.is_a?(CancellationToken)

      raise ArgumentError, "cancellation must be a CancellationToken or nil"
    end

    # rubocop:disable Metrics/MethodLength, Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
    def process_call(method, request, metadata, limit, cancellation, lifecycle: nil, injection: nil, &observe)
      # @type var state: CallCancellation?
      state = nil
      # @type var independent: Exception?
      independent = nil
      timeout, supplied = limit
      deadline = Protocol.call_deadline(timeout, supplied)
      active = state = CallCancellation.new(deadline: deadline, cancellation: cancellation)
      active.check!
      metadata = Metadata.new(metadata)
      chain = @unary_interceptors
      if chain
        result = intercept_call(chain, method, request, metadata, deadline, active, lifecycle, injection, &observe)
        if result.is_a?(StandardError)
          independent = result
          raise independent, cause: independent.cause
        end
      else
        if lifecycle
          metadata = injected_metadata(metadata, injection)
          lifecycle.request_metadata = Instrumentation.carrier(metadata)
        end
        result = exchange(method, request, metadata, deadline, active, lifecycle, &observe)
      end
      active.commit!
      result
    rescue StandardError => e
      lifecycle&.record_failure(e)
      # Interceptor exceptions keep their identity over an accepted cancellation.
      state&.commit! unless independent
      raise
    ensure
      state&.close
    end

    def exchange(method, request, metadata, deadline, active, lifecycle) # rubocop:disable Metrics/ParameterLists
      outgoing = build_request(method, request, metadata, active, lifecycle)
      outgoing.deadline = deadline
      outgoing.cancellation = active.view
      outgoing.headers["connect-timeout-ms"] = [deadline.timeout_ms] if deadline
      outgoing.lifecycle = lifecycle if lifecycle
      active.check!
      lifecycle.request_bytes = outgoing.body.bytesize if lifecycle && outgoing.http_method != "GET"
      transport = @transport #: _Transport
      response = transport.call(outgoing)
      yield response if block_given?
      active.check!
      Protocol.check_size!(response.body, @response_wire_limit)
      read_response(response, method, active, lifecycle)
    end

    # Returns the outermost result, or the exception that escaped the chain
    # without being the innermost dispatch failure (failure provenance).
    def intercept_call(chain, method, request, metadata, deadline, active, lifecycle, injection, &) # rubocop:disable Metrics/ParameterLists
      call = ClientUnaryCall.new(method: method, protocol: :connect, request: request, request_metadata: metadata,
                                 deadline: deadline, cancellation: active.view)
      lifecycle.request_metadata = Instrumentation.carrier(metadata) if lifecycle
      # @type var innermost: Exception?
      innermost = nil
      chain.run(call, lifecycle) do
        dispatched = dispatch_metadata(call, lifecycle, injection)
        active.check!
        exchange(method, call.request, dispatched, deadline, active, lifecycle, &)
      rescue Exception => e # rubocop:disable Lint/RescueException -- provenance compares the exact escape
        innermost = e
        raise
      end
    rescue StandardError => e
      raise if e.equal?(innermost)

      e
    end

    # Closes the call and returns the metadata actually sent.
    def dispatch_metadata(call, lifecycle, injection)
      call.__send__(:dispatch!)
      metadata = call.request_metadata
      return metadata unless lifecycle

      metadata = injected_metadata(metadata, injection)
      lifecycle.request_metadata = Instrumentation.carrier(metadata)
      metadata
    end

    def instrumented_call(method, request, metadata, limit, cancellation)
      # @type var injection: Hash[String, String | Array[String]]
      injection = {}
      instrumentation = @instrumentation #: Instrumentation
      lifecycle = instrumentation.start(owner: self, role: :client, method: method.is_a?(Method) ? method : nil,
                                        carrier: injection, **request_facts(method))
      lifecycle.begin_processing(self)
      # @type var response: TransportResponse?
      response = nil
      # @type var error: Exception?
      error = nil
      completed = false
      begin
        result = lifecycle.activate do
          process_call(method, request, metadata, limit, cancellation, lifecycle: lifecycle,
                                                                       injection: injection) do |received|
            response = received
          end
        end
        completed = true
        result
      rescue Exception => e # rubocop:disable Lint/RescueException -- observe the actual escape and preserve it
        error = e
        raise
      ensure
        head, trailers = Instrumentation.unary_carriers(response ? response.headers : {})
        lifecycle.processing_finished(code: error.is_a?(Error) ? error.code : ("OK" if completed),
                                      error: error, normalized_error: error, http_status: response&.status,
                                      response_bytes: response&.body&.bytesize,
                                      response_metadata: head, response_trailers: trailers)
        lifecycle.finish(delivery: :unobserved)
      end
    end

    # rubocop:enable Metrics/MethodLength, Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity

    def injected_metadata(metadata, injection)
      instrumentation = @instrumentation #: Instrumentation
      carrier = injection #: metadata_input
      instrumentation.safely do
        validated = Metadata.new(carrier)
        # Baggage is a list header; caller members stay ahead of injected ones.
        merged = metadata.to_h.merge(validated.to_h) do |key, own, injected|
          key == "baggage" ? own + injected : injected
        end
        return Metadata.new(merged)
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
      raise Error.new(:invalid_argument, "Cannot encode request"), cause: nil
    end

    def build_request(method, request, metadata, deadline, lifecycle = nil) # rubocop:disable Metrics/MethodLength, Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
      body = encode_request(method, request, deadline, lifecycle)
      body = compress_request(body, deadline, lifecycle) if @encoding == :gzip
      Protocol.check_size!(body, @request_wire_limit)
      url = @base_url.dup
      url.path = request_path(method)
      get = @use_get && method.idempotency == :no_side_effects
      lifecycle.request_bytes = body.bytesize if lifecycle && get
      GetQuery.build(url, body, codec: @codec, encoding: @encoding, limit: @get_url_limit, deadline: deadline) if get
      lifecycle&.message_sent(body.bytesize)
      TransportRequest.new(url: url, http_method: get ? "GET" : "POST",
                           headers: request_headers(metadata, nil, get: get), body: get ? "" : body,
                           response_limit: @response_wire_limit, deadline: nil)
    end

    def request_path(method)
      base_path = @base_url.path #: String
      base_path.delete_suffix("/") + method.path
    end

    # Client endpoint facts for instrumentation; never the query, which can carry a GET message.
    def request_facts(method)
      transport = @transport #: (_Transport | _StreamingTransport | _GRPCTransport) & _TransportClassProbe
      { server_address: @base_url.hostname.freeze, server_port: @base_url.port, url_scheme: @base_url.scheme.freeze,
        url_path: request_path(method).freeze, transport_class: transport.class }
    end

    def request_headers(metadata, deadline, get: false)
      headers = metadata.to_h.merge("content-type" => [Protocol::CONTENT_TYPES.fetch(@codec)],
                                    "accept-encoding" => [@accept_encoding.join(", ")],
                                    "connect-protocol-version" => ["1"])
      headers["content-encoding"] = ["gzip"] if @encoding == :gzip
      %w[content-type content-encoding connect-protocol-version].each { |key| headers.delete(key) } if get
      headers["connect-timeout-ms"] = [deadline.timeout_ms] if deadline
      default_user_agent(headers, Protocol::USER_AGENT)
    end

    # Presence, not truthiness: "user-agent" => [] sends none. Interceptors
    # run earlier and do not see this default.
    def default_user_agent(headers, value)
      headers["user-agent"] = [value] unless headers.key?("user-agent")
      headers
    end

    def compress_request(body, deadline, lifecycle = nil)
      Compression.encode(body, limit: @request_wire_limit, deadline: deadline,
                               on_error: ->(error) { lifecycle&.record_failure(error) })
    rescue Zlib::Error => e
      lifecycle&.record_failure(e)
      raise Error.new(:internal, "Cannot compress request"), cause: nil
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
      raise Error.new(:internal, "Cannot decompress response"), cause: nil if status == 200

      # Malformed error Bare-Messages use HTTP fallback, preserving metadata.
      ""
    rescue Zlib::Error => e
      lifecycle&.record_failure(e)
      raise Error.new(:internal, "Cannot decompress response"), cause: nil
    end

    def response_encoding!(headers)
      encoding = Protocol.content_encoding(headers)
      return encoding if encoding && @accept_encoding.include?(encoding)

      raise Error.new(:internal, "Unsupported response content encoding")
    end

    def validate_response!(status, headers, body, cause = nil)
      raise Protocol.parse_error(status, headers, body), cause: cause unless status == 200

      content_type = Protocol.single(headers, "content-type")
      return if Protocol.content_codec(content_type) == @codec

      code = content_type&.downcase&.start_with?("application/") ? :internal : :unknown
      raise Error.new(code, "Unexpected response content type")
    end

    def read_response(response, method, deadline, lifecycle = nil)
      headers, body = accept_response(response, deadline, lifecycle)
      message = decode_response(body, method, deadline, lifecycle)
      lifecycle&.message_received(response.body.bytesize)
      Response.new(message: message, metadata: Protocol.metadata(headers).freeze,
                   trailers: Protocol.metadata(headers, trailers: true).freeze).freeze
    end

    def accept_response(response, deadline, lifecycle)
      headers = Protocol.headers(response.headers)
      body = response_body(response, headers, deadline, lifecycle)
      validate_response!(response.status, headers, body, response.cause)
      [headers, body]
    rescue ArgumentError => e
      # Header names and values with invalid encodings fail String case and pattern checks.
      lifecycle&.record_failure(e)
      raise Error.new(:internal, "Malformed response metadata"), cause: nil
    end

    def decode_response(bytes, method, deadline, lifecycle = nil)
      message = @binding.decode(bytes, type: method.response_type, codec: @codec)
      # A call cancelled or past its deadline during decode was not received.
      deadline&.check!
      message
    rescue CodecError => e
      lifecycle&.record_failure(e)
      raise Error.new(:internal, "Cannot decode response"), cause: nil
    end
  end
end
