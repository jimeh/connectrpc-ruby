# frozen_string_literal: true

module ConnectRPC
  class Server # rubocop:disable Metrics/ClassLength
    attr_reader :services

    # Public configuration keeps each validation next to its assignment.
    # rubocop:disable Metrics/MethodLength, Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
    def initialize(services:, binding: nil, handlers: {}, invoker: nil, request_limit: Protocol::DEFAULT_LIMIT,
                   response_limit: Protocol::DEFAULT_LIMIT, request_wire_limit: request_limit,
                   response_wire_limit: response_limit, protocol: :connect, mode: :unary, encoding: :identity,
                   instrumentation: nil, get_query_limit: nil, terminal_wire_limit: 65_536, terminal_limit: 65_536,
                   status_codec: nil, grpc_status_wire_limit: 65_536, grpc_status_limit: 65_536, cors: nil,
                   interceptors: [], on_error: nil)
      Protocol.profile!(protocol, mode, encoding, server: true)
      @protocol = Protocol.server_protocols(protocol)
      unless cors.nil? || (cors.is_a?(GRPCWebCORS) && @protocol.include?(:grpc_web))
        raise ArgumentError, "cors requires a GRPCWebCORS policy and explicit gRPC-Web enablement"
      end

      if on_error
        probe = on_error #: (^(StandardError) -> void | _ServerErrorHandler) & _InterceptorProbe
        raise ArgumentError, "on_error must be callable" unless probe.respond_to?(:call)
      end

      @on_error = on_error
      @cors = cors
      @grpc_status_wire_limit = Protocol.limit!(grpc_status_wire_limit)
      @grpc_status_limit = Protocol.limit!(grpc_status_limit)
      binding = Protocol.binding!(binding)
      @status_codec = GRPCProtocol.status_codec!(binding, status_codec) if @protocol.intersect?(%i[grpc grpc_web])
      @terminal_wire_limit = Protocol.limit!(terminal_wire_limit)
      @terminal_limit = Protocol.limit!(terminal_limit)
      @services = services.dup.freeze
      @methods = declarations(services)
      raise ArgumentError, "Handler path is not declared" unless handlers.keys.all? { |key| @methods.key?(key) }

      @handlers = handlers.dup.freeze
      @invoker = invoker || method(:invoke_handler)
      @unary_interceptors, @stream_interceptors = Interceptors.server(interceptors)
      @instrumentation = instrumentation
      @binding = binding
      @encoding = encoding
      @request_wire_limit = Protocol.limit!(request_wire_limit)
      query_limit = (3 * (4 * ((@request_wire_limit + 2) / 3))) + 1024
      @get_query_limit = Protocol.limit!(get_query_limit.nil? ? query_limit : get_query_limit)
      @response_wire_limit = Protocol.limit!(response_wire_limit)
      @request_limit = Protocol.limit!(request_limit)
      @response_limit = Protocol.limit!(response_limit)
      @error_limit = [@response_limit, @response_wire_limit].min
    end

    # rubocop:enable Metrics/MethodLength, Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity

    def handles?(path)
      services.any? { |service| path == "/#{service.name}" || path.start_with?("/#{service.name}/") }
    end

    def start_call(path:, headers:, http_method: "POST")
      instrumentation = @instrumentation
      return unless instrumentation

      carrier = Instrumentation.carrier(headers)
      address, port = request_authority(carrier["host"])
      instrumentation.start(owner: self, path: path.dup.freeze, role: :server, method: @methods[path],
                            carrier: carrier, server_address: address, server_port: port,
                            protocol: request_protocol(headers, http_method))
    end

    # Keep processing and terminal ensures adjacent to their owner.
    # rubocop:disable Metrics/MethodLength, Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
    # on_error is the host's per-call reporter. It runs after the Server's own
    # on_error for the same unexpected exception.
    def call(path:, http_method:, headers:, input:, query: "", lifecycle: nil, cancellation: nil, on_error: nil)
      if @protocol.include?(:grpc_web)
        early = cors_response(path: path, http_method: http_method, headers: headers)
        return early if early
        if request_protocol(headers, http_method) == :grpc_web
          return call_grpc_web_unary(path, http_method, headers, input, lifecycle, cancellation, on_error)
        end
      end
      unless lifecycle || @instrumentation
        return process_call(path: path, http_method: http_method, headers: headers, input: input, query: query,
                            cancellation: cancellation, on_error: on_error)
      end

      owned = lifecycle.nil?
      lifecycle ||= start_call(path: path, headers: headers, http_method: http_method)
      active = lifecycle #: Instrumentation::Handle
      active.begin_processing(self, path)
      # @type var error: Exception?
      error = nil
      begin
        record_request_metadata(active, Protocol.headers(headers))
        active.activate do
          process_call(path: path, http_method: http_method, headers: headers, input: input, query: query,
                       lifecycle: active, cancellation: cancellation, on_error: on_error)
        end
      rescue Exception => e # rubocop:disable Lint/RescueException -- include escapes from processing cleanup
        error = e
        raise
      ensure
        active.processing_finished(code: nil, error: error) unless active.outcome
        active.finish(delivery: :unobserved) if owned
      end
    end

    private

    AUTHORITY = /\A(?:\[(?<ipv6>[0-9A-Fa-f.]*:[0-9A-Fa-f.]*:[0-9A-Fa-f:.]*)\]|(?<name>[0-9A-Za-z._-]+))
                 (?::(?<port>[0-9]{1,5}))?\z/x
    private_constant :AUTHORITY

    # The address the client used, from a single host or :authority value:
    # host, host:port, [v6] or [v6]:port. Anything else yields neither fact.
    def request_authority(values)
      return [nil, nil] unless values && values.size == 1

      authority = values.fetch(0)
      return [nil, nil] if authority.bytesize > 255

      match = AUTHORITY.match(authority)
      port = match[:port]&.to_i if match
      return [nil, nil] unless match && (port.nil? || port.between?(1, 65_535))

      [(match[:ipv6] || match[:name] || "").dup.freeze, port]
    end

    def request_protocol(headers, http_method)
      # Query controls own Connect GET, including rejection before header work.
      return :connect if http_method == "GET" && @protocol.include?(:connect)

      headers = Protocol.headers(headers)
      return :grpc_web if GRPCWeb.intent?(headers)

      GRPCProtocol.intent?(headers) ? :grpc : :connect
    end

    def process_call(path:, http_method:, headers:, input:, query:, lifecycle: nil, cancellation: nil, on_error: nil)
      # @type var response: TransportResponse?
      # @type var context: Context?
      context = nil
      # @type var error: Exception?
      error = nil
      # @type var cleaned: bool
      cleaned = false
      begin
        cancellation&.check!
        return response = bare(415, {}, cancellation: cancellation) unless @protocol.include?(:connect)

        method = @methods[path]
        safe = method.is_a?(Method) && method.idempotency == :no_side_effects
        unless http_method == "POST" || (http_method == "GET" && (!method || safe))
          return response = bare(405, { "allow" => [safe ? "GET, POST" : "POST"] }, cancellation: cancellation)
        end
        if http_method == "GET"
          return response = process_get(path, headers, input, query, lifecycle, cancellation) do |created|
            context = created
          end
        end

        headers = Protocol.headers(headers)
        codec = Protocol.content_codec(Protocol.single(headers, "content-type"))
        unless codec
          return response = bare(415, { "accept-post" => ["application/proto, application/json"] },
                                 cancellation: cancellation)
        end

        decoded_context = context = request_context(headers, lifecycle, cancellation)
        response = dispatch(path, headers, input, codec, decoded_context, lifecycle: lifecycle)
      rescue Error => e
        lifecycle&.record_failure(e)
        response = error_response(terminal_error(e, context, cancellation), context, lifecycle)
      rescue StandardError => e
        lifecycle&.record_failure(e)
        internal = Error.new(:internal, "Internal error")
        safe = terminal_error(internal, context, cancellation)
        # A deadline or cancellation outcome that wins arbitration is not reported.
        report_unexpected(e, on_error) if safe.equal?(internal)
        response = error_response(safe, context, lifecycle)
      ensure
        context&.close
        vary_get(response) if http_method == "GET" && response
        cleaned = true
      end
    rescue Exception => e # rubocop:disable Lint/RescueException -- include rescue handlers and cleanup in the escape boundary
      error = e
      raise
    ensure
      if lifecycle && !lifecycle.outcome
        head, trailers = Instrumentation.unary_carriers(response ? response.headers : {})
        lifecycle.processing_finished(code: response&.status == 200 && cleaned && !error ? "OK" : lifecycle.code,
                                      error: error,
                                      normalized_error: lifecycle.normalized_error,
                                      http_status: response&.status, response_bytes: response&.body&.bytesize,
                                      response_metadata: head, response_trailers: trailers)
      end
    end

    # rubocop:enable Metrics/MethodLength, Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity

    # GET negotiation and dispatch preserve the specified rejection order.
    # rubocop:disable Metrics/MethodLength, Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
    def process_get(path, headers, input, query, lifecycle, state) # rubocop:disable Metrics/ParameterLists
      Protocol.check_size!(query, @get_query_limit)
      headers = Protocol.headers(headers)
      # Start valid deadlines before query work, but defer invalid timeout errors
      # until after codec/body/compression/version negotiation.
      timeout_error = nil #: Error?
      deadline = begin
        request_deadline(headers)
      rescue Error => e
        timeout_error = e
        nil
      end
      context = Context.new(deadline: deadline, lifecycle: lifecycle, cancellation: state)
      yield context
      values = GetQuery.parse(query, limit: @get_query_limit, wire_limit: @request_wire_limit, deadline: context)
      codecs = { "proto" => :proto, "json" => :json } #: Hash[String, codec]
      codec = codecs[values["encoding"] || ""]
      return bare(415, { "accept-post" => ["application/proto, application/json"] }, context: context) unless codec
      unless empty_get_body?(headers, input, context)
        return bare(415, { "accept-post" => ["application/proto, application/json"] }, context: context)
      end

      negotiated = get_headers(headers, values)
      raise Error.new(:invalid_argument, "Missing GET message") unless values.key?("message")
      raise timeout_error if timeout_error

      Protocol.metadata(headers).each { |key, values| context.request_metadata[key] = values }
      context.check!
      method = @methods[path]
      raise Error.new(:unimplemented, "Unknown method") unless method.is_a?(Method)

      bytes = GetQuery.payload(values, limit: @request_wire_limit, deadline: context,
                                       on_error: ->(error) { context.record_failure(error) })
      wire_bytes = bytes.bytesize
      lifecycle.request_bytes = wire_bytes if lifecycle
      bytes = inflate_request(bytes, negotiated, context)
      request = decode_request(bytes, method, codec, context)
      lifecycle&.message_received(wire_bytes)
      response = invoke_unary(method, request, context, :connect)
      encoding = Protocol.response_encoding(negotiated, Protocol.content_encoding(negotiated), @encoding)
      encode_response(response, method, codec, context, encoding, lifecycle)
    end

    # rubocop:enable Metrics/MethodLength, Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity

    def empty_get_body?(headers, input, deadline)
      deadline&.check!
      length = Protocol.content_length!(headers["content-length"], invalid_code: :invalid_argument)
      return false if length&.positive?

      bytes = input&.read(1)
      deadline&.check!
      bytes.nil? || bytes.empty?
    end

    def get_headers(headers, values)
      negotiated = headers.except("content-type", "content-encoding", "connect-protocol-version")
      coding = values["compression"]
      negotiated["content-encoding"] = [coding] if coding && !coding.empty?
      # Validate coding before version, matching the GET negotiation order.
      validate_headers!(negotiated)
      version = values["connect"]
      if version && !version.empty? && version != "v1"
        raise Error.new(:invalid_argument, "Unsupported Connect protocol version")
      end

      negotiated
    end

    def vary_get(response)
      values = response.headers.fetch("vary", [])
      tokens = values.flat_map { |value| value.split(",").map(&:strip) }
      return if tokens.any? { |token| token == "*" || token.casecmp?("accept-encoding") }

      response.headers["vary"] = values + ["Accept-Encoding"]
    end

    def declarations(services)
      raise ArgumentError, "Invalid service declarations" unless services.is_a?(Array) && services.all?(Service)
      raise ArgumentError, "Duplicate service" unless services.map(&:name).uniq.size == services.size

      services.flat_map(&:rpcs).to_h { |method| [method.path, method] }.freeze
    end

    def invoke_handler(method, request, context)
      handler = @handlers[method.path]
      raise Error.new(:unimplemented, "Unknown method") unless handler

      unary = handler #: _Handler[untyped, untyped]
      unary.call(request, context)
    end

    # The interceptor chain wraps the invoker, the host's handler-execution
    # boundary, so it covers every unary protocol and custom invokers.
    def invoke_unary(method, request, context, protocol)
      chain = @unary_interceptors
      return @invoker.call(method, request, context) unless chain

      call = ServerUnaryCall.new(method: method, protocol: protocol, request: request, context: context)
      chain.run(call, context) do
        call.__send__(:dispatch!)
        @invoker.call(method, call.request, context)
      end
    end

    def dispatch(path, headers, input, codec, context, lifecycle: nil) # rubocop:disable Metrics/MethodLength, Metrics/AbcSize
      context.check!
      method = @methods[path]
      raise Error.new(:unimplemented, "Unknown method") unless method.is_a?(Method)

      length = Protocol.content_length!(headers["content-length"], invalid_code: :invalid_argument)
      bytes = MessageIO.new(input, @request_wire_limit, context).read(length)
      wire_bytes = bytes.bytesize
      lifecycle.request_bytes = wire_bytes if lifecycle
      bytes = inflate_request(bytes, headers, context)
      request = decode_request(bytes, method, codec, context)
      lifecycle&.message_received(wire_bytes)
      response = invoke_unary(method, request, context, :connect)
      encoding = Protocol.response_encoding(headers, Protocol.content_encoding(headers), @encoding)
      encode_response(response, method, codec, context, encoding, lifecycle)
    end

    def inflate_request(bytes, headers, context)
      if Protocol.content_encoding(headers) == :gzip && !bytes.empty?
        bytes = Compression.decode(bytes, limit: @request_limit, deadline: context,
                                          on_error: ->(error) { context.record_failure(error) })
      end
      Protocol.check_size!(bytes, @request_limit)
      bytes
    rescue Compression::Malformed => e
      context.record_failure(e)
      raise Error.new(:invalid_argument, "Cannot decompress request"), cause: nil
    end

    def decode_request(bytes, method, codec, context)
      context.check!
      request = @binding.decode(bytes, type: method.request_type, codec: codec)
      context.check!
      request
    rescue CodecError => e
      context.record_failure(e)
      raise Error.new(:invalid_argument, "Cannot decode request"), cause: nil
    end

    def encode_response(message, method, codec, context, encoding, lifecycle = nil) # rubocop:disable Metrics/MethodLength, Metrics/ParameterLists
      body = serialize_response(message, method, codec, context)
      headers = response_headers(context, Protocol::CONTENT_TYPES.fetch(codec))
      if encoding == :gzip
        body = Compression.encode(body, limit: @response_wire_limit, deadline: context,
                                        on_error: ->(error) { context.record_failure(error) })
        headers["content-encoding"] = ["gzip"]
      end
      Protocol.check_size!(body, @response_wire_limit)
      context.check!
      context.commit!
      lifecycle&.message_sent(body.bytesize)
      TransportResponse.new(status: 200, headers: headers, body: body)
    end

    def serialize_response(message, method, codec, context)
      context.check!
      raise Error.new(:internal, "Nil response message"), cause: nil if message.nil?

      body = @binding.encode(message, type: method.response_type, codec: codec)
      context.check!
      Protocol.check_size!(body, @response_limit)
      body
    end

    def bare(status, headers, context: nil, cancellation: nil)
      context ? context.commit! : cancellation&.check!
      TransportResponse.new(status: status, headers: headers, body: "")
    end

    # Record once from normalized headers, before validation, so terminal
    # fallbacks without a request Context keep the application metadata.
    def record_request_metadata(lifecycle, headers)
      lifecycle.request_metadata = Instrumentation.carrier(Protocol.metadata(headers))
    end

    def request_context(headers, lifecycle = nil, cancellation = nil)
      validate_headers!(headers)
      Context.new(request_metadata: Protocol.metadata(headers), deadline: request_deadline(headers),
                  lifecycle: lifecycle, cancellation: cancellation)
    rescue ArgumentError => e
      lifecycle&.record_failure(e)
      raise Error.new(:invalid_argument, "Malformed request metadata"), cause: nil
    end

    def validate_headers!(headers)
      if headers.key?("connect-protocol-version") && Protocol.single(headers, "connect-protocol-version") != "1"
        raise Error.new(:invalid_argument, "Unsupported Connect protocol version")
      end

      encoding = Protocol.content_encoding(headers)
      return if encoding == :identity || (encoding == :gzip && @encoding == :gzip)

      supported = @encoding == :gzip ? "gzip, identity" : "identity"
      raise Error.new(:unimplemented, "Supported content encodings: #{supported}")
    end

    def request_deadline(headers)
      return unless headers.key?("connect-timeout-ms")

      value = Protocol.single(headers, "connect-timeout-ms")
      unless value && /\A[0-9]{1,10}\z/.match?(value) && value.to_i.positive?
        raise Error.new(:invalid_argument, "Invalid Connect timeout")
      end

      Deadline.new(value.to_i / 1000.0)
    end

    def response_headers(context, content_type)
      # @type var headers: headers
      headers = context ? context.response_metadata.to_h : {}
      context&.response_trailers&.each { |key, values| headers["trailer-#{key}"] = values }
      headers["accept-encoding"] = ["gzip, identity"] if @encoding == :gzip
      headers.merge("content-type" => [content_type])
    end

    # Reporters run on the calling thread or fiber, inside the instrumented call
    # scope, and never change the peer response.
    def report_unexpected(error, host = nil)
      [@on_error, host].each do |reporter|
        next unless reporter

        begin
          reporter.call(error)
        rescue StandardError
          nil
        end
      end
      nil
    end

    # Report only a converted exception whose generic internal error survived
    # arbitration. Only the portable gRPC-Web unary path carries a host reporter.
    def report_stream_error(error, safe, state)
      return if error.equal?(safe) || !state.error.equal?(safe)

      report_unexpected(error, state.is_a?(GRPCWeb::ServerState) ? state.on_error : nil)
    end

    def terminal_error(error, context, cancellation)
      if context
        context.commit!
      else
        cancellation&.check!
      end
      error
    rescue Error => e
      e
    end

    def error_response(error, context, lifecycle = nil) # rubocop:disable Metrics/MethodLength, Metrics/AbcSize
      # @type var effective: Error?
      # @type var code: String?
      effective = code = nil
      body, status = if lifecycle
                       Protocol.error_body(error, @error_limit) do |selected, emitted|
                         effective = selected
                         code = emitted
                       end
                     else
                       Protocol.error_body(error, @error_limit)
                     end
      headers = response_headers(context, "application/json")
      error.metadata.each { |key, values| headers[key] = (headers[key] || []) + values }
      error.trailers.each { |key, values| headers["trailer-#{key}"] = (headers["trailer-#{key}"] || []) + values }
      response = TransportResponse.new(status: status, headers: headers, body: body)
      if lifecycle
        lifecycle.code = code
        lifecycle.normalized_error = effective
      end
      response
    end
  end
end
