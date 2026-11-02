# frozen_string_literal: true

require "English"

module ConnectRPC
  class Server # rubocop:disable Metrics/ClassLength
    attr_reader :services

    # Public configuration keeps each validation next to its assignment.
    # rubocop:disable Metrics/MethodLength, Metrics/AbcSize
    def initialize(services:, binding:, handlers: {}, invoker: nil, request_limit: Protocol::DEFAULT_LIMIT,
                   response_limit: Protocol::DEFAULT_LIMIT, request_wire_limit: request_limit,
                   response_wire_limit: response_limit, protocol: :connect, mode: :unary, encoding: :identity,
                   instrumentation: nil)
      Protocol.profile!(protocol, mode, encoding)
      @services = services.dup.freeze
      @methods = declarations(services)
      raise ArgumentError, "Handler path is not declared" unless handlers.keys.all? { |key| @methods.key?(key) }

      @handlers = handlers.dup.freeze
      @invoker = invoker || method(:invoke_handler)
      @instrumentation = instrumentation
      @binding = binding
      @encoding = encoding
      @request_wire_limit = Protocol.limit!(request_wire_limit)
      @response_wire_limit = Protocol.limit!(response_wire_limit)
      @request_limit = Protocol.limit!(request_limit)
      @response_limit = Protocol.limit!(response_limit)
      @error_limit = [@response_limit, @response_wire_limit].min
    end

    # rubocop:enable Metrics/MethodLength, Metrics/AbcSize

    def handles?(path)
      services.any? { |service| path == "/#{service.name}" || path.start_with?("/#{service.name}/") }
    end

    def start_call(path:, headers:)
      return unless @instrumentation

      @instrumentation.start(owner: self, path: path.dup.freeze, role: :server, method: @methods[path],
                             carrier: Instrumentation.carrier(headers))
    end

    # Keep processing and terminal ensures adjacent to their owner.
    # rubocop:disable Metrics/MethodLength, Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
    def call(path:, http_method:, headers:, input:, lifecycle: nil)
      unless lifecycle || @instrumentation
        return process_call(path: path, http_method: http_method, headers: headers, input: input)
      end

      owned = lifecycle.nil?
      lifecycle ||= start_call(path: path, headers: headers)
      lifecycle.begin_processing(self, path)
      begin
        lifecycle.activate do
          process_call(path: path, http_method: http_method, headers: headers, input: input, lifecycle: lifecycle)
        end
      ensure
        lifecycle.processing_finished(code: nil, error: $ERROR_INFO) unless lifecycle.outcome
        lifecycle.finish(delivery: :unobserved) if owned
      end
    end

    private

    def process_call(path:, http_method:, headers:, input:, lifecycle: nil)
      return response = bare(405, "allow" => ["POST"]) unless http_method == "POST"

      headers = Protocol.headers(headers)
      codec = Protocol.content_codec(Protocol.single(headers, "content-type"))
      return response = bare(415, "accept-post" => ["application/proto, application/json"]) unless codec

      context = request_context(headers, lifecycle)
      response = dispatch(path, headers, input, codec, context, lifecycle: lifecycle)
    rescue Error => e
      lifecycle&.record_failure(e)
      response = error_response(e, context, lifecycle)
    rescue StandardError => e
      lifecycle&.record_failure(e)
      response = error_response(Error.new(:internal, "Internal error"), context, lifecycle)
    ensure
      if lifecycle && !lifecycle.outcome
        lifecycle.processing_finished(code: response&.status == 200 ? "OK" : lifecycle.code, error: $ERROR_INFO,
                                      normalized_error: lifecycle.normalized_error,
                                      http_status: response&.status, response_bytes: response&.body&.bytesize,
                                      response_metadata: response ? Instrumentation.carrier(response.headers) : {})
      end
    end

    # rubocop:enable Metrics/MethodLength, Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity

    def declarations(services)
      raise ArgumentError, "Invalid service declarations" unless services.is_a?(Array) && services.all?(Service)
      raise ArgumentError, "Duplicate service" unless services.map(&:name).uniq.size == services.size

      services.flat_map(&:methods).to_h { |method| [method.path, method] }.freeze
    end

    def invoke_handler(method, request, context)
      handler = @handlers[method.path]
      raise Error.new(:unimplemented, "Unknown method") unless handler

      handler.call(request, context)
    end

    def dispatch(path, headers, input, codec, context, lifecycle: nil) # rubocop:disable Metrics/MethodLength, Metrics/AbcSize
      context.check!
      method = @methods[path]
      raise Error.new(:unimplemented, "Unknown method") unless method

      length = Protocol.content_length!(headers["content-length"], invalid_code: :invalid_argument)
      bytes = MessageIO.new(input, @request_wire_limit, context).read(length)
      lifecycle.request_bytes = bytes.bytesize if lifecycle
      bytes = inflate_request(bytes, headers, context)
      request = decode_request(bytes, method, codec, context)
      response = @invoker.call(method, request, context)
      encoding = Protocol.response_encoding(headers, Protocol.content_encoding(headers), @encoding)
      encode_response(response, method, codec, context, encoding)
    end

    def inflate_request(bytes, headers, context)
      if Protocol.content_encoding(headers) == :gzip && !bytes.empty?
        bytes = Compression.decode(bytes, limit: @request_limit, deadline: context.deadline)
      end
      Protocol.check_size!(bytes, @request_limit)
      bytes
    rescue Compression::Malformed => e
      context.record_failure(e)
      raise Error.new(:invalid_argument, "Cannot decompress request")
    end

    def decode_request(bytes, method, codec, context)
      context.check!
      request = @binding.decode(bytes, type: method.request_type, codec: codec)
      context.check!
      request
    rescue CodecError => e
      context.record_failure(e)
      raise Error.new(:invalid_argument, "Cannot decode request")
    end

    def encode_response(message, method, codec, context, encoding)
      body = serialize_response(message, method, codec, context)
      headers = response_headers(context, Protocol::CONTENT_TYPES.fetch(codec))
      if encoding == :gzip
        body = Compression.encode(body, limit: @response_wire_limit, deadline: context.deadline)
        headers["content-encoding"] = ["gzip"]
      end
      Protocol.check_size!(body, @response_wire_limit)
      context.check!
      TransportResponse.new(status: 200, headers: headers, body: body)
    end

    def serialize_response(message, method, codec, context)
      context.check!
      body = @binding.encode(message, type: method.response_type, codec: codec)
      context.check!
      Protocol.check_size!(body, @response_limit)
      body
    end

    def bare(status, headers) = TransportResponse.new(status: status, headers: headers, body: "")

    def request_context(headers, lifecycle = nil)
      validate_headers!(headers)
      metadata = Protocol.metadata(headers)
      lifecycle.request_metadata = Instrumentation.carrier(metadata) if lifecycle
      Context.new(request_metadata: metadata, deadline: request_deadline(headers), lifecycle: lifecycle)
    rescue ArgumentError => e
      lifecycle&.record_failure(e)
      raise Error.new(:invalid_argument, "Malformed request metadata")
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
      headers = context ? context.response_metadata.to_h : {}
      context&.response_trailers&.each { |key, values| headers["trailer-#{key}"] = values }
      headers["accept-encoding"] = ["gzip, identity"] if @encoding == :gzip
      headers.merge("content-type" => [content_type])
    end

    def error_response(error, context, lifecycle = nil) # rubocop:disable Metrics/MethodLength, Metrics/AbcSize
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
