# frozen_string_literal: true

require_relative "schema"
require_relative "framing"
require_relative "telemetry"
require_relative "cancellation_observation"
require "connectrpc/transports/net_http"
require "stringio"
require "zlib"

module Conformance
  # Instrument the actual transport exchange, after connection setup. No request
  # headers or results are repaired by this adapter.
  class ObservedNetHTTP < ConnectRPC::Transports::NetHTTP
    def initialize(name, cancellation: false, observation: nil)
      @test_name = name
      @cancellation_case = cancellation
      @observation = observation
      super()
    end

    def call(request)
      return @observation.observe(:transport) { super } if @observation

      super
    rescue ConnectRPC::Error => e
      Telemetry.record("wire_failure", name: @test_name, code: e.code) unless @observation || @wire_observed
      raise
    end

    private

    # Observe only successful BufferedIO writes, after the underlying socket
    # accepted all bytes. Keep telemetry serialization outside the timed write.
    module WrittenBytes
      attr_accessor :observed_writes, :last_write_at, :write_attempts

      def write(*parts)
        self.write_attempts += 1
        count = super
        if observed_writes
          observed_writes << parts.join.b
          self.last_write_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        end
        count
      end
    end

    # Observe lifecycle calls where they actually return, including setup before
    # exchange. A socket never attached to the observer has unknown closure.
    module LifecycleObservation
      def observe_lifecycle
        @observation_mutex = Mutex.new
        @observed_sockets = []
        @observed_close_calls = 0
        @observed_retired = @observed_interrupted = false
      end

      def attach(socket)
        @observation_mutex.synchronize do
          @observed_sockets << socket
          was_open = !socket.closed?
          super
          @observed_interrupted = true if was_open && socket.closed? && @observed_close_calls.positive?
        end
      end

      def close
        @observation_mutex.synchronize do
          was_open = @observed_sockets.any? { |socket| !socket.closed? }
          super
          unless @observed_retired
            @observed_close_calls += 1
            @observed_interrupted = true if was_open && @observed_sockets.all?(&:closed?)
          end
        end
      end

      def retire
        @observation_mutex.synchronize do
          super
          @observed_retired = true
        end
      end

      def socket_observation
        @observation_mutex.synchronize do
          { attached: @observed_sockets.size,
            closed: @observed_sockets.empty? ? nil : @observed_sockets.all?(&:closed?),
            close_calls: @observed_close_calls, interrupted: @observed_interrupted, retired: @observed_retired }
        end
      end
    end

    module LifecycleAssignment
      def socket_lifecycle=(sockets)
        sockets.extend(LifecycleObservation)
        sockets.observe_lifecycle
        super
      end
    end

    def with_connection(http, deadline, cancellation = nil, &)
      return super unless @observation

      http.singleton_class.prepend(LifecycleAssignment)
      @observation.observe(:setup) do
        super
      ensure
        fields = http.socket_lifecycle.socket_observation
        Telemetry.record("socket_cleanup", name: @test_name, **fields)
      end
    end

    def written_header(leading, name)
      return unless leading

      line = leading.lines.find { |field| field.downcase.start_with?("#{name}:") }
      line.split(":", 2).last.strip if line
    end

    def decoded_request(request, body, headers)
      bytes = headers["content-encoding"] == ["gzip"] ? Zlib::GzipReader.new(StringIO.new(body)).read : body
      type = request.url.path.end_with?("/IdempotentUnary") ? IdempotentUnaryRequest : UnaryRequest
      message = headers["content-type"] == ["application/json"] ? type.decode_json(bytes) : type.decode(bytes)
      JSON.parse(type.encode_json(message))
    end

    def exchange(http, request, outgoing)
      return @observation.observe(:exchange) { observed_exchange(http, request, outgoing) { super } } if @observation

      observed_exchange(http, request, outgoing) { super }
    end

    def observed_exchange(http, request, outgoing)
      if @cancellation_case
        buffered = http.instance_variable_get(:@socket)
        buffered.extend(WrittenBytes)
        buffered.observed_writes = []
        buffered.write_attempts = 0
      end
      message = decoded_request(request, outgoing.body, outgoing.to_hash) if @cancellation_case
      Telemetry.record("dispatch", name: @test_name, deadline: !request.deadline.nil?,
                                   response_limit: request.response_limit, request_headers: outgoing.to_hash,
                                   http_method: outgoing.method, query: Telemetry.query(request.url.query),
                                   wire: Telemetry.wire(outgoing.body || "", outgoing.to_hash),
                                   request: message)
      response = yield
      @wire_observed = true
      Telemetry.record("wire_response", name: @test_name, status: response.status,
                                        wire: Telemetry.wire(response.body, response.headers))
      response
    rescue StandardError => e
      if @observation
        Telemetry.record("wire_failure", name: @test_name,
                                         code: e.is_a?(ConnectRPC::Error) ? e.code : nil,
                                         error_class: e.class.name, cause: e.cause&.class&.name)
      end
      raise
    ensure
      if buffered
        raw = buffered.observed_writes.join.b
        leading, body = raw.split("\r\n\r\n", 2)
        headers = outgoing.to_hash
        message = nil
        if body && body.bytesize == written_header(leading, "content-length").to_i
          message = decoded_request(request, body, headers)
        end
        prefix = outgoing.body.byteslice(0, body&.bytesize || 0)
        Telemetry.record("request_written", name: @test_name, bytes: raw.bytesize,
                                            written_at: buffered.last_write_at,
                                            attempts: buffered.write_attempts,
                                            completed_writes: buffered.observed_writes.size,
                                            observer_complete: true,
                                            headers_complete: !body.nil?, body: Telemetry.wire(body || "", headers),
                                            prepared_body_prefix: Telemetry.wire(prefix, headers),
                                            test_name_header: written_header(leading, "x-test-case-name"),
                                            content_length: written_header(leading, "content-length"),
                                            request: message,
                                            timeout_header: leading&.downcase&.include?("connect-timeout-ms:"))
      end
    end
  end

  class ClientAdapter # rubocop:disable Metrics/ClassLength
    def run(input = $stdin, output = $stdout)
      while (request = Framing.read(input, ClientCompatRequest))
        Telemetry.record("received", name: request.test_name, compression: request.compression, codec: request.codec)
        response = perform(request)
        Framing.write(output, response)
        Telemetry.record("responded", name: request.test_name,
                                      result: JSON.parse(ClientCompatResponse.encode_json(response)))
      end
      Telemetry.record("client_eof")
    end

    def perform(request)
      declaration, message, client, metadata, observation = prepare(request)
      result = if request.cancel
                 instruction = JSON.parse(request.cancel.class.encode_json(request.cancel))
                 Telemetry.record("cancellation_instruction", name: request.test_name, instruction: instruction)
                 Telemetry.record("call_admitted", name: request.test_name, timeout_present: request.has_timeout_ms?,
                                                   request: JSON.parse(message.class.encode_json(message)))
                 with_cancellation(request, instruction) do |token|
                   invoke(client, declaration, message, metadata, request, cancellation: token,
                                                                           observation: observation)
                 end
               else
                 invoke(client, declaration, message, metadata, request)
               end
      Telemetry.record("completed", name: request.test_name, outcome: result.error ? "error" : "success",
                                    code: result.error&.code)
      ClientCompatResponse.new(test_name: request.test_name, response: result)
    rescue StandardError => e
      Telemetry.record("adapter_error", name: request.test_name, error: "#{e.class}: #{e.message}")
      ClientCompatResponse.new(test_name: request.test_name,
                               error: ClientErrorResult.new(message: "#{e.class}: #{e.message}"))
    end

    def prepare(request)
      validate!(request)
      declaration = SERVICE.methods.find { |rpc| rpc.name == request["method"] }
      wire = request.request_messages.first
      unless wire.type_url.split("/").last == declaration.request_type.descriptor.name
        raise "Unexpected request type: #{wire.type_url}"
      end

      message = declaration.request_type.decode(wire.value)
      observation = CancellationObservation.new(request.test_name) if request.cancel
      transport = ObservedNetHTTP.new(request.test_name, cancellation: !request.cancel.nil?, observation: observation)
      client = ConnectRPC::Client.new(base_url: "http://#{request.host}:#{request.port}", binding: BINDING,
                                      transport: transport,
                                      use_get: request.use_get_http_method,
                                      codec: request.codec == :CODEC_PROTO ? :proto : :json,
                                      encoding: request.compression == :COMPRESSION_GZIP ? :gzip : :identity,
                                      response_limit: Conformance.limit(request.message_receive_limit))
      [declaration, message, client, Conformance.metadata(request.request_headers), observation]
    end

    def validate!(request)
      supported = request.http_version == :HTTP_VERSION_1 && request.protocol == :PROTOCOL_CONNECT &&
                  %i[CODEC_PROTO
                     CODEC_JSON].include?(request.codec) && %i[COMPRESSION_IDENTITY
                                                               COMPRESSION_GZIP].include?(request.compression) &&
                  request.stream_type == :STREAM_TYPE_UNARY &&
                  request.server_tls_cert.empty? && !request.client_tls_creds && !request.raw_request &&
                  request.request_messages.size == 1 && request.request_delay_ms.zero? &&
                  request.service == SERVICE_NAME && %w[Unary IdempotentUnary Unimplemented].include?(request["method"])
      raise "Unsupported client instruction" unless supported
    end

    def with_cancellation(request, instruction)
      source = ConnectRPC::CancellationSource.new
      mutex = Mutex.new
      condition = ConditionVariable.new
      done = false
      # Arm before the blocking Client call, exactly as the reference client.
      armed_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      expires = armed_at + (instruction.fetch("afterCloseSendMs") / 1000.0)
      Telemetry.record("cancellation_armed", name: request.test_name, armed_at: armed_at, expires: expires)
      timer = Thread.new do
        fire = mutex.synchronize do
          until done
            remaining = expires - Process.clock_gettime(Process::CLOCK_MONOTONIC)
            break unless remaining.positive?

            condition.wait(mutex, remaining)
          end
          !done
        end
        if fire
          requested_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          changed = source.cancel
          Telemetry.record("cancellation_signal", name: request.test_name, requested_at: requested_at, changed: changed,
                                                  reason: source.token.reason)
        end
      end
      yield source.token
    ensure
      mutex&.synchronize do
        done = true
        condition.signal
      end
      timer&.join
    end

    def invoke(client, declaration, message, metadata, request, cancellation: nil, observation: nil)
      timeout = request.has_timeout_ms? ? request.timeout_ms / 1000.0 : nil
      call = -> { client.call(declaration, message, metadata: metadata, timeout: timeout, cancellation: cancellation) }
      response = observation ? observation.observe(:call, timeout: timeout, &call) : call.call
      payloads = declaration == UNIMPLEMENTED ? [] : [response.message.payload || ConformancePayload.new]
      ClientResponseResult.new(payloads: payloads, response_headers: Conformance.headers(response.metadata),
                               response_trailers: Conformance.headers(response.trailers))
    rescue ConnectRPC::Error => e
      details = e.details.map { |detail| Google::Protobuf::Any.new(type_url: "type.googleapis.com/#{detail.type}", value: detail.value) }
      error = Error.new(code: :"CODE_#{e.code.upcase}", message: e.message, details: details)
      ClientResponseResult.new(error: error, response_headers: Conformance.headers(e.metadata),
                               response_trailers: Conformance.headers(e.trailers))
    end
  end
end

Conformance::ClientAdapter.new.run if $PROGRAM_NAME == __FILE__
