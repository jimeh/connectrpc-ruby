# frozen_string_literal: true

require_relative "schema"
require_relative "framing"
require_relative "telemetry"
require "connectrpc/transports/net_http"

module Conformance
  # Instrument the actual transport exchange, after connection setup. No request
  # headers or results are repaired by this adapter.
  class ObservedNetHTTP < ConnectRPC::Transports::NetHTTP
    def initialize(name)
      @test_name = name
      super()
    end

    def call(request)
      super
    rescue ConnectRPC::Error => e
      Telemetry.record("wire_failure", name: @test_name, code: e.code) unless @wire_observed
      raise
    end

    private

    def exchange(http, request, outgoing)
      Telemetry.record("dispatch", name: @test_name, deadline: !request.deadline.nil?,
                                   response_limit: request.response_limit, request_headers: outgoing.to_hash,
                                   http_method: outgoing.method, query: Telemetry.query(request.url.query),
                                   wire: Telemetry.wire(outgoing.body || "", outgoing.to_hash))
      response = super
      @wire_observed = true
      Telemetry.record("wire_response", name: @test_name, status: response.status,
                                        wire: Telemetry.wire(response.body, response.headers))
      response
    end
  end

  class ClientAdapter
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
      declaration, message, client, metadata = prepare(request)
      if request.cancel
        instruction = JSON.parse(request.cancel.class.encode_json(request.cancel))
        Telemetry.record("unsupported_cancellation", name: request.test_name, instruction: instruction)
      end
      # Caller cancellation has no library API. Execute the real, uncanceled
      # RPC and faithfully return its outcome for the runner to judge.
      result = invoke(client, declaration, message, metadata, request)
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
      client = ConnectRPC::Client.new(base_url: "http://#{request.host}:#{request.port}", binding: BINDING,
                                      transport: ObservedNetHTTP.new(request.test_name),
                                      use_get: request.use_get_http_method,
                                      codec: request.codec == :CODEC_PROTO ? :proto : :json,
                                      encoding: request.compression == :COMPRESSION_GZIP ? :gzip : :identity,
                                      response_limit: Conformance.limit(request.message_receive_limit))
      [declaration, message, client, Conformance.metadata(request.request_headers)]
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

    def invoke(client, declaration, message, metadata, request)
      timeout = request.has_timeout_ms? ? request.timeout_ms / 1000.0 : nil
      response = client.call(declaration, message, metadata: metadata, timeout: timeout)
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
