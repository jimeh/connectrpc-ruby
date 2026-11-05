# frozen_string_literal: true

require_relative "native_diagnostics"
require_relative "framing"
require_relative "telemetry"
require_relative "native"
require_relative "native_observation"

module Conformance
  class NativeServerAdapter # rubocop:disable Metrics/ClassLength
    attr_reader :server

    def initialize(config)
      web = config.protocol == :PROTOCOL_GRPC_WEB && ENV["CONNECTRPC_CONFORMANCE_WEB"] == "1"
      validate_configuration(config, web)

      instrumentation = ConnectRPC::Instrumentation.new(subscribers: [NativeSubscriber.new])
      @server = ObservedNativeServer.new(services: [NATIVE_SERVICE], binding: BINDING, encoding: :gzip,
                                         instrumentation: instrumentation,
                                         protocol: if web
                                                     :grpc_web
                                                   else
                                                     (config.protocol == :PROTOCOL_GRPC ? :grpc : :connect)
                                                   end,
                                         request_limit: Conformance.limit(config.message_receive_limit),
                                         handlers: { UNARY.path => method(:unary),
                                                     IDEMPOTENT_UNARY.path => method(:idempotent_unary),
                                                     SERVER_STREAM.path => method(:server_stream),
                                                     CLIENT_STREAM.path => method(:client_stream),
                                                     BIDI_STREAM.path => method(:bidi_stream) })
    end

    def validate_configuration(config, web)
      return if web

      http1 = [ENV.fetch("CONNECTRPC_CONFORMANCE_HTTP1", nil), ENV.fetch("CONNECTRPC_NATIVE_HOST", nil),
               config.protocol, config.http_version] == ["1", "falcon", :PROTOCOL_CONNECT, :HTTP_VERSION_1]
      http2 = %i[PROTOCOL_CONNECT PROTOCOL_GRPC].include?(config.protocol) && config.http_version == :HTTP_VERSION_2
      raise "Unsupported native server configuration" unless http1 || http2
    end

    def unary(request, context)
      UnaryResponse.new(payload: unary_payload(request, context))
    end

    def idempotent_unary(request, context)
      IdempotentUnaryResponse.new(payload: unary_payload(request, context))
    end

    def unary_payload(request, context)
      definition = request.response_definition || UnaryResponseDefinition.new
      Conformance.metadata(definition.response_headers).each { |key, values| context.response_metadata[key] = values }
      Conformance.metadata(definition.response_trailers).each { |key, values| context.response_trailers[key] = values }
      info = request_info(context, [Conformance.pack(request)])
      Conformance.delay(definition.response_delay_ms)
      context.check!
      emit_error(definition.error, info) if definition.error
      ConformancePayload.new(data: definition.response_data, request_info: info)
    end

    def request_info(context, requests)
      info = RequestInfo.new(request_headers: Conformance.headers(context.request_metadata), requests: requests)
      info.timeout_ms = (context.deadline.remaining * 1000).floor if context.deadline
      info
    end

    def publish_definition(definition, context)
      Conformance.metadata(definition.response_trailers).each { |key, values| context.response_trailers[key] = values }
      context.send_headers(metadata: Conformance.metadata(definition.response_headers))
    end

    def emit_error(error, info = nil)
      details = error.details.map do |detail|
        ConnectRPC::ErrorDetail.new(type: detail.type_url.split("/").last, value: detail.value)
      end
      details << ConnectRPC::ErrorDetail.new(type: RequestInfo.descriptor.name, value: info.to_proto) if info
      raise ConnectRPC::Error.new(error.code.to_s.delete_prefix("CODE_").downcase, error.message, details: details)
    end

    def server_stream(request, writer, context)
      definition = request.response_definition || StreamResponseDefinition.new
      publish_definition(definition, context)
      info = request_info(context, [Conformance.pack(request)])
      definition.response_data.each_with_index do |data, index|
        Conformance.delay(definition.response_delay_ms)
        context.check!
        payload = ConformancePayload.new(data: data, request_info: index.zero? ? info : nil)
        writer.send_message(ServerStreamResponse.new(payload: payload))
      end
      emit_error(definition.error, definition.response_data.empty? ? info : nil) if definition.error
    end

    def client_stream(reader, context)
      definition = nil
      requests = []
      reader.each do |message|
        unless definition
          definition = message.response_definition || UnaryResponseDefinition.new
          publish_definition(definition, context)
        end
        requests << Conformance.pack(message)
      end
      definition ||= UnaryResponseDefinition.new
      info = request_info(context, requests)
      Conformance.delay(definition.response_delay_ms)
      context.check!
      emit_error(definition.error, info) if definition.error
      ClientStreamResponse.new(payload: ConformancePayload.new(data: definition.response_data, request_info: info))
    end

    def bidi_stream(duplex, context)
      definition = nil
      requests = []
      full = false
      sent = 0
      duplex.each do |message|
        unless definition
          definition = initial_bidi_definition(message, context)
          full = message.full_duplex
        end
        requests << Conformance.pack(message)
        next unless full
        break if sent >= definition.response_data.size

        info = sent.zero? ? request_info(context, requests) : RequestInfo.new(requests: requests)
        send_bidi_response(duplex, definition, sent, info)
        sent += 1
        requests = []
      end
      complete_bidi_response(duplex, definition || StreamResponseDefinition.new, sent, requests, context)
    end

    def initial_bidi_definition(message, context)
      definition = message.response_definition || StreamResponseDefinition.new
      publish_definition(definition, context)
      definition
    end

    def send_bidi_response(duplex, definition, sent, info)
      Conformance.delay(definition.response_delay_ms)
      duplex.send_message(BidiStreamResponse.new(payload: ConformancePayload.new(data: definition.response_data[sent],
                                                                                 request_info: info)))
    end

    def complete_bidi_response(duplex, definition, sent, requests, context)
      while sent < definition.response_data.size
        send_bidi_response(duplex, definition, sent, sent.zero? ? request_info(context, requests) : nil)
        sent += 1
      end
      emit_error(definition.error, sent.zero? ? request_info(context, requests) : nil) if definition.error
    end
  end
end

require_relative "native_server_process"
Conformance::NativeServerAdapter.run if $PROGRAM_NAME == __FILE__
