# frozen_string_literal: true

require_relative "native_client"
require_relative "native_http1_observation"

module Conformance
  class NativeHTTP1ClientAdapter < NativeClientAdapter
    def native_declaration(request)
      unless request.http_version == :HTTP_VERSION_1 && request.protocol == :PROTOCOL_CONNECT &&
             supported_shape?(request)
        raise "Unsupported native HTTP/1 client instruction"
      end

      NATIVE_SERVICE.methods.find { |method| method.name == request["method"] } || raise("Unknown H1 method")
    end

    def supported_shape?(request)
      %i[CODEC_PROTO CODEC_JSON].include?(request.codec) && !request.raw_request &&
        %i[STREAM_TYPE_UNARY STREAM_TYPE_SERVER_STREAM STREAM_TYPE_CLIENT_STREAM].include?(request.stream_type)
    end

    def native_core(request, transport, origin)
      ObservedNativeClient.new(observation: @observation, base_url: origin, transport: transport, binding: BINDING,
                               use_get: request.use_get_http_method,
                               codec: request.codec == :CODEC_PROTO ? :proto : :json,
                               encoding: request.compression == :COMPRESSION_GZIP ? :gzip : :identity,
                               response_limit: Conformance.limit(request.message_receive_limit))
    end

    def perform(request)
      Async(finished: false) do
        declaration = native_declaration(request)
        Dir.mktmpdir("native-http1-conformance-") do |directory|
          @observation = NativeHTTP1Observation.new(request.test_name)
          options = native_transport_options(request, directory)
          transport = ObservedNativeHTTP1Transport.new(observation: @observation, **options)
          core = native_core(request, transport, options.fetch(:origin))
          execute_native(request, declaration, transport, core)
        end
      end.wait
    rescue StandardError => e
      Telemetry.record("adapter_error", name: request.test_name, error: "#{e.class}: #{e.message}")
      ClientCompatResponse.new(test_name: request.test_name,
                               error: ClientErrorResult.new(message: "#{e.class}: #{e.message}"))
    end
  end
end

Conformance::NativeHTTP1ClientAdapter.new.run if $PROGRAM_NAME == __FILE__
