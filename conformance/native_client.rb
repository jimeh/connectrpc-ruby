# frozen_string_literal: true

require_relative "native_diagnostics"
require_relative "client"
require_relative "native"
require_relative "native_observation"
require_relative "native_client_operations"

module Conformance
  class NativeClientAdapter < ClientAdapter
    def perform(request)
      Async(finished: false) do
        declaration = native_declaration(request)
        Dir.mktmpdir("native-conformance-") do |directory|
          @observation = NativeObservation.new(request.test_name)
          options = native_transport_options(request, directory)
          transport = ObservedNativeTransport.new(observation: @observation, **options)
          core = native_core(request, transport, options.fetch(:origin))
          execute_native(request, declaration, transport, core)
        end
      end.wait
    rescue StandardError => e
      Telemetry.record("adapter_error", name: request.test_name, error: "#{e.class}: #{e.message}")
      ClientCompatResponse.new(test_name: request.test_name,
                               error: ClientErrorResult.new(message: "#{e.class}: #{e.message}"))
    end

    def native_declaration(request)
      unless request.http_version == :HTTP_VERSION_2 && %i[PROTOCOL_CONNECT PROTOCOL_GRPC].include?(request.protocol) &&
             %i[CODEC_PROTO CODEC_JSON].include?(request.codec) && !request.raw_request
        raise "Unsupported native client instruction"
      end

      NATIVE_SERVICE.methods.find { |method| method.name == request["method"] } || raise("Unknown native method")
    end

    def native_transport_options(request, directory)
      scheme = request.server_tls_cert.empty? ? "http" : "https"
      options = { origin: "#{scheme}://#{request.host}:#{request.port}" }
      unless request.server_tls_cert.empty?
        ca_file = File.join(directory, "ca.pem")
        File.write(ca_file, request.server_tls_cert)
        options[:ca_file] = ca_file
      end
      if request.client_tls_creds
        options[:certificate_chain] = [request.client_tls_creds.cert]
        options[:private_key] = request.client_tls_creds.key
      end
      options
    end

    def native_core(request, transport, origin)
      ObservedNativeClient.new(observation: @observation, base_url: origin, transport: transport, binding: BINDING,
                               codec: request.codec == :CODEC_PROTO ? :proto : :json,
                               protocol: request.protocol == :PROTOCOL_GRPC ? :grpc : :connect,
                               encoding: request.compression == :COMPRESSION_GZIP ? :gzip : :identity,
                               response_limit: Conformance.limit(request.message_receive_limit))
    end

    def execute_native(request, declaration, transport, core)
      source = ConnectRPC::CancellationSource.new
      result = ClientResponseResult.new
      timeout = request.has_timeout_ms? ? request.timeout_ms / 1000.0 : nil
      @sent = 0
      @endpoint = nil
      Telemetry.record("native_call_started", name: request.test_name, timeout: timeout,
                                              stream_type: request.stream_type, codec: request.codec,
                                              cancellation: !request.cancel.nil?,
                                              compression: request.compression, tls: !request.server_tls_cert.empty?)
      begin
        options = { metadata: Conformance.metadata(request.request_headers), timeout: timeout,
                    cancellation: source.token }
        invoke_native(core, declaration, native_messages(request, declaration),
                      request: request, options: options, result: result, source: source)
      rescue ConnectRPC::Error => e
        result.error = Conformance.proto_error(e)
        result.response_headers.replace(Conformance.headers(e.metadata)) if result.response_headers.empty?
        result.response_trailers.replace(Conformance.headers(e.trailers))
        if declaration.is_a?(ConnectRPC::ClientStreamMethod)
          result.num_unsent_requests = request.request_messages.size - @sent
        end
      ensure
        finish_native(request, result, source, transport)
      end
      Telemetry.record("completed", name: request.test_name, outcome: result.error ? "error" : "success",
                                    code: result.error&.code)
      ClientCompatResponse.new(test_name: request.test_name, response: result)
    end

    def native_messages(request, declaration)
      request.request_messages.map do |wire|
        unless wire.type_url.split("/").last == declaration.request_type.descriptor.name
          raise "Native request type mismatch"
        end

        declaration.request_type.decode(wire.value)
      end
    end

    def finish_native(request, result, source, transport)
      @timer&.stop
      @timer = nil
      if @endpoint.is_a?(ConnectRPC::ResponseReader) && @endpoint.trailers
        result.response_trailers.replace(Conformance.headers(@endpoint.trailers))
      end
      transport.close(grace: 0)
      @observation.complete(**transport.closed_facts, source_canceled: source.token.cancelled?)
      Telemetry.record("native_call_finished", name: request.test_name, code: result.error&.code,
                                               source_canceled: source.token.cancelled?,
                                               transport_state: transport.inspect)
    end
  end
end

Conformance::NativeClientAdapter.new.run if $PROGRAM_NAME == __FILE__
