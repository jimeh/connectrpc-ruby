# frozen_string_literal: true

require_relative "schema"
require_relative "framing"
require_relative "telemetry"
require_relative "../examples/localhost"

module Conformance
  class ServerAdapter
    def initialize(config)
      unless config.protocol == :PROTOCOL_CONNECT && config.http_version == :HTTP_VERSION_1 &&
             !config.use_tls && config.client_tls_cert.empty? && !config.server_creds
        raise "Unsupported server configuration"
      end

      @server = ConnectRPC::Server.new(services: [SERVICE], binding: BINDING,
                                       request_limit: Conformance.limit(config.message_receive_limit), encoding: :gzip,
                                       handlers: { UNARY.path => method(:unary),
                                                   IDEMPOTENT_UNARY.path => method(:idempotent_unary) })
    end

    def app
      rack = ConnectRPC::Rack.new(server: @server)
      lambda do |env|
        name = env["HTTP_X_TEST_CASE_NAME"]
        input = Telemetry::Input.new(env["rack.input"])
        env["rack.input"] = input
        Telemetry.record("dispatch", name: name, http_method: env["REQUEST_METHOD"],
                                     query: Telemetry.query(env["QUERY_STRING"]),
                                     request_encoding: env.fetch("HTTP_CONTENT_ENCODING", "identity"),
                                     content_length: env["CONTENT_LENGTH"], content_type: env["CONTENT_TYPE"])
        previous = Thread.current[:conformance_get_query]
        query = env["REQUEST_METHOD"] == "GET" ? env.fetch("QUERY_STRING", "") : nil
        Thread.current[:conformance_get_query] = query
        result = rack.call(env)
        Telemetry.record("completed", name: name, status: result.first, request_wire: input.observation,
                                      response_wire: Telemetry.wire(result[2].join, result[1]))
        result
      rescue StandardError => e
        Telemetry.record("adapter_error", name: name, error: "#{e.class}: #{e.message}")
        raise
      ensure
        Thread.current[:conformance_get_query] = previous
      end
    end

    def unary(request, context)
      UnaryResponse.new(payload: unary_payload(request, context))
    end

    def idempotent_unary(request, context)
      IdempotentUnaryResponse.new(payload: unary_payload(request, context))
    end

    def unary_payload(request, context)
      context.check!
      info = RequestInfo.new(request_headers: Conformance.headers(context.request_metadata),
                             requests: [Conformance.pack(request)])
      if (query = Thread.current[:conformance_get_query])
        params = URI.decode_www_form(query).each_with_object({}) { |(key, value), all| (all[key] ||= []) << value }
        info.connect_get_info = ConformancePayload::ConnectGetInfo.new(query_params: Conformance.headers(params))
      end
      info.timeout_ms = (context.deadline.remaining * 1000).floor if context.deadline
      definition = request.response_definition || UnaryResponseDefinition.new
      raise "Raw responses belong only to reference servers" if definition.raw_response

      Conformance.metadata(definition.response_headers).each { |key, values| context.response_metadata[key] = values }
      Conformance.metadata(definition.response_trailers).each { |key, values| context.response_trailers[key] = values }
      # This delay is the requested RPC behavior, not readiness synchronization.
      sleep(definition.response_delay_ms / 1000.0) if definition.response_delay_ms.positive?
      context.check!
      raise_error(definition.error, info) if definition.error
      ConformancePayload.new(data: definition.response_data, request_info: info)
    end

    def raise_error(error, info)
      details = error.details.map { |detail| ConnectRPC::ErrorDetail.new(type: detail.type_url.split("/").last, value: detail.value) }
      details << ConnectRPC::ErrorDetail.new(type: RequestInfo.descriptor.name, value: info.to_proto)
      code = error.code.to_s.delete_prefix("CODE_").downcase
      raise ConnectRPC::Error.new(code, error.message, details: details)
    end

    def self.run(input = $stdin, output = $stdout)
      # A self-pipe wakes the main thread. Signal handlers never join host workers.
      reader, writer = IO.pipe
      previous = Signal.trap("TERM") { writer.write_nonblock(".", exception: false) }
      config = Framing.read(input, ServerCompatRequest)
      raise "Missing server configuration" unless config

      adapter = new(config)
      host = EchoExample::Localhost.new(adapter.app)
      uri = URI(host.url)
      Framing.write(output, ServerCompatResponse.new(host: uri.host, port: uri.port))
      Telemetry.record("server_ready", port: uri.port)
      # The runner closes stdin after configuration. EOF is observable but does
      # not terminate the server; only SIGTERM wakes the shutdown loop.
      watched = [reader, input]
      loop do
        readable = IO.select(watched).first
        break if readable.include?(reader)
        raise "Unexpected data after server configuration" unless input.read(1).nil?

        Telemetry.record("server_stdin_eof")
        watched.delete(input)
      end
    ensure
      host&.close
      Signal.trap("TERM", previous) if previous
      reader&.close
      writer&.close
      Telemetry.record("server_stopped")
    end
  end
end

Conformance::ServerAdapter.run if $PROGRAM_NAME == __FILE__
