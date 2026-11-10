# frozen_string_literal: true

require_relative "version"

module ConnectRPC
  module OTel
    SEMCONV_VERSION = "1.44.0"

    class Subscriber
      SERVER_ERRORS = %w[unknown deadline_exceeded unimplemented internal unavailable data_loss].freeze
      private_constant :SERVER_ERRORS

      def initialize(tracer_provider: OpenTelemetry.tracer_provider, propagator: nil,
                     request_metadata: [], response_metadata: [])
        @tracer = tracer_provider.tracer("connectrpc-otel", VERSION,
                                         attributes: { "connectrpc.semconv.version" => SEMCONV_VERSION })
        @propagator = propagator || OpenTelemetry::Context::Propagation::CompositeTextMapPropagator.compose_propagators(
          [OpenTelemetry::Trace::Propagation::TraceContext.text_map_propagator,
           OpenTelemetry::Baggage::Propagation.text_map_propagator]
        )
        @request_metadata = allowlist(request_metadata)
        @response_metadata = allowlist(response_metadata)
      end

      def start(call) # rubocop:disable Metrics/MethodLength, Metrics/AbcSize
        parent = if call.role == :server
                   # Always extract serialized propagation, even for an in-process call.
                   @propagator.extract(call.carrier, context: OpenTelemetry::Context.empty, getter: Getter.new)
                 else
                   OpenTelemetry::Context.current
                 end
        name = if call.method
                 "#{call.method.service}/#{call.method.name}"
               else
                 (%i[grpc grpc_web].include?(call.protocol) ? "grpc" : "connectrpc")
               end
        span = @tracer.start_span(name, kind: call.role, with_parent: parent, attributes: attributes(call, name))
        context = OpenTelemetry::Trace.context_with_span(span, parent_context: parent)
        begin
          @propagator.inject(call.carrier, context: context) if call.role == :client
          Observer.new(span, context, call.role, @request_metadata, @response_metadata, call.protocol)
        rescue Exception # rubocop:disable Lint/RescueException -- release span and preserve fatal exceptions
          span.finish
          raise
        end
      end

      private

      def attributes(call, name)
        result = { "rpc.system.name" => %i[grpc grpc_web].include?(call.protocol) ? "grpc" : "connectrpc",
                   "rpc.method" => call.method ? name : "_OTHER" }
        result["connectrpc.protocol"] = "grpc_web" if call.protocol == :grpc_web
        if call.role == :client
          result["server.address"] = call.server_address if call.server_address
          result["server.port"] = call.server_port if call.server_port
        end
        result
      end

      # Validate the complete allowlist transaction before retaining any configuration.
      # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
      def allowlist(keys)
        unless keys.is_a?(Array) && keys.size <= 32 && keys.all? do |key|
                 key.is_a?(String) && key.bytesize <= 128 && /\A[0-9A-Za-z_.-]+\z/.match?(key) &&
                 !Metadata.reserved?(key.downcase)
               end
          raise ArgumentError, "Metadata capture requires at most 32 explicit metadata names of at most 128 bytes"
        end

        keys.map { |key| key.downcase.freeze }.uniq.freeze
      end

      # rubocop:enable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity

      class Getter
        def get(carrier, key) # rubocop:disable Metrics/CyclomaticComplexity
          values = Array(carrier[key])
          return unless values.all? { |value| value.is_a?(String) && value.ascii_only? }
          return values.first if key == "traceparent" && values.size == 1
          return if key == "traceparent" || values.empty?

          values.join(",")
        end

        def keys(carrier) = carrier.keys
      end
      private_constant :Getter

      class Observer
        def initialize(span, context, role, request_keys, response_keys, protocol) # rubocop:disable Metrics/ParameterLists
          @protocol = protocol
          @span = span
          @context = context
          @role = role
          @request_keys = request_keys
          @response_keys = response_keys
        end

        def attach = OpenTelemetry::Context.attach(@context)
        def detach(token) = OpenTelemetry::Context.detach(token)

        def processing_finished(outcome) # rubocop:disable Metrics/AbcSize
          @span.set_attribute("rpc.response.status_code", status_code(outcome.code)) if outcome.code
          @span.set_attribute("http.response.status_code", outcome.http_status) if outcome.http_status
          @span.set_attribute("connectrpc.processing.duration", outcome.duration)
          @span.set_attribute("connectrpc.request.wire_bytes", outcome.request_bytes) if outcome.request_bytes
          @span.set_attribute("connectrpc.response.wire_bytes", outcome.response_bytes) if outcome.response_bytes
          capture("request", outcome.request_metadata, @request_keys)
          capture("response", outcome.response_metadata, @response_keys)
          error = processing_error(outcome)
          fail_span(status_code(error)) if error
        end

        def finished(completion) # rubocop:disable Metrics/MethodLength, Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
          @span.set_attribute("connectrpc.processing.state", completion.processing_state.to_s)
          @span.set_attribute("connectrpc.decision", completion.decision.to_s)
          @span.set_attribute("connectrpc.delivery", completion.delivery.to_s)
          @span.set_attribute("http.response.status_code", completion.http_status) if completion.http_status
          if completion.delivery == :aborted
            code = completion.outcome&.code
            type = if %i[grpc
                         grpc_web].include?(@protocol) && code && code != "OK"
                     status_code(code)
                   else
                     "delivery_aborted"
                   end
            fail_span(type)
          elsif completion.decision != :processed
            fail_span(completion.decision.to_s)
          end
        ensure
          @span.finish
        end

        private

        # Keep role-specific wire codes and independent operation failures in one table.
        # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
        def processing_error(outcome)
          if outcome.code && outcome.code != "OK"
            return outcome.code if @role == :client || SERVER_ERRORS.include?(outcome.code)

            return nil
          end
          return "operation_failed" if outcome.error && !outcome.error.is_a?(ConnectRPC::Error)
          return "operation_failed" if outcome.code.nil? && outcome.error
          return "preflight_rejected" if outcome.http_status && outcome.http_status != 200 && outcome.code.nil?

          nil
        end

        # rubocop:enable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity

        def fail_span(type)
          @span.set_attribute("error.type", type)
          @span.status = OpenTelemetry::Trace::Status.error
        end

        def status_code(code)
          return code unless %i[grpc grpc_web].include?(@protocol)

          code == "canceled" ? "CANCELLED" : code.upcase
        end

        def capture(direction, metadata, keys) # rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
          keys.each do |key|
            values = metadata[key]
            next unless values

            values = values.first(8).filter_map do |value|
              value.byteslice(0, 256) if value.is_a?(String) && value.ascii_only? && /\A[\x20-\x7e]*\z/.match?(value)
            end
            prefix = %i[grpc grpc_web].include?(@protocol) ? "rpc" : "connectrpc"
            @span.set_attribute("#{prefix}.#{direction}.metadata.#{key}", values) unless values.empty?
          end
        end
      end
      private_constant :Observer
    end
  end
end
