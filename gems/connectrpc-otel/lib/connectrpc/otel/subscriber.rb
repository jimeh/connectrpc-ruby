# frozen_string_literal: true

require_relative "version"

module ConnectRPC
  module OTel
    SEMCONV_VERSION = "1.44.0"

    class Subscriber
      SERVER_ERRORS = %w[unknown deadline_exceeded unimplemented internal unavailable data_loss].freeze
      private_constant :SERVER_ERRORS

      # Without an explicit propagator, the global one is read at each use, so
      # SDK configuration after construction applies.
      def initialize(tracer_provider: OpenTelemetry.tracer_provider, propagator: nil,
                     request_metadata: [], response_metadata: [])
        @tracer = tracer_provider.tracer("connectrpc-otel", VERSION,
                                         attributes: { "connectrpc.semconv.version" => SEMCONV_VERSION })
        @propagator = propagator
        @request_metadata = allowlist(request_metadata)
        @response_metadata = allowlist(response_metadata)
      end

      def start(call) # rubocop:disable Metrics/MethodLength, Metrics/AbcSize, Metrics/PerceivedComplexity
        parent = if call.role == :server
                   # Always extract serialized propagation, even for an in-process call.
                   (@propagator || OpenTelemetry.propagation)
                     .extract(call.carrier, context: OpenTelemetry::Context.empty, getter: Getter.new)
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
          inject(call.carrier, context) if call.role == :client
          Observer.new(span, context, call.role, @request_metadata, @response_metadata, call.protocol)
        rescue Exception # rubocop:disable Lint/RescueException -- release span and preserve fatal exceptions
          span.finish
          raise
        end
      end

      private

      # Propagators may replace, join, edit in place or remove the shared
      # carrier's baggage. Keep baggage that earlier subscribers staged exactly
      # once, ahead of the members this propagator added.
      def inject(carrier, context) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity
        # Copy entries too: a propagator may mutate the staged strings in place.
        staged = Array(carrier["baggage"]).map(&:dup).freeze
        (@propagator || OpenTelemetry.propagation).inject(carrier, context: context)
        return if staged.empty?

        # Drop each staged member once from what the propagator left behind.
        remaining = baggage_members(staged)
        added = baggage_members(Array(carrier["baggage"])).reject do |member|
          index = remaining.index(member)
          remaining.delete_at(index) if index
        end
        joined, others = added.partition { |member| member.is_a?(String) }
        carrier["baggage"] = [*staged, *([joined.join(",")] unless joined.empty?), *others]
      end

      # Comma-separated W3C list members, so joined values compare member by
      # member. Entries that are not printable ASCII header strings pass through
      # unchanged, so core's metadata validation still rejects the injection.
      def baggage_members(entries)
        entries.flat_map do |entry|
          next [entry] unless entry.is_a?(String) && entry.ascii_only? && /\A[\x20-\x7e]*\z/.match?(entry)

          entry.split(",").map(&:strip).reject(&:empty?)
        end
      end

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

      # Carrier keys are lowercase; propagators such as B3 multi-header and X-Ray
      # ask for mixed-case header names.
      class Getter
        def get(carrier, key) # rubocop:disable Metrics/CyclomaticComplexity
          key = key.downcase
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
          fail_span(error) if error
        end

        def finished(completion) # rubocop:disable Metrics/MethodLength, Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
          @span.set_attribute("connectrpc.processing.state", completion.processing_state.to_s)
          @span.set_attribute("connectrpc.decision", completion.decision.to_s)
          @span.set_attribute("connectrpc.delivery", completion.delivery.to_s)
          @span.set_attribute("http.response.status_code", completion.http_status) if completion.http_status
          if completion.delivery == :aborted
            # A non-OK code keeps its role classification: a client cancellation
            # aborts server delivery but is not a server error. Reapplied here
            # because an earlier observer's fatal exit can skip processing_finished.
            outcome = completion.outcome
            if outcome&.code && outcome.code != "OK"
              error = processing_error(outcome)
              fail_span(error) if error
            else
              fail_span("delivery_aborted")
            end
          elsif completion.decision != :processed
            fail_span(completion.decision.to_s)
          end
        ensure
          @span.finish
        end

        private

        # Keep role-specific wire codes and independent operation failures in one table.
        # Status codes take the protocol's casing and other exceptions their class
        # name (semconv); local classifications stay lowercase.
        # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
        def processing_error(outcome)
          if outcome.code && outcome.code != "OK"
            return status_code(outcome.code) if @role == :client || SERVER_ERRORS.include?(outcome.code)

            return nil
          end
          error = outcome.error
          # Anonymous exception classes have no name.
          return error.class.name || "operation_failed" if error && !error.is_a?(ConnectRPC::Error)
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
