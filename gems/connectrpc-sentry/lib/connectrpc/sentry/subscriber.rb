# frozen_string_literal: true

require_relative "version"

module ConnectRPC
  module Sentry
    # Inside this namespace the Sentry SDK is always ::Sentry.
    class Subscriber
      ORIGIN = "auto.rpc.connectrpc"
      private_constant :ORIGIN

      def initialize(capture_exceptions: true)
        unless [true, false].include?(capture_exceptions)
          raise ArgumentError, "capture_exceptions must be true or false"
        end

        @capture_exceptions = capture_exceptions
        @guard = Guard.new
      end

      def start(call)
        return unless ::Sentry.initialized?

        rpc = RPC.build(call)
        inert = Support.inert?(::Sentry.configuration)
        # Fail closed: no span, no injection and no scope change; only capture remains.
        return capture_only(call, rpc, clear: !inert) if inert || !@guard.safe?

        call.role == :server ? server(call, rpc) : client(call, rpc)
      end

      private

      def capture_only(call, rpc, clear:)
        Capture.new(rpc, clear: clear) if call.role == :server && @capture_exceptions
      end

      def server(call, rpc)
        scope = ::Sentry.get_current_scope
        parent = scope&.get_span
        if parent && parent.timestamp.nil?
          span = Support.annotate(parent.start_child(op: "rpc.server", description: rpc.name, origin: ORIGIN), rpc)
          return Child.new(rpc, span, scope, @guard, capture: @capture_exceptions, nested: true)
        end

        incoming = incoming(call.carrier)
        hub = ::Sentry.get_current_hub
        Owned.new(rpc, owned_transaction(hub, rpc, incoming), hub, incoming, @guard, capture: @capture_exceptions)
      end

      def owned_transaction(hub, rpc, incoming)
        return unless ::Sentry.configuration.tracing_enabled?

        context = ::Sentry::PropagationContext.new(nil, incoming)
        transaction = ::Sentry::Transaction.new(
          name: rpc.name, source: :route, op: "rpc.server", origin: ORIGIN,
          trace_id: context.trace_id, parent_span_id: context.parent_span_id, parent_sampled: context.parent_sampled,
          baggage: (context.baggage if context.incoming_trace), sample_rand: context.sample_rand
        )
        sampling = { connectrpc: { role: :server, protocol: rpc.protocol, method: rpc.method_name } }
        hub.start_transaction(transaction: Support.annotate(transaction, rpc), custom_sampling_context: sampling)
      end

      def client(call, rpc)
        scope = ::Sentry.get_current_scope
        parent = scope&.get_span
        if parent && parent.timestamp.nil?
          span = Support.annotate(parent.start_child(op: "rpc.client", description: rpc.name, origin: ORIGIN), rpc)
        end
        inject(call, span) if propagate?(call)
        span && Child.new(rpc, span, scope, @guard, capture: false, nested: false)
      end

      def incoming(carrier)
        trace = carrier["sentry-trace"]
        baggage = carrier["baggage"]
        { "sentry-trace" => (trace.first if trace&.size == 1),
          "baggage" => (baggage.join(",") if baggage && !baggage.empty?) }.compact
      end

      # Call has no URL, so targets match the host and host:port.
      def propagate?(call)
        configuration = ::Sentry.configuration
        return false unless configuration.propagate_traces

        host = call.server_address.to_s
        candidates = [host, ("#{host}:#{call.server_port}" if call.server_address && call.server_port)].compact
        configuration.trace_propagation_targets.any? do |target|
          candidates.any? { |candidate| candidate.match?(target) }
        end
      end

      # Earlier subscribers' staged baggage stays after Sentry's entries.
      def inject(call, span)
        headers = if span
                    { "sentry-trace" => span.to_sentry_trace, "baggage" => span.to_baggage }
                  else
                    ::Sentry.get_trace_propagation_headers || {}
                  end
        carrier = call.carrier
        trace = headers["sentry-trace"]
        carrier["sentry-trace"] = [trace] if trace
        baggage = headers["baggage"]
        carrier["baggage"] = [baggage, *carrier["baggage"]] if baggage && !baggage.empty?
      end

      module Support
        STATUS = { "OK" => "ok", "canceled" => "cancelled", "unknown" => "unknown_error",
                   "internal" => "internal_error" }.freeze
        CAPTURED_CODES = %w[internal unknown].freeze

        module_function

        # Tracing and propagation stay off when another instrumenter feeds Sentry.
        def inert?(configuration)
          return true if configuration.instrumenter == :otel

          configuration.respond_to?(:otlp) && configuration.otlp&.enabled ? true : false
        end

        def status(code) = STATUS.fetch(code, code)

        def annotate(span, rpc)
          rpc.data.each { |key, value| span.set_data(key, value) }
          span
        end

        # Unexpected handler failures only; deliberate errors and client input arbitrate to other codes.
        def unexpected?(outcome)
          error = outcome.error
          return false unless error.is_a?(StandardError) && !error.is_a?(ConnectRPC::Error)

          normalized = outcome.normalized_error
          code = normalized.is_a?(ConnectRPC::Error) ? normalized.code : outcome.code
          code.nil? || CAPTURED_CODES.include?(code)
        end

        # Copies every scope attribute, including those update_from_scope omits.
        # update_from_scope shares the source's containers, so it reads from a
        # Scope#dup (deep copies) and changes to the copy never reach the source.
        def copy_scope(target, source)
          target.clear
          target.update_from_scope(source.dup)
          # Scope#dup also copies the span object; keep the source's own span.
          span = source.get_span
          target.set_span(span) if span
          target.set_rack_env(source.rack_env)
          target.set_level(source.level)
          source.event_processors.each { |processor| target.add_event_processor(&processor) }
          target
        end

        # Transaction#finish captures with the current scope, so finish on a copy of the RPC scope.
        def finish_transaction(transaction, source, name: nil)
          return transaction.finish unless ::Sentry.initialized?

          ::Sentry.with_scope do |scope|
            source ? copy_scope(scope, source) : scope.clear
            scope.set_transaction_name(name, source: :route) if name
            scope.set_span(transaction)
            transaction.finish
          end
        end
      end
      private_constant :Support

      # Fiber hosts share one thread hub unless the application isolates hubs per fiber.
      class Guard
        WARNING = "ConnectRPC::Sentry: a fiber scheduler is active and config.hub_isolation_level is :thread; " \
                  "RPC tracing and propagation are off. Set config.hub_isolation_level = :fiber."
        private_constant :WARNING

        def initialize
          @lock = Mutex.new
          @warned = false
        end

        def safe?
          return true unless ::Fiber.scheduler
          return true if ::Sentry.configuration.hub_isolation_level == :fiber

          warn_once
          false
        end

        def attachable? = ::Sentry.initialized? && safe?

        private

        def warn_once
          first = @lock.synchronize { !@warned && (@warned = true) }
          ::Sentry.sdk_logger&.warn(::Sentry::LOGGER_PROGNAME) { WARNING } if first
        end
      end
      private_constant :Guard

      RPC = Data.define(:name, :system, :method_name, :protocol, :data, :tags)
      private_constant :RPC

      class RPC
        # Bounded names and attributes only: no payloads, metadata, URLs or exception text.
        def self.build(call)
          system = %i[grpc grpc_web].include?(call.protocol) ? "grpc" : "connectrpc"
          declared = call.method
          name = declared ? "#{declared.service}/#{declared.name}" : system
          method_name = declared ? name : "_OTHER"
          tags = { "rpc.system.name" => system, "rpc.method" => method_name }
          tags["connectrpc.protocol"] = "grpc_web" if call.protocol == :grpc_web
          new(name: name, system: system, method_name: method_name, protocol: call.protocol,
              data: tags.merge(peer(call)).freeze, tags: tags.freeze)
        end

        def self.peer(call)
          return {} unless call.role == :client

          { "server.address" => call.server_address, "server.port" => call.server_port }.compact
        end

        def declared? = method_name != "_OTHER"

        def code(value)
          return value unless system == "grpc"

          value == "canceled" ? "CANCELLED" : value.upcase
        end
      end

      class Observer
        MECHANISM = "connectrpc"
        HTTP_STATUS = "http.response.status_code"
        private_constant :MECHANISM, :HTTP_STATUS

        def initialize(rpc, span, capture:)
          @rpc = rpc
          @span = span
          @capture = capture
        end

        private

        # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
        def record_outcome(outcome)
          span = @span
          return unless span

          span.set_data("rpc.response.status_code", @rpc.code(outcome.code)) if outcome.code
          span.set_data(HTTP_STATUS, outcome.http_status) if outcome.http_status
          span.set_data("connectrpc.processing.duration", outcome.duration)
          span.set_data("connectrpc.request.wire_bytes", outcome.request_bytes) if outcome.request_bytes
          span.set_data("connectrpc.response.wire_bytes", outcome.response_bytes) if outcome.response_bytes
          if outcome.code
            span.set_status(Support.status(outcome.code))
          elsif outcome.http_status
            span.set_http_status(outcome.http_status)
          elsif outcome.error
            span.set_status("unknown_error")
          end
        end

        def record_completion(completion)
          span = @span
          return unless span

          span.set_data("connectrpc.processing.state", completion.processing_state.to_s)
          span.set_data("connectrpc.decision", completion.decision.to_s)
          span.set_data("connectrpc.delivery", completion.delivery.to_s)
          span.set_data(HTTP_STATUS, completion.http_status) if completion.http_status
          code = completion.outcome&.code
          if completion.delivery == :aborted && (code.nil? || code == "OK")
            span.set_status("unknown_error")
          elsif code.nil? && completion.http_status
            span.set_http_status(completion.http_status)
          elsif span.status.nil?
            span.set_status("unknown_error") unless completion.decision == :processed
          end
        end
        # rubocop:enable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity

        def capture(outcome)
          return unless @capture && Support.unexpected?(outcome)

          hint = { mechanism: ::Sentry::Mechanism.new(type: MECHANISM, handled: false) }
          ::Sentry.capture_exception(outcome.error, hint: hint) do |scope|
            prepare(scope)
            scope.set_tags(@rpc.tags)
          end
        end

        def prepare(scope) = scope
      end
      private_constant :Observer

      # Records nothing; reports unexpected failures on the current or a cleared scope.
      class Capture < Observer
        def initialize(rpc, clear:)
          super(rpc, nil, capture: true)
          @clear = clear
        end

        def processing_finished(outcome) = capture(outcome)

        private

        def prepare(scope)
          scope.clear if @clear
          scope
        end
      end
      private_constant :Capture

      # A server transaction continued from the carrier, finished with delivery facts.
      class Owned < Observer
        def initialize(rpc, transaction, hub, incoming, guard, capture:)
          super(rpc, transaction, capture: capture)
          @hub = hub
          @incoming = incoming
          @guard = guard
        end

        def attach # rubocop:disable Metrics/MethodLength
          return unless @guard.attachable?

          hub = ::Sentry.get_current_hub
          # Hosts nest activations; the inner one reuses the active RPC scope so handler changes reach the event.
          return if @scope && hub.current_scope.equal?(@scope)

          hub.push_scope
          scope = hub.current_scope
          begin
            activate(scope)
          rescue StandardError
            hub.pop_scope if hub.current_scope.equal?(scope)
            raise
          end
          [hub, scope]
        end

        def detach(token)
          return unless token

          hub, scope = token
          hub.pop_scope if hub.current_scope.equal?(scope)
        end

        def processing_finished(outcome)
          record_outcome(outcome)
          capture(outcome)
        end

        def finished(completion)
          record_completion(completion)
        ensure
          finish_transaction if @span
        end

        private

        # Transaction#finish stops profiling only on the finishing context's hub,
        # so first release the registration on the hub that started the transaction.
        def finish_transaction
          @hub.stop_profiler!(@span)
        ensure
          Support.finish_transaction(@span, @scope)
        end

        # The first activation becomes the RPC scope; later ones in other contexts copy it.
        def activate(scope)
          if (rpc_scope = @scope)
            Support.copy_scope(scope, rpc_scope)
          else
            scope.generate_propagation_context(@incoming)
            scope.set_transaction_name(@rpc.name, source: :route)
            @scope = scope
          end
          scope.set_span(@span) if @span
        end

        def prepare(scope)
          rpc_scope = @scope
          rpc_scope ? Support.copy_scope(scope, rpc_scope) : scope.clear
          scope.set_span(@span) if @span
          scope
        end
      end
      private_constant :Owned

      # A child of the active span. Nested server spans end when processing ends,
      # while their parent is still open; client spans end at terminal completion.
      class Child < Observer
        def initialize(rpc, span, scope, guard, capture:, nested:)
          super(rpc, span, capture: capture)
          @scope = scope
          @guard = guard
          @nested = nested
          recorder = span.span_recorder
          @rejected = recorder ? recorder.spans.rindex { |recorded| recorded.equal?(span) }.nil? : false
        end

        # Saves and restores the current span; pushes only when there is none to restore.
        # A server child remembers the scope its handler runs on, wherever it started.
        def attach # rubocop:disable Metrics/MethodLength
          return unless @guard.attachable?

          scope = ::Sentry.get_current_scope
          previous = scope.get_span
          if previous
            scope.set_span(@span)
            @handler_scope = scope if @nested
            return [:span, scope, previous]
          end

          hub = ::Sentry.get_current_hub
          hub.push_scope
          scope = hub.current_scope
          # A server child's pushed scope starts from the RPC's start scope, so request data survives.
          Support.copy_scope(scope, @scope) if @nested
          scope.set_span(@span)
          @handler_scope = scope if @nested
          [:scope, hub, scope]
        end

        def detach(token)
          kind, owner, value = token
          if kind == :span
            owner.set_span(value) if owner.get_span.equal?(@span)
          elsif kind == :scope
            owner.pop_scope if owner.current_scope.equal?(value)
          end
        end

        def processing_finished(outcome)
          record_outcome(outcome)
          return unless @nested

          @span.set_data("connectrpc.processing.state", "finished")
          rename
          capture(outcome)
        ensure
          close if @nested
        end

        # A finished child still serializes with its open parent, so late terminal facts land there.
        def finished(completion)
          record_completion(completion) unless @promoted || (@closed && @span.transaction.timestamp)
        ensure
          close
        end

        private

        # Names Sentry's HTTP server transaction after the RPC route.
        def rename
          host = @span.transaction
          return unless @rpc.declared? && host.op == "http.server" && @scope.get_transaction.equal?(host)

          @scope.set_transaction_name(@rpc.name, source: :route)
        end

        def prepare(scope)
          Support.copy_scope(scope, @handler_scope || @scope)
          scope.set_span(@span)
          scope
        end

        def close
          return if @closed

          @closed = true
          @promoted = @rejected || !@span.transaction.timestamp.nil?
          @promoted ? promote : @span.finish
        end

        # A child its parent can no longer carry becomes its own transaction with
        # the same identity, so it is sent exactly once.
        def promote # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
          span = @span
          parent = span.transaction
          transaction = ::Sentry::Transaction.new(
            name: @rpc.name, source: :route, op: span.op, origin: span.origin, description: span.description,
            trace_id: span.trace_id, span_id: span.span_id, parent_span_id: span.parent_span_id,
            sampled: span.sampled, start_timestamp: span.start_timestamp,
            baggage: parent.get_baggage, sample_rand: parent.sample_rand
          )
          span.data.each { |key, value| transaction.set_data(key, value) }
          span.tags.each { |key, value| transaction.set_tag(key, value) }
          transaction.set_status(span.status)
          Support.finish_transaction(transaction, @handler_scope || @scope, name: @rpc.name)
        end
      end
      private_constant :Child
    end
  end
end
