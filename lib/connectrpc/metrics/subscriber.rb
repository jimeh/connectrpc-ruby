# frozen_string_literal: true

module ConnectRPC
  module Metrics
    # Connect code strings to Go codes.Code#String values; anything else is Unknown.
    CODES = {
      "ok" => "OK", "canceled" => "Canceled", "unknown" => "Unknown", "invalid_argument" => "InvalidArgument",
      "deadline_exceeded" => "DeadlineExceeded", "not_found" => "NotFound", "already_exists" => "AlreadyExists",
      "permission_denied" => "PermissionDenied", "resource_exhausted" => "ResourceExhausted",
      "failed_precondition" => "FailedPrecondition", "aborted" => "Aborted", "out_of_range" => "OutOfRange",
      "unimplemented" => "Unimplemented", "internal" => "Internal", "unavailable" => "Unavailable",
      "data_loss" => "DataLoss", "unauthenticated" => "Unauthenticated"
    }.freeze
    OTHER = "_OTHER"

    def self.code(value)
      CODES.fetch(value.to_s.downcase, "Unknown")
    end

    # Labels come only from the declared method; undeclared paths collapse to _OTHER.
    def self.labels(call)
      method = call.method
      labels = if method
                 { grpc_type: method.cardinality.to_s, grpc_service: method.service, grpc_method: method.name }
               else
                 { grpc_type: "unknown", grpc_service: OTHER, grpc_method: OTHER }
               end
      labels[:rpc_protocol] = call.protocol.to_s
      labels.freeze
    end

    # Instrumentation subscriber recording the fixed RPC metric set through a recorder.
    class Subscriber
      ROLE_KEYS = %i[started_total handled_total handling_seconds msg_received_total msg_sent_total
                     msg_received_bytes msg_sent_bytes].freeze
      SERVER_KEYS = %i[rejected_total delivery_aborted_total].freeze
      private_constant :ROLE_KEYS, :SERVER_KEYS

      def initialize(recorder:, definitions:)
        raise ArgumentError, "definitions must be ConnectRPC::Metrics::Definitions" unless
          definitions.is_a?(Definitions)

        @recorder = recorder
        # Resolve every recorder key once; each must name a defined instrument.
        @keys = %i[server client].to_h do |role|
          names = role == :server ? ROLE_KEYS + SERVER_KEYS : ROLE_KEYS
          [role, names.to_h { |name| [name, definitions.fetch(:"#{role}_#{name}").key] }.freeze]
        end.freeze
      end

      def start(call)
        labels = Metrics.labels(call)
        keys = @keys.fetch(call.role)
        @recorder.increment(keys.fetch(:started_total), labels, 1)
        Observer.new(@recorder, keys, call.role == :server, labels)
      end

      # Per-call observer. Callbacks may run on separate read and write fibers.
      class Observer
        def initialize(recorder, keys, server, labels)
          @recorder = recorder
          @keys = keys
          @server = server
          @labels = labels
        end

        def message_received(bytes)
          @recorder.increment(@keys.fetch(:msg_received_total), @labels, 1)
          @recorder.observe(@keys.fetch(:msg_received_bytes), @labels, bytes)
        end

        def message_sent(bytes)
          @recorder.increment(@keys.fetch(:msg_sent_total), @labels, 1)
          @recorder.observe(@keys.fetch(:msg_sent_bytes), @labels, bytes)
        end

        # Completion classification; see docs/plans/metrics.md and the package READMEs.
        def finished(completion)
          reason = rejection(completion)
          return reject(reason) if reason

          outcome = completion.outcome
          handled(Metrics.code(Observer.code(outcome) || "unknown"), outcome ? outcome.duration : completion.duration)
          @recorder.increment(@keys.fetch(:delivery_aborted_total), @labels, 1) if
            @server && completion.delivery == :aborted
        end

        def self.code(outcome)
          return unless outcome

          error = outcome.normalized_error
          outcome.code || (error.code if error.is_a?(Error))
        end

        private

        # Servers reject non-processed decisions and processed lifecycles without a code or error.
        def rejection(completion)
          return unless @server
          return completion.decision.to_s unless completion.decision == :processed

          outcome = completion.outcome
          "unprocessed" if Observer.code(outcome).nil? && !outcome&.error
        end

        def handled(code, duration)
          @recorder.increment(@keys.fetch(:handled_total), @labels.merge(grpc_code: code), 1)
          @recorder.observe(@keys.fetch(:handling_seconds), @labels, duration)
        end

        def reject(reason)
          @recorder.increment(@keys.fetch(:rejected_total), @labels.merge(reason: reason), 1)
        end
      end
    end
  end
end
