# frozen_string_literal: true

module ConnectRPC
  module Metrics
    # Callable connection observer for client transports that publish connection
    # events (type, state, previous_state, outcome, duration), such as
    # ConnectRPC::Async::Transport's connection_observer.
    class ConnectionObserver
      TRANSPORT = /\A[a-z0-9_]{1,64}\z/

      attr_reader :transport

      def initialize(recorder:, definitions:, transport:)
        raise ArgumentError, "definitions must be ConnectRPC::Metrics::Definitions" unless
          definitions.is_a?(Definitions)

        @recorder = recorder
        @transport = ConnectionObserver.transport!(transport)
        @labels = { transport: @transport }.freeze
        @keys = %i[client_connection_attempts_total client_connection_attempt_seconds
                   client_connection_state_changes_total client_connection_state
                   client_admission_rejected_total].to_h { |key| [key, definitions.fetch(key).key] }.freeze
        initialize_state
      end

      def call(event)
        case event.type
        when :state_changed then state_changed(event.previous_state, event.state)
        when :attempt_finished then attempt_finished(event.outcome, event.duration)
        when :admission_rejected then @recorder.increment(@keys.fetch(:client_admission_rejected_total), @labels, 1)
        end
        nil
      end

      def self.transport!(name)
        unless name.is_a?(String) && TRANSPORT.match?(name)
          raise ArgumentError, "transport must match #{TRANSPORT.source}"
        end

        name.dup.freeze
      end

      private

      # Every state series exists from the start; a new transport is idle.
      def initialize_state
        CONNECTION_STATES.each do |state|
          @recorder.set(@keys.fetch(:client_connection_state), state_labels(state), state == :idle ? 1 : 0)
        end
      end

      def state_changed(previous, state)
        @recorder.increment(@keys.fetch(:client_connection_state_changes_total), state_labels(state), 1)
        @recorder.set(@keys.fetch(:client_connection_state), state_labels(previous), 0) if previous
        @recorder.set(@keys.fetch(:client_connection_state), state_labels(state), 1)
      end

      def attempt_finished(outcome, duration)
        @recorder.increment(@keys.fetch(:client_connection_attempts_total),
                            @labels.merge(outcome: outcome.to_s), 1)
        @recorder.observe(@keys.fetch(:client_connection_attempt_seconds), @labels, duration) if duration
      end

      def state_labels(state) = @labels.merge(state: state.to_s)
    end
  end
end
