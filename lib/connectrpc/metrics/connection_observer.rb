# frozen_string_literal: true

module ConnectRPC
  module Metrics
    # Callable connection observer for client transports that publish connection
    # events (type, state, previous_state, outcome, duration and the active,
    # draining and pending_calls counts), such as
    # ConnectRPC::Async::Transport's connection_observer.
    #
    # The state gauge follows the observer's own last published state rather
    # than the event's previous_state: a transport that replaces its connection
    # manager starts the new one at idle while the old state is still published.
    # Events carry no manager identity, so a late event from a replaced manager
    # can still overwrite the current state and counts.
    class ConnectionObserver
      TRANSPORT = /\A[a-z0-9_]{1,64}\z/

      attr_reader :transport

      def initialize(recorder:, definitions:, transport:)
        raise ArgumentError, "definitions must be ConnectRPC::Metrics::Definitions" unless
          definitions.is_a?(Definitions)

        @recorder = recorder
        @transport = ConnectionObserver.transport!(transport)
        @labels = { transport: @transport }.freeze
        @keys = definitions.instruments.transform_values(&:key).freeze
        @state = :idle
        @counts = { active: 0, draining: 0, pending_calls: 0 }.freeze
        @mutex = Mutex.new
      end

      # Sets every state series (1 for the current state, idle for a new
      # observer, 0 for the others) and the last known counts (0 for a new
      # observer). Sinks call this once the metrics exist.
      def publish_state
        @mutex.synchronize do
          CONNECTION_STATES.each { |state| set_state(state, state == @state ? 1 : 0) }
          record_counts(@counts, nil)
        end
        nil
      end

      def call(event)
        observe(event)
        case event.type
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

      # Every event carries the counts; only a state change moves the state.
      # Tracks both before recording, so a failed recording leaves
      # publish_state with the transport's actual state and counts.
      def observe(event)
        counts = { active: event.active, draining: event.draining, pending_calls: event.pending_calls }.freeze
        changed = event.type == :state_changed
        @mutex.synchronize do
          previous_state = @state
          previous_counts = @counts
          @state = event.state if changed
          @counts = counts
          record_state(previous_state, @state) if changed
          record_counts(counts, previous_counts)
        end
      end

      def record_state(previous, state)
        @recorder.increment(@keys.fetch(:client_connection_state_changes_total), state_labels(state), 1)
        set_state(previous, 0) unless previous == state
        set_state(state, 1)
      end

      # Sets each count series that differs from the previous counts, or every
      # one without previous counts.
      def record_counts(counts, previous)
        counts.each do |name, value|
          next if previous && previous[name] == value

          if name == :pending_calls
            @recorder.set(@keys.fetch(:client_pending_calls), @labels, value)
          else
            @recorder.set(@keys.fetch(:client_connections), state_labels(name), value)
          end
        end
      end

      def attempt_finished(outcome, duration)
        @recorder.increment(@keys.fetch(:client_connection_attempts_total),
                            @labels.merge(outcome: outcome.to_s), 1)
        @recorder.observe(@keys.fetch(:client_connection_attempt_seconds), @labels, duration) if duration
      end

      def set_state(state, value) = @recorder.set(@keys.fetch(:client_connection_state), state_labels(state), value)

      def state_labels(state) = @labels.merge(state: state.to_s)
    end
  end
end
