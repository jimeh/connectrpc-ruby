# frozen_string_literal: true

module ConnectRPC
  # Backend-neutral RPC metrics. Sinks implement the recorder SPI:
  # increment(key, labels, by), observe(key, labels, value), set(key, labels, value).
  module Metrics
    # One instrument. Exported name is namespace, name and optional unit joined by "_".
    Instrument = Data.define(:key, :kind, :name, :unit, :labels, :buckets, :help)

    BASE_LABELS = %i[grpc_type grpc_service grpc_method rpc_protocol].freeze
    CONNECTION_STATES = %i[idle connecting ready transient_failure closing closed].freeze

    # The fixed metric set. Names and labels follow go-grpc-middleware providers/prometheus.
    class Definitions # rubocop:disable Metrics/ClassLength -- one declarative list of the fixed metric set
      NAMESPACE = /\A[a-z_][a-z0-9_]*\z/
      DURATION_BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10].freeze
      SIZE_BUCKETS = [64, 256, 1024, 4096, 16_384, 65_536, 262_144, 1_048_576, 4_194_304].freeze

      # Help text shared with go-grpc-middleware providers/prometheus where the names match.
      UPSTREAM_HELP = {
        server: {
          started: "Total number of RPCs started on the server.",
          handled: "Total number of RPCs completed on the server, regardless of success or failure.",
          handling: "Histogram of response latency (seconds) of gRPC that had been application-level handled " \
                    "by the server.",
          received: "Total number of RPC stream messages received on the server.",
          sent: "Total number of gRPC stream messages sent by the server."
        }.freeze,
        client: {
          started: "Total number of RPCs started on the client.",
          handled: "Total number of RPCs completed by the client, regardless of success or failure.",
          handling: "Histogram of response latency (seconds) of the gRPC until it is finished by the application.",
          received: "Total number of RPC stream messages received by the client.",
          sent: "Total number of gRPC stream messages sent by the client."
        }.freeze
      }.freeze
      private_constant :UPSTREAM_HELP

      attr_reader :namespace, :instruments

      def initialize(namespace: "grpc", duration_buckets: DURATION_BUCKETS, size_buckets: SIZE_BUCKETS)
        @namespace = Definitions.namespace!(namespace)
        durations = Definitions.buckets!(duration_buckets, "duration_buckets")
        sizes = Definitions.buckets!(size_buckets, "size_buckets")
        @instruments = (rpc_instruments(:server, durations, sizes) + rpc_instruments(:client, durations, sizes) +
                        connection_instruments(durations)).to_h { |instrument| [instrument.key, instrument] }.freeze
        freeze
      end

      def fetch(key) = @instruments.fetch(key)

      # The exported name: namespace, name and unit, as Prometheus and yabeda-prometheus build it.
      def full_name(instrument)
        [@namespace, instrument.name, instrument.unit].compact.join("_")
      end

      def self.namespace!(value)
        unless (value.is_a?(String) || value.is_a?(Symbol)) && NAMESPACE.match?(value.to_s)
          raise ArgumentError, "namespace must match #{NAMESPACE.source}"
        end

        value.to_s.dup.freeze
      end

      def self.buckets!(values, name)
        unless values.is_a?(Array) && !values.empty? && values.all? { |value| bound?(value) } &&
               values.each_cons(2).all? { |low, high| low < high }
          raise ArgumentError, "#{name} must be strictly increasing positive numbers"
        end

        values.dup.freeze
      end

      def self.bound?(value) = value.is_a?(Numeric) && value.real? && value.finite? && value.positive?
      private_class_method :bound?

      private

      def rpc_instruments(role, durations, sizes) # rubocop:disable Metrics/MethodLength, Metrics/AbcSize
        help = UPSTREAM_HELP.fetch(role)
        on = role == :server ? "on the server" : "by the client"
        by = role == :server ? "by the server" : "by the client"
        base = BASE_LABELS
        instruments = [
          counter(role, "started_total", base, help.fetch(:started)),
          counter(role, "handled_total", [*base, :grpc_code], help.fetch(:handled)),
          histogram(role, "handling", :seconds, base, durations, help.fetch(:handling)),
          counter(role, "msg_received_total", base, help.fetch(:received)),
          counter(role, "msg_sent_total", base, help.fetch(:sent)),
          histogram(role, "msg_received", :bytes, base, sizes,
                    "Histogram of encoded message sizes (bytes) received #{on}."),
          histogram(role, "msg_sent", :bytes, base, sizes,
                    "Histogram of encoded message sizes (bytes) sent #{by}.")
        ]
        return instruments unless role == :server

        instruments + [
          counter(role, "rejected_total", [*base, :reason],
                  "Total number of server RPC lifecycles rejected before a handled status."),
          counter(role, "delivery_aborted_total", base,
                  "Total number of handled server RPCs whose response delivery was aborted.")
        ]
      end

      def connection_instruments(durations) # rubocop:disable Metrics/MethodLength
        [
          counter(:client, "connection_attempts_total", %i[transport outcome],
                  "Total number of finished client connection attempts."),
          histogram(:client, "connection_attempt", :seconds, %i[transport], durations,
                    "Histogram of client connection attempt durations (seconds)."),
          counter(:client, "connection_state_changes_total", %i[transport state],
                  "Total number of client connection state changes, by new state."),
          gauge(:client, "connection_state", %i[transport state],
                "Current client connection state: 1 for the current state, 0 otherwise."),
          counter(:client, "admission_rejected_total", %i[transport],
                  "Total number of client calls rejected by connection admission."),
          gauge(:client, "connections", %i[transport state],
                "Current client connections by state: active accepts new calls; draining no longer does and " \
                "closes when its calls end."),
          gauge(:client, "pending_calls", %i[transport], "Current client calls waiting for a connection.")
        ]
      end

      def counter(role, name, labels, help)
        Instrument.new(key: :"#{role}_#{name}", kind: :counter, name: "#{role}_#{name}", unit: nil,
                       labels: labels.dup.freeze, buckets: nil, help: help)
      end

      def gauge(role, name, labels, help)
        Instrument.new(key: :"#{role}_#{name}", kind: :gauge, name: "#{role}_#{name}", unit: nil,
                       labels: labels.dup.freeze, buckets: nil, help: help)
      end

      def histogram(role, name, unit, labels, buckets, help) # rubocop:disable Metrics/ParameterLists
        Instrument.new(key: :"#{role}_#{name}_#{unit}", kind: :histogram, name: "#{role}_#{name}", unit: unit,
                       labels: labels.dup.freeze, buckets: buckets, help: help)
      end
    end
  end
end
