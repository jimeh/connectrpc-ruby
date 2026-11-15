# frozen_string_literal: true

module ConnectRPC
  module Prometheus
    # Registers the fixed ConnectRPC metric set in one prometheus-client registry.
    class Metrics
      attr_reader :definitions, :registry, :subscriber

      def initialize(registry: ::Prometheus::Client.registry, namespace: "grpc",
                     duration_buckets: ::ConnectRPC::Metrics::Definitions::DURATION_BUCKETS,
                     size_buckets: ::ConnectRPC::Metrics::Definitions::SIZE_BUCKETS)
        @definitions = ::ConnectRPC::Metrics::Definitions.new(namespace: namespace, duration_buckets: duration_buckets,
                                                              size_buckets: size_buckets)
        @registry = registry
        @recorder = Recorder.new(@definitions.instruments.transform_values { |instrument| register(instrument) })
        @subscriber = ::ConnectRPC::Metrics::Subscriber.new(recorder: @recorder, definitions: @definitions)
        @transports = {}
        @mutex = Mutex.new
      end

      # One observer per transport name; a repeated name would merge two transports' states.
      def connection_observer(transport:)
        @mutex.synchronize do
          raise ArgumentError, "transport #{transport.inspect} already has an observer" if @transports.key?(transport)

          @transports[transport] = ::ConnectRPC::Metrics::ConnectionObserver.new(
            recorder: @recorder, definitions: @definitions, transport: transport
          )
        end
      end

      private

      def register(instrument)
        name = @definitions.full_name(instrument).to_sym
        options = { docstring: instrument.help, labels: instrument.labels }
        case instrument.kind
        when :counter then @registry.counter(name, **options)
        when :histogram then @registry.histogram(name, **options, buckets: instrument.buckets)
        else @registry.gauge(name, **options, store_settings: gauge_settings)
        end
      end

      # The default stores reject settings; forked DirectFileStore workers sum their state gauges.
      def gauge_settings
        store = ::Prometheus::Client.config.data_store
        return {} unless defined?(::Prometheus::Client::DataStores::DirectFileStore) &&
                         store.is_a?(::Prometheus::Client::DataStores::DirectFileStore)

        { aggregation: :sum }
      end

      # The neutral recorder SPI over registered prometheus-client metrics.
      class Recorder
        def initialize(metrics)
          @metrics = metrics.freeze
        end

        def increment(key, labels, by) = @metrics.fetch(key).increment(by: by, labels: labels)
        def observe(key, labels, value) = @metrics.fetch(key).observe(value, labels: labels)
        def set(key, labels, value) = @metrics.fetch(key).set(value, labels: labels)
      end
      private_constant :Recorder
    end
  end
end
