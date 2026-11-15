# frozen_string_literal: true

module ConnectRPC
  module Yabeda
    # Declares the fixed ConnectRPC metric set through Yabeda.configure. Group,
    # name and unit come from the shared definitions, so yabeda-prometheus
    # exports the same names as connectrpc-prometheus.
    class Metrics
      attr_reader :definitions, :group, :subscriber

      def initialize(group: :grpc, duration_buckets: ::ConnectRPC::Metrics::Definitions::DURATION_BUCKETS,
                     size_buckets: ::ConnectRPC::Metrics::Definitions::SIZE_BUCKETS)
        @definitions = ::ConnectRPC::Metrics::Definitions.new(namespace: group, duration_buckets: duration_buckets,
                                                              size_buckets: size_buckets)
        @group = @definitions.namespace.to_sym
        Metrics.declare(@group, @definitions.instruments.values)
        @recorder = Recorder.new(@definitions.instruments.keys.to_h do |key|
          [key, "#{@group}_#{@definitions.fetch(key).name}"]
        end)
        @subscriber = ::ConnectRPC::Metrics::Subscriber.new(recorder: @recorder, definitions: @definitions)
        @transports = {}
        @mutex = Mutex.new
      end

      # One observer per transport name; a repeated name would merge two transports' states.
      # Before Yabeda.configure! the metrics do not exist, and configure blocks run
      # before adapters register them, so a collector publishes the state instead.
      def connection_observer(transport:)
        @mutex.synchronize do
          raise ArgumentError, "transport #{transport.inspect} already has an observer" if @transports.key?(transport)

          observer = ::ConnectRPC::Metrics::ConnectionObserver.new(recorder: @recorder, definitions: @definitions,
                                                                   transport: transport)
          ::Yabeda.configured? ? observer.publish_state : ::Yabeda.collect { observer.publish_state }
          @transports[transport] = observer
        end
      end

      # Runs now when Yabeda is already configured, otherwise at Yabeda.configure!.
      def self.declare(group_name, instruments)
        declarations = instruments.map { |instrument| declaration(instrument) }
        ::Yabeda.configure do
          group(group_name) { declarations.each { |kind, name, options| public_send(kind, name, **options) } }
        end
      end

      # The Yabeda DSL method, metric name and options for one instrument.
      def self.declaration(instrument)
        options = { comment: instrument.help, tags: instrument.labels }
        case instrument.kind
        when :histogram then options.merge!(unit: instrument.unit, buckets: instrument.buckets)
        when :gauge then options[:aggregation] = :sum
        end
        [instrument.kind, instrument.name.to_sym, options]
      end

      # The neutral recorder SPI over Yabeda metrics, resolved at record time so
      # declaration may precede Yabeda.configure!.
      class Recorder
        def initialize(names)
          @names = names.freeze
        end

        def increment(key, labels, by) = metric(key).increment(labels, by: by)
        def observe(key, labels, value) = metric(key).measure(labels, value)
        def set(key, labels, value) = metric(key).set(labels, value)

        private

        def metric(key) = ::Yabeda.metrics.fetch(@names.fetch(key))
      end
      private_constant :Recorder
    end
  end
end
