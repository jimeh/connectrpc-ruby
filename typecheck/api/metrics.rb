# frozen_string_literal: true

definitions = ConnectRPC::Metrics::Definitions.new(namespace: "rpc", duration_buckets: [0.1, 1], size_buckets: [64])
instrument = definitions.fetch(:server_handled_total)
[definitions.full_name(instrument), instrument.kind, instrument.labels.size, instrument.buckets&.size,
 definitions.instruments.size, definitions.namespace, ConnectRPC::Metrics::BASE_LABELS.size].inspect
recorder = APIRecorder.new
subscriber = ConnectRPC::Metrics::Subscriber.new(recorder: recorder, definitions: definitions)
ConnectRPC::Instrumentation.new(subscribers: [subscriber])
observer = ConnectRPC::Metrics::ConnectionObserver.new(recorder: recorder, definitions: definitions,
                                                       transport: "billing")
observer.call(APIConnectionEvent.new)
observer.transport.size
# @type var received: ConnectRPC::_ObserverMessageReceived
received = APIMessageObserver.new
received.message_received(5)
# @type var sent: ConnectRPC::_ObserverMessageSent
sent = APIMessageObserver.new
sent.message_sent(7)

prometheus = ConnectRPC::Prometheus::Metrics.new(registry: APIRegistry.new, namespace: :rpc,
                                                 duration_buckets: [1], size_buckets: [1024])
ConnectRPC::Instrumentation.new(subscribers: [prometheus.subscriber])
prometheus.connection_observer(transport: "search").call(APIConnectionEvent.new)
prometheus.definitions.namespace.size
ConnectRPC::Prometheus::VERSION.size

yabeda = ConnectRPC::Yabeda::Metrics.new(group: :grpc, size_buckets: [64])
ConnectRPC::Instrumentation.new(subscribers: [yabeda.subscriber])
yabeda.connection_observer(transport: "billing").call(APIConnectionEvent.new)
yabeda.group.size
