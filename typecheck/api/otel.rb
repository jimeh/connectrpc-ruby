# frozen_string_literal: true

subscriber = ConnectRPC::OTel::Subscriber.new(tracer_provider: APITracerProvider.new, propagator: APIPropagator.new,
                                              request_metadata: ["x-request-id"], response_metadata: ["x-region"])
ConnectRPC::Instrumentation.new(subscribers: [subscriber])
ConnectRPC::OTel::Subscriber.new
ConnectRPC::OTel::SEMCONV_VERSION.size
# @type var observer: ConnectRPC::_ObserverFinished
observer = APISubscriber.new
completion = ConnectRPC::Instrumentation::Completion.new(
  outcome: nil, processing_state: :not_started, decision: :admission_rejected, delivery: :aborted,
  error: nil, http_status: 503, duration: 0.0
)
observer.finished(completion)

ConnectRPC::OTel::Subscriber.new(tracer_provider: APITracerProvider.new)
APISpan.new.context.valid?
