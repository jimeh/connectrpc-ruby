# frozen_string_literal: true

# @type var observer: ConnectRPC::_ObserverFinished
observer = BadAPIObserver.new
completion = ConnectRPC::Instrumentation::Completion.new(
  outcome: nil, processing_state: :not_started, decision: :processed, delivery: :unobserved,
  error: nil, http_status: nil, duration: 0.0
)
observer.finished(completion)
