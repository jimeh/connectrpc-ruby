# frozen_string_literal: true

subscriber = ConnectRPC::Sentry::Subscriber.new(capture_exceptions: false)
ConnectRPC::Instrumentation.new(subscribers: [subscriber, ConnectRPC::Sentry::Subscriber.new])
method = ConnectRPC::Method.new(service: "demo.Service", name: "Call", request_type: String, response_type: String)
call = ConnectRPC::Instrumentation::Call.new(role: :server, protocol: :grpc_web, method: method,
                                             server_address: nil, server_port: nil, carrier: {})
observer = subscriber.start(call)
observer.inspect
ConnectRPC::Sentry::VERSION.size
