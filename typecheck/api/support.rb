# frozen_string_literal: true

class APIBinding
  def encode(message, type:, codec:)
    [message, type, codec].inspect
  end

  def decode(bytes, type:, codec:)
    [bytes, type, codec]
  end
end

class APISubscriber
  def start(call)
    call.carrier["traceparent"] = "application-carrier"
    self
  end

  def finished(completion)
    completion.outcome&.duration&.round
    completion.processing_state.to_s
    nil
  end
end

class APIInvoker
  def call(method, request, context)
    context.response_metadata.add("rpc", method.name)
    request
  end
end

class APIProvider
  def call
    { version: "v1", certificate_chain: ["application-leaf-pem"], private_key: "application-key-pem", client_ca: nil }
  end
end

class APIConnectionObserver
  def call(event)
    event.state.to_s
    event.outcome&.to_s
    nil
  end
end

class APIReporter
  def call(event)
    event[:status][:generation]&.succ
    event[:category].to_s
    nil
  end
end

class APIApp
  def call(env)
    [200, { "content-type" => "text/plain" }, [env.fetch("PATH_INFO").to_s]]
  end
end

class APIHeaders
  def header
    entries = [["content-type", "application/proto"]] #: Array[[String, String]]
    entries.each
  end
end

class APIRequest
  def path = "/demo.Service/Call"
  def method = "POST"
  def headers = APIHeaders.new
  def body = nil
end

class APIController < ActionController::API
  before_action :observe_connect
  include ConnectRPC::OnRails::Controller

  def inspect_context
    connect_context&.response_metadata&.add("callback", "yes")
    connect_method&.path
  end

  private

  def observe_connect
    connect_context&.check!
  end

  def connect_service = ConnectRPC::Service.new(name: "demo.Service", rpcs: [])
  def connect_binding = APIBinding.new
  def connect_handlers = {}
  def connect_limits = { request_limit: 1024, response_wire_limit: 2048, encoding: :gzip }
  def connect_instrumentation = ConnectRPC::Instrumentation.new(subscribers: [])
  def connect_interceptors = [APIServerInterceptor.new, APIServerStreamInterceptor.new]
end

class APIEnvironment
  include ConnectRPC::Falcon::Environment

  def connectrpc_server = ConnectRPC::Server.new(services: [], binding: APIBinding.new)
  def tls_provider = APIProvider.new
  def server_options = { idle_timeout: nil, on_tls_error: APIReporter.new }
end

class APIPropagator
  def extract(carrier, context:, getter:)
    getter.keys(carrier)
    getter.get(carrier, "traceparent")
    context
  end

  def inject(carrier, context:)
    carrier["traceparent"] = context.to_s
    nil
  end
end

class APITracerProvider
  def tracer(name, version, attributes:)
    [name, version, attributes].inspect
    APITracer.new
  end
end

class APITracer
  def start_span(name, kind:, with_parent:, attributes:)
    [name, kind, with_parent, attributes].inspect
    APISpan.new
  end
end

class APISpan
  def context = APISpanContext.new

  def set_attribute(key, value)
    [key, value].inspect
    nil
  end

  def status=(value)
    value.to_s
    nil
  end

  def finish = nil
end

class APIClock
  def call = 0.5
end

class APIHosting
  def exercise(session, socket)
    accepted = session.accept(socket)
    if accepted
      accepted.timeout = 10
      accepted.remote_address.ip_port
      accepted.serve
      accepted.close
      accepted.wait_closed
    end
    session.schedule(&:wait)
    nil
  end
end

class APIBaseController < ActionController::Base
  include ConnectRPC::OnRails::Controller

  before_action :observe_connect

  private

  def observe_connect
    connect_context&.check!
  end
end

class APISpanContext
  def valid? = true
  def trace_id = "t" * 16
  def span_id = "s" * 8
  def trace_flags = APITraceFlags.new
  def tracestate = "vendor=value"
end

class APITraceFlags
  def sampled? = true
end

class APICallableBody
  def call(stream)
    stream.read
    stream.read(0, +"")
    stream << "hello"
    stream.write("world")
    stream.flush
    stream.close_read
    stream.close_write
    stream.closed?
    stream.close
  end
end

class APIHeaderReader
  def self.names(headers)
    headers.header.map { |key, value| "#{key}:#{value}" }
  end
end

# A custom typed SPI with opaque String binding tokens. This fixture's finite
# response is protocol evidence; installed network evidence uses the native SPI.
class APIStreamExchange
  def initialize
    payload = "typed"
    terminal = "{}"
    @input = StringIO.new([0, payload.bytesize].pack("CN") + payload + [2, terminal.bytesize].pack("CN") + terminal)
  end

  def response_head(timeout: nil)
    raise ArgumentError if timeout&.negative?

    ConnectRPC::StreamHead.new(status: 200, headers: { "content-type" => ["application/connect+proto"] })
  end

  def read(length, timeout: nil)
    raise ArgumentError if timeout&.negative?

    @input.read(length)
  end

  def write(_bytes) = nil
  def close_send = nil

  def finish(timeout: nil)
    raise ArgumentError if timeout&.negative?

    nil
  end

  def abort = nil
end

class APIStreamingTransport
  def streaming_capabilities = %i[server_stream client_stream bidi_stream]

  def open_stream(_request)
    yield APIStreamExchange.new
  end
end

class APIWebOutput
  def write(_bytes, timeout: nil) # rubocop:disable Lint/UnusedMethodArgument -- structural consumer
    nil
  end

  def finish(terminal:) # rubocop:disable Lint/UnusedMethodArgument -- structural consumer
    nil
  end

  def abort = nil
end

class APIStreamPublisher
  def call(_head) = nil
end

# Structural metrics recorder and prometheus-client registry capabilities.
class APIRecorder
  attr_reader :calls

  def initialize = @calls = []
  def increment(key, labels, by) = @calls << [key, labels, by]
  def observe(key, labels, value) = @calls << [key, labels, value]
  def set(key, labels, value) = @calls << [key, labels, value]
end

class APIMetric
  def increment(by:, labels:) = [by, labels].freeze && nil
  def observe(value, labels:) = [value, labels].freeze && nil
  def set(value, labels:) = [value, labels].freeze && nil
end

# Records the registration shape and returns one structural metric.
class APIRegistry
  def counter(name, docstring:, labels:) = register(name, docstring, labels)
  def histogram(name, docstring:, labels:, buckets:) = register(name, docstring, labels, buckets)
  def gauge(name, docstring:, labels:, store_settings:) = register(name, docstring, labels, store_settings)

  private

  def register(*shape)
    shape.freeze
    APIMetric.new
  end
end

class APIConnectionEvent
  def type = :state_changed
  def state = :ready
  def previous_state = :connecting
  def outcome = nil
  def duration = nil
  def active = 1
  def draining = 0
  def pending_calls = 0
end

class APIMessageObserver
  def message_received(bytes)
    bytes.inspect
    nil
  end

  def message_sent(bytes)
    bytes.inspect
    nil
  end
end

class BadAPIRecorder
  def increment(key) = key.to_proc && nil
end

class APIClientInterceptor
  def client_unary(call)
    call.request_metadata["x-rpc"] = "#{call.protocol}:#{call.method.path}"
    call.request = call.request.dup
    call.cancellation.check!
    call.deadline&.remaining
    response = yield
    response.trailers["x-tail"]&.size
    response
  end
end

class APICachingInterceptor
  def client_unary(call)
    return yield unless call.request_metadata["x-cache"]

    ConnectRPC::Response.new(message: "cached", metadata: ConnectRPC::Metadata.new,
                             trailers: ConnectRPC::Metadata.new)
  end
end

class APIClientStreamInterceptor
  def client_stream(call)
    call.request_metadata["x-shape"] = call.method.cardinality.to_s
    case call
    when ConnectRPC::ClientStreamCall::ServerStreaming then call.request = call.request.dup
    end
    call.wrap_stream { |stream| stream }
    call.cancellation.check!
    call.deadline&.remaining
    yield
  end
end

class APIServerStreamInterceptor
  def server_stream(call)
    case call
    when ConnectRPC::ServerStreamCall::ServerStreaming then call.request = call.request.dup
    end
    call.context.response_trailers.add("x-shape", "#{call.protocol}:#{call.method.cardinality}")
    call.wrap_stream { |stream| stream }
    yield
  end
end

class APIServerInterceptor
  def server_unary(call)
    context = call.context
    context.request_metadata["x-principal"] = "#{call.protocol}:#{call.method.name}"
    call.request = call.request.dup
    response = yield
    context.response_trailers.add("x-done", "yes")
    response
  end
end
