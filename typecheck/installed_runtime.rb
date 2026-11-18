# frozen_string_literal: true

libraries = %w[
  connectrpc/rack connectrpc/testing connectrpc/on_rails connectrpc/async connectrpc/falcon connectrpc/otel
  connectrpc/prometheus connectrpc/yabeda connectrpc/health connectrpc/sentry connectrpc/hyper
]
libraries.reverse! if ARGV == ["reverse"]
libraries.each { |name| require name }
require "action_controller"
require "async/service/configuration"
require "protocol/http/body/stream"
require "stringio"
require_relative "api/support"

versions = {
  "connectrpc" => ConnectRPC::VERSION, "connectrpc-rack" => ConnectRPC::Rack::VERSION,
  "connectrpc-on-rails" => ConnectRPC::OnRails::VERSION, "connectrpc-async" => ConnectRPC::Async::VERSION,
  "connectrpc-falcon" => ConnectRPC::Falcon::VERSION, "connectrpc-testing" => ConnectRPC::Testing::VERSION,
  "connectrpc-otel" => ConnectRPC::OTel::VERSION, "connectrpc-prometheus" => ConnectRPC::Prometheus::VERSION,
  "connectrpc-yabeda" => ConnectRPC::Yabeda::VERSION, "connectrpc-health" => ConnectRPC::Health::VERSION,
  "connectrpc-sentry" => ConnectRPC::Sentry::VERSION, "connectrpc-hyper" => ConnectRPC::Hyper::VERSION
}
versions.each do |name, version|
  raise "Installed runtime version differs: #{name}" unless version == Gem.loaded_specs.fetch(name).version.to_s
end
raise "Installed release train differs" unless versions.values.uniq == [ConnectRPC::VERSION]

raise "SDK loaded" if defined?(OpenTelemetry::SDK) || Gem.loaded_specs.key?("opentelemetry-sdk")

Gem.loaded_specs.each_value do |spec|
  next if spec.default_gem?

  raise "Library escaped GEM_HOME: #{spec.name}" unless spec.full_gem_path.start_with?("#{ENV.fetch('GEM_HOME')}/")
end

class InstalledController < ActionController::API
  include ConnectRPC::OnRails::Controller
end
raise "Only connect is a controller action" unless InstalledController.action_methods.include?("connect") &&
                                                   !InstalledController.action_methods.include?("connect_service")

[APIController, APIBaseController].each do |controller|
  unless controller._process_action_callbacks.any? { |callback| callback.filter == :observe_connect }
    raise "Named Rails callback was not registered"
  end
  raise "Private callback became an action" if controller.action_methods.include?("observe_connect")
end
configuration = Async::Service::Configuration.new
Async::Service::Loader.load_file(configuration, File.join(__dir__, "api/falcon_dsl.rb"))
Async::Service::Loader.load_file(configuration, File.join(__dir__, "api/falcon_inline.rb"))
APIFalconConfiguration.new.configure(Async::Service::Loader.new(configuration))
evaluator = configuration.environments.fetch(0).evaluator
raise "Falcon DSL values differ" unless evaluator.count == 2 && evaluator.url == "http://127.0.0.1:9292"
raise "Falcon DSL server differs" unless evaluator.connectrpc_server.is_a?(ConnectRPC::Server)
raise "Falcon DSL fallback differs" unless evaluator.rack_app.call("PATH_INFO" => "/ordinary").first == 200
unless evaluator.server_options == { idle_timeout: nil, max_connections: 8, cache: true, keepalive: nil,
                                     stream_receive_window: 1_048_576, dynamic_receive_windows: true,
                                     max_receive_window: 4_194_304 }
  raise "Falcon DSL options differ"
end

evaluator.prepare!(Object.new)
APIInlineFalconConfiguration.new.configure(Async::Service::Loader.new(configuration))
configuration.environments.drop(1).each do |environment|
  inline = environment.evaluator
  raise "Inline DSL fallback failed" unless inline.rack_app.call("PATH_INFO" => "/inline") == [200, {}, ["/inline"]]
  raise "Inline DSL provider failed" unless inline.tls_provider.call[:version] == "v1"
end
native_headers = Protocol::HTTP::Headers[[%w[x-native value]]]
raise "Native headers are not enumerable" unless native_headers.header.is_a?(Enumerator)
raise "Native header caller failed" unless APIHeaderReader.names(native_headers) == ["x-native:value"]

class InstalledStreamOutput < StringIO
  def close_write(_error = nil) = super()
end
output = InstalledStreamOutput.new
stream = Protocol::HTTP::Body::Stream.new(Protocol::HTTP::Body::Buffered.new(["input"]), output)
APICallableBody.new.call(stream)
raise "Ordinary callable stream failed" unless output.string == "helloworld" && stream.closed?

class InstalledBinding
  def encode(message, type: nil, codec: nil)
    raise "Unexpected codec" unless %i[proto json].include?(codec)
    raise "Unexpected token" unless type == String

    message
  end

  def decode(bytes, type: nil, codec: nil)
    raise "Unexpected codec" unless %i[proto json].include?(codec)
    raise "Unexpected token" unless type == String

    bytes
  end
end

method = ConnectRPC::Method.new(service: "installed.Echo", name: "Call", request_type: String, response_type: String,
                                idempotency: :no_side_effects)
service = ConnectRPC::Service.new(name: method.service, rpcs: [method])
server = ConnectRPC::Server.new(services: [service], binding: InstalledBinding.new,
                                handlers: { method.path => ->(message, _context) { message } })
transport = ConnectRPC::Testing::Transport.new(server: server)
client = ConnectRPC::Client.new(base_url: "https://in-process.invalid", binding: InstalledBinding.new,
                                transport: transport, use_get: true, get_url_limit: 8192)
raise "Installed round trip failed" unless client.call(method, "installed").message == "installed"

status, = ConnectRPC::Rack.new(server: server).call("PATH_INFO" => "/other")
raise "Rack fallthrough failed" unless status == 404

endpoint = ConnectRPC::Falcon::Endpoint.parse("https://127.0.0.1:9292")
raise "Falcon endpoint failed" unless endpoint.scheme == "https"

Async do
  bound = ConnectRPC::Falcon::Endpoint.parse("http://127.0.0.1:0").bound
  falcon = ConnectRPC::Falcon::Server.new(server: server, endpoint: bound, cache: true)
  falcon.run
  falcon.close(grace: 0)
  bound.close
  application = ConnectRPC::Async::Application.new(server: server)
  session = ConnectRPC::Async::ServerSession.new(application: application)
  session.start
  session.close(grace: 0)
  tls = ConnectRPC::Async::TLSConfiguration.new
  raise "Unexpected TLS generation" unless tls.start.generation.nil?

  tls.close
end.wait
# Installed callers use actual owned network APIs outside every checkout load path.
require "connectrpc/transports/net_http"
listener = TCPServer.new("127.0.0.1", 0)
progress = Queue.new
peer = Thread.new do
  socket = listener.accept
  headers = socket.gets("\r\n\r\n")
  socket.read(headers[/Content-Length: ([0-9]+)/i, 1].to_i)
  progress << true
  raise "Installed NetHTTP socket not closed" unless socket.read == ""
ensure
  socket&.close
end
source = ConnectRPC::CancellationSource.new
network_client = ConnectRPC::Client.new(base_url: "http://127.0.0.1:#{listener.addr[1]}", binding: InstalledBinding.new)
caller = Thread.new do
  network_client.call(method, "blocked", timeout: nil, cancellation: source.token)
rescue ConnectRPC::Error => e
  e
end
raise "Installed NetHTTP did not write request" unless progress.pop(timeout: 2)

source.cancel
raise "Installed NetHTTP did not finish" unless caller.join(2)
unless caller.value.is_a?(ConnectRPC::Error) && caller.value.code == "canceled" && caller.value.cause.nil?
  raise "Installed NetHTTP result"
end
raise "Installed NetHTTP peer not retired" unless peer.join(2)

peer.value
listener.close

Async do |task| # rubocop:disable Metrics/BlockLength
  entered = Async::Queue.new
  release = Async::Queue.new
  context = nil
  native_server = ConnectRPC::Server.new(services: [service], binding: InstalledBinding.new,
                                         handlers: { method.path => lambda { |message, current|
                                           if message == "blocked"
                                             context = current
                                             entered.enqueue(true)
                                             release.dequeue
                                             current.check!
                                           end
                                           message
                                         } })
  host = ConnectRPC::Async::Host.new(server: native_server).start
  origin = "http://127.0.0.1:#{host.port}"
  native_transport = ConnectRPC::Async::Transport.new(origin: origin)
  native_client = ConnectRPC::Client.new(base_url: origin, binding: InstalledBinding.new, transport: native_transport)
  source = ConnectRPC::CancellationSource.new
  pending = task.async(finished: false) do
    native_client.call(method, "blocked", timeout: nil, cancellation: source.token)
  end
  entered.dequeue
  Thread.new { source.cancel }.value
  begin
    pending.wait
    raise "Installed native cancellation missing"
  rescue ConnectRPC::Error => e
    raise "Installed native cancellation result" unless e.code == "canceled" && e.cause.nil?
  end
  task.with_timeout(2) { task.yield until context.cancelled? }
  raise "Installed native Context reason" unless context.cancellation_reason == :canceled

  release.enqueue(true)
  raise "Installed native pool reuse" unless native_client.call(method, "healthy").message == "healthy"

  # The compiled hyper extension blocks its calling thread, so it calls the
  # native host from another thread while this reactor joins it and serves.
  hyper = ConnectRPC::Hyper::Transport.new
  hyper_client = ConnectRPC::Client.new(base_url: origin, binding: InstalledBinding.new, transport: hyper)
  replies = Thread.new { Array.new(2) { hyper_client.call(method, "hyper").message } }.value
  raise "Installed hyper h2c calls failed" unless replies == %w[hyper hyper]
  raise "Installed hyper close did not drain" unless Thread.new { hyper.close(grace: 2) }.value
ensure
  release&.enqueue(true)
  hyper&.close(grace: 0)
  native_transport&.close(grace: 0)
  host&.close(grace: 0)
  raise "Installed Host wait after close" unless host&.wait.nil?
end.wait
puts "Installed NetHTTP and native HTTP/2 cancellation: real request progress, " \
     "foreign-thread source, socket/reset cleanup and pool reuse; hyper h2c calls from a thread"

records = [ConnectRPC::Response.new, ConnectRPC::Response.new(message: "reply", metadata: ConnectRPC::Metadata.new,
                                                              trailers: ConnectRPC::Metadata.new),
           ConnectRPC::TransportRequest.new, ConnectRPC::TransportResponse.new]
records.each do |record|
  raise "Struct pair shape differs" unless record.each_pair.all? { |pair| pair.is_a?(Array) && pair[0].is_a?(Symbol) }
end
carrier = {}
call = ConnectRPC::Instrumentation::Call.new(role: :client, protocol: :connect, method: nil,
                                             server_address: nil, server_port: nil, carrier: carrier)
custom = ConnectRPC::OTel::Subscriber.new(
  tracer_provider: APITracerProvider.new,
  propagator: OpenTelemetry::Trace::Propagation::TraceContext.text_map_propagator
)
observer = custom.start(call)
expected_parent = "00-#{'74' * 16}-#{'73' * 8}-01"
raise "Explicit propagation missed custom span context" unless carrier["traceparent"] == expected_parent &&
                                                               carrier["tracestate"] == "vendor=value"

unobserved = ConnectRPC::Instrumentation::Completion.new(outcome: nil, processing_state: :not_started,
                                                         decision: :processed, delivery: :unobserved,
                                                         error: nil, http_status: nil, duration: 0.0)
observer.finished(unobserved)
# The default uses the global propagator, which is no-op without an SDK.
global_carrier = {}
ConnectRPC::OTel::Subscriber.new(tracer_provider: APITracerProvider.new)
                            .start(call.with(carrier: global_carrier)).finished(unobserved)
raise "No-op global propagation injected" unless global_carrier.empty?

subscriber = ConnectRPC::OTel::Subscriber.new
ConnectRPC::Instrumentation.new(subscribers: [subscriber])
raise "SDK loaded late" if defined?(OpenTelemetry::SDK)

# Both metrics sinks compose the same core layer in one installed process.
registry = Prometheus::Client::Registry.new
sinks = [ConnectRPC::Prometheus::Metrics.new(registry: registry), ConnectRPC::Yabeda::Metrics.new]
Yabeda.configure!
completion = ConnectRPC::Instrumentation::Completion.new(outcome: nil, processing_state: :not_started,
                                                         decision: :host_rejected, delivery: :completed,
                                                         error: nil, http_status: 501, duration: 0.0)
sinks.each { |sink| sink.subscriber.start(call).finished(completion) }
other = { grpc_type: "unknown", grpc_service: "_OTHER", grpc_method: "_OTHER", rpc_protocol: "connect" }
unless registry.get(:grpc_server_rejected_total).get(labels: other.merge(reason: "host_rejected")).zero? &&
       registry.get(:grpc_client_handled_total).get(labels: other.merge(grpc_code: "Unknown")).to_i == 1 &&
       Yabeda.grpc_client_started_total.get(other) == 1
  raise "Installed metrics sinks did not record"
end

# The health service answers the generated client through the installed Testing transport.
require "connectrpc/bindings/google_protobuf"
health = ConnectRPC::Health::Registry.new
health.set("installed.v1.Service", :not_serving)
health_server = ConnectRPC::Server.new(services: [ConnectRPC::Health::SERVICE], handlers: health.handlers,
                                       binding: ConnectRPC::Bindings::GoogleProtobuf.new)
health_client = Grpc::Health::V1::Health::Client.new(
  client: ConnectRPC::Client.new(base_url: "https://example.invalid",
                                 binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                                 transport: ConnectRPC::Testing::Transport.new(server: health_server))
)
health_request = Grpc::Health::V1::HealthCheckRequest.new(service: "installed.v1.Service")
raise "Installed health Check failed" unless health_client.check(health_request).message.status == :NOT_SERVING

# Sentry stays inert until the application initializes the SDK; requiring it configures nothing.
raise "Sentry initialized on require" if Sentry.initialized?
raise "Uninitialized Sentry observed an RPC" unless ConnectRPC::Sentry::Subscriber.new.start(call).nil?
raise "sentry-rails or sentry-opentelemetry auto-required" if defined?(Sentry::Rails) || defined?(Sentry::OpenTelemetry)

puts "Installed optional runtime composition #{ARGV.first}: Rails subclass, Rack/Testing round trip, " \
     "native headers/session/TLS, callable stream, Struct pairs, Falcon inline DSL, " \
     "Rails callbacks, API-only OTel/default propagation, Prometheus and Yabeda metrics, health Check, inert Sentry"

require_relative "installed_streaming"

require_relative "installed_web"
