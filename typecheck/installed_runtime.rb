# frozen_string_literal: true

libraries = %w[connectrpc/rack connectrpc/testing connectrpc/rails connectrpc/async connectrpc/falcon connectrpc/otel]
libraries.reverse! if ARGV == ["reverse"]
libraries.each { |name| require name }
require "action_controller"
require "async/service/configuration"
require "protocol/http/body/stream"
require "stringio"
require_relative "api/support"

raise "SDK loaded" if defined?(OpenTelemetry::SDK) || Gem.loaded_specs.key?("opentelemetry-sdk")

Gem.loaded_specs.each_value do |spec|
  next if spec.default_gem?

  raise "Library escaped GEM_HOME: #{spec.name}" unless spec.full_gem_path.start_with?("#{ENV.fetch('GEM_HOME')}/")
end

class InstalledController < ActionController::API
  include ConnectRPC::Rails::Controller
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
unless evaluator.server_options == { idle_timeout: nil, max_connections: 8, cache: true }
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
service = ConnectRPC::Service.new(name: method.service, methods: [method])
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
ensure
  release&.enqueue(true)
  native_transport&.close(grace: 0)
  host&.close(grace: 0)
  raise "Installed Host wait after close" unless host&.wait.nil?
end.wait
puts "Installed NetHTTP and native HTTP/2 cancellation: real request progress, " \
     "foreign-thread source, socket/reset cleanup and pool reuse"

records = [ConnectRPC::Response.new, ConnectRPC::Response.new(message: "reply", metadata: ConnectRPC::Metadata.new,
                                                              trailers: ConnectRPC::Metadata.new),
           ConnectRPC::TransportRequest.new, ConnectRPC::TransportResponse.new]
records.each do |record|
  raise "Struct pair shape differs" unless record.each_pair.all? { |pair| pair.is_a?(Array) && pair[0].is_a?(Symbol) }
end
carrier = {}
call = ConnectRPC::Instrumentation::Call.new(role: :client, protocol: :connect, method: nil,
                                             server_address: nil, server_port: nil, carrier: carrier)
custom = ConnectRPC::OTel::Subscriber.new(tracer_provider: APITracerProvider.new)
observer = custom.start(call)
expected_parent = "00-#{'74' * 16}-#{'73' * 8}-01"
raise "Default propagation missed custom span context" unless carrier["traceparent"] == expected_parent &&
                                                              carrier["tracestate"] == "vendor=value"

observer.finished(ConnectRPC::Instrumentation::Completion.new(outcome: nil, processing_state: :not_started,
                                                              decision: :processed, delivery: :unobserved,
                                                              error: nil, http_status: nil, duration: 0.0))
subscriber = ConnectRPC::OTel::Subscriber.new
ConnectRPC::Instrumentation.new(subscribers: [subscriber])
raise "SDK loaded late" if defined?(OpenTelemetry::SDK)

puts "Installed optional runtime composition #{ARGV.first}: Rails subclass, Rack/Testing round trip, " \
     "native headers/session/TLS, callable stream, Struct pairs, Falcon inline DSL, " \
     "Rails callbacks, API-only OTel/default propagation"

require_relative "installed_streaming"

require_relative "installed_web"
