# frozen_string_literal: true

libraries = %w[connectrpc/rack connectrpc/testing connectrpc/rails connectrpc/async connectrpc/falcon connectrpc/otel]
libraries.reverse! if ARGV == ["reverse"]
libraries.each { |name| require name }
require "action_controller"
require "async/service/configuration"
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
APIFalconConfiguration.new.configure(Async::Service::Loader.new(configuration))
evaluator = configuration.environments.fetch(0).evaluator
raise "Falcon DSL values differ" unless evaluator.count == 2 && evaluator.url == "http://127.0.0.1:9292"
raise "Falcon DSL server differs" unless evaluator.connectrpc_server.is_a?(ConnectRPC::Server)
raise "Falcon DSL fallback differs" unless evaluator.rack_app.call("PATH_INFO" => "/ordinary").first == 200
raise "Falcon DSL options differ" unless evaluator.server_options == { idle_timeout: nil, max_connections: 8 }

evaluator.prepare!(Object.new)

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

method = ConnectRPC::Method.new(service: "installed.Echo", name: "Call", request_type: String, response_type: String)
service = ConnectRPC::Service.new(name: method.service, methods: [method])
server = ConnectRPC::Server.new(services: [service], binding: InstalledBinding.new,
                                handlers: { method.path => ->(message, _context) { message } })
transport = ConnectRPC::Testing::Transport.new(server: server)
client = ConnectRPC::Client.new(base_url: "https://in-process.invalid", binding: InstalledBinding.new,
                                transport: transport)
raise "Installed round trip failed" unless client.call(method, "installed").message == "installed"

status, = ConnectRPC::Rack.new(server: server).call("PATH_INFO" => "/other")
raise "Rack fallthrough failed" unless status == 404

endpoint = ConnectRPC::Falcon::Endpoint.parse("https://127.0.0.1:9292")
raise "Falcon endpoint failed" unless endpoint.scheme == "https"

Async do
  application = ConnectRPC::Async::Application.new(server: server)
  session = ConnectRPC::Async::ServerSession.new(application: application)
  session.start
  session.close(grace: 0)
  tls = ConnectRPC::Async::TLSConfiguration.new
  raise "Unexpected TLS generation" unless tls.start.generation.nil?

  tls.close
end.wait
subscriber = ConnectRPC::OTel::Subscriber.new
ConnectRPC::Instrumentation.new(subscribers: [subscriber])
raise "SDK loaded late" if defined?(OpenTelemetry::SDK)

puts "Installed optional runtime composition #{ARGV.first}: Rails subclass, Rack/Testing round trip, " \
     "native application/session/TLS, Falcon endpoint/real service DSL, named Rails callbacks, API-only OTel"
