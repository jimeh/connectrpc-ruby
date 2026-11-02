# frozen_string_literal: true

# Copied outside the checkout with generated fixtures by check_rails_package.rb.
require "connectrpc/rails"
abort "Eager Base/API load" unless ActionController.autoload?(:Base) && ActionController.autoload?(:API)
require "rails"
require "action_controller/railtie"
require "rack/mock"
require "rack/lint"
require "connectrpc/bindings/google_protobuf"
$LOAD_PATH.unshift(File.expand_path("generated", __dir__))
require "echo_connect"

abort "Wrong Rails version" unless Rails.version == ENV.fetch("CONNECTRPC_RAILS_VERSION")
module InstalledIntegration
  class InstalledRailsApp < Rails::Application
    config.load_defaults Rails::VERSION::STRING.split(".").first(2).join(".")
    config.root = File.expand_path("app", __dir__)
    config.secret_key_base = "isolated-integration-only-" * 8
    config.eager_load = true
    config.hosts = ["www.example.com"]
    config.logger = Logger.new(File::NULL)
  end

  class InstalledEchoController < ActionController::API
    include ConnectRPC::Rails::Controller

    before_action { connect_context.response_metadata.add("x-callback", "native") }

    private

    def connect_service = Example::V1::EchoService::SERVICE
    def connect_limits = { encoding: :gzip }
    def connect_binding = ConnectRPC::Bindings::GoogleProtobuf.new
    def connect_handlers = Example::V1::EchoService.handlers(echo: method(:rpc_echo), fail: method(:rpc_fail))

    def rpc_echo(message, context)
      context.response_trailers.add("x-installed", "yes")
      message
    end

    def rpc_fail(message, _context)
      detail = ConnectRPC::ErrorDetail.new(type: "example.v1.EchoMessage", value: message.to_proto)
      raise ConnectRPC::Error.new(
        :invalid_argument, "safe", details: [detail], trailers: { "x-error" => ["installed"] }
      )
    end
  end
end
app = InstalledIntegration::InstalledRailsApp.initialize!
app.routes.draw do
  match "/rpc/example.v1.EchoService(/*rpc)", to: "installed_integration/installed_echo#connect",
                                              via: :all, format: false
end
binding = ConnectRPC::Bindings::GoogleProtobuf.new
transport = lambda do |request|
  env = Rack::MockRequest.env_for(request.url.to_s, method: request.http_method, input: request.body)
  env["HTTP_HOST"] = request.url.host
  request.headers.each do |key, values|
    env[key == "content-type" ? "CONTENT_TYPE" : "HTTP_#{key.upcase.tr('-', '_')}"] = values.join(", ")
  end
  status, headers, body = Rack::Lint.new(app).call(env)
  bytes = +""
  body.each { |chunk| bytes << chunk }
  body.close
  expected = request.headers.fetch("content-type").first
  abort "Changed content type" unless headers["content-type"] == (status == 200 ? expected : "application/json")
  if request.headers["content-encoding"] == ["gzip"]
    abort "Installed request not gzip" unless request.body.b.start_with?("\x1f\x8b".b)
    if status == 200
      abort "Installed Rails response not gzip" unless headers["content-encoding"] == ["gzip"] &&
                                                       bytes.b.start_with?("\x1f\x8b".b)
    elsif headers.key?("content-encoding")
      abort "Installed Rails error must be identity"
    end
  end
  ConnectRPC::TransportResponse.new(status: status, headers: headers, body: bytes)
end
%i[proto json].product(%i[identity gzip]).each do |codec, encoding|
  client = Example::V1::EchoService::Client.new(client: ConnectRPC::Client.new(
    base_url: "http://www.example.com/rpc", binding: binding, codec: codec, transport: transport, encoding: encoding
  ))
  response = client.echo(Example::V1::EchoMessage.new(name: "installed"))
  unless response.message.name == "installed" && response.metadata["x-callback"] == ["native"]
    abort "Typed result/callback failed"
  end
  abort "Trailing metadata failed" unless response.trailers["x-installed"] == ["yes"]
  begin
    client.fail(Example::V1::EchoMessage.new(name: "detail"))
    abort "Expected error missing"
  rescue ConnectRPC::Error => e
    unless e.code == "invalid_argument" && e.message == "safe" && e.trailers["x-error"] == ["installed"]
      abort "Structured error failed"
    end
    detail = e.details.first.decode(binding: binding, type: Example::V1::EchoMessage)
    abort "Typed detail failed" unless detail.name == "detail"
  end
  puts "Installed Rails #{Rails.version} Ruby #{RUBY_VERSION}: #{codec}/#{encoding} typed success/error/detail passed"
  puts "Installed metadata/trailers + Rack::Lint passed"
end
%w[connectrpc connectrpc-rails actionpack railties rack google-protobuf].each do |name|
  spec = Gem.loaded_specs.fetch(name)
  abort "#{name} loaded outside isolated GEM_HOME" unless spec.full_gem_path.start_with?("#{ENV.fetch('GEM_HOME')}/")
end
source = ConnectRPC::Rails::Controller.instance_method(:connect).source_location.first
unless source.start_with?(Gem.loaded_specs.fetch("connectrpc-rails").full_gem_path)
  abort "Adapter source escaped installed archive"
end
