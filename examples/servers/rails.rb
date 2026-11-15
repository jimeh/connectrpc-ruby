# frozen_string_literal: true

# Rails server: a single-file Rails application whose controller includes
# ConnectRPC::Rails::Controller. Rails serves Connect unary calls over HTTP/1.1
# and runs normal controller callbacks around each handler.
require "rails"
require "action_controller/railtie"
require "connectrpc/rails"
require_relative "../demo/launch"
require_relative "../demo/handlers"
require_relative "../localhost"

class DemoApplication < Rails::Application
  config.load_defaults Rails::VERSION::STRING.split(".").first(2).join(".")
  config.root = __dir__
  config.eager_load = false
  config.enable_reloading = false
  config.logger = Logger.new(File::NULL)
  config.secret_key_base = "runnable-example-only-not-a-secret-" * 2
end

class DemoController < ActionController::API
  include ConnectRPC::Rails::Controller

  private

  def connect_service = Demo::V1::DemoService::SERVICE
  def connect_binding = ConnectRPC::Bindings::GoogleProtobuf.new
  def connect_handlers = DemoExample::Handlers.new("rails").to_h
  def connect_limits = { encoding: :gzip }
  # Logs each RPC, like the other servers.
  def connect_instrumentation = DemoExample.instrumentation
end

DemoApplication.initialize!
DemoApplication.routes.draw do
  # format: false keeps the dots in the service name; via: :all lets Connect
  # answer unsupported HTTP methods itself.
  match "/demo.v1.DemoService(/*rpc)", to: "demo#connect", via: :all, format: false
end

DemoExample::Launch.serve("rails") do |launch|
  host = EchoExample::Localhost.new(DemoApplication, port: launch.port)
  launch.ready(host.url)
  launch.wait_for_shutdown
ensure
  host&.close
end
