# frozen_string_literal: true

require "rails"
require "action_controller/railtie"
require "active_record"
require "sentry-rails"
require "sentry/rails/error_subscriber"
require "connectrpc-on-rails"

# sentry-rails copies Rails.logger into each SDK configuration and warns on stdout without one.
Rails.logger = Logger.new(File::NULL)

# One Rails application per process. sentry-rails activates tracing and the
# controller extensions in after_initialize, so the SDK is initialized first;
# examples then reinitialize it per example as usual.
module SentryRailsApplication
  ROOT = Dir.mktmpdir("connectrpc-sentry-rails-")

  class Application < ::Rails::Application
    config.load_defaults ::Rails::VERSION::STRING.split(".").first(2).join(".")
    config.root = ROOT
    config.secret_key_base = "integration-only-not-a-secret-" * 8
    config.eager_load = false
    config.logger = Logger.new(File::NULL)
    config.hosts.clear
    config.action_dispatch.show_exceptions = :none
    # The Active Record railtie is not loaded; keep its standard rescue response.
    config.action_dispatch.rescue_responses["ActiveRecord::RecordNotFound"] = :not_found
  end

  def self.boot!(helpers)
    helpers.sentry
    ActiveRecord::Base.establish_connection(adapter: "sqlite3", database: ":memory:")
    ActiveRecord::Base.connection.create_table(:notes) { |table| table.string :name }
    Application.initialize!
    Application.routes.draw do
      match "/example.v1.EchoService(/*rpc)", to: "sentry_rails_application/echo#connect", via: :all, format: false
    end
    Application
  end

  class Note < ActiveRecord::Base; end

  class EchoController < ActionController::API
    include ConnectRPC::OnRails::Controller

    class_attribute :rpc_instrumentation

    private

    def connect_service = Example::V1::EchoService::SERVICE
    def connect_binding = ConnectRPC::Bindings::GoogleProtobuf.new
    def connect_instrumentation = rpc_instrumentation

    def connect_handlers
      Example::V1::EchoService.handlers(echo: method(:echo_handler), fail: method(:echo_handler))
    end

    def echo_handler(message, _context)
      Note.create!(name: message.name)
      case message.name
      when "bug" then raise "secret handler bug"
      when "missing" then Note.find(0)
      when "deliberate" then raise ConnectRPC::Error.new(:invalid_argument, "deliberate")
      end
      message
    end
  end
end
