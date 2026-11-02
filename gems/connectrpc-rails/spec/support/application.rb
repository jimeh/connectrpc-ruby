# frozen_string_literal: true

require "rails"
require "action_controller/railtie"
require "rack/mock"
require "rack/lint"
require "tmpdir"
require "fileutils"
require "connectrpc/bindings/google_protobuf"
require "google/protobuf/wrappers_pb"
require "google/protobuf/struct_pb"

fixture_root = File.join(ENV.fetch("CONNECTRPC_ROOT"), "spec/fixtures/generation/echo")
$LOAD_PATH.unshift(fixture_root)
require "echo_connect"

module RailsIntegration
  ROOT = Dir.mktmpdir("connectrpc-rails-app-", ENV.fetch("CONNECTRPC_RAILS_TMPDIR", nil))
  FileUtils.cp_r(Dir.glob(File.expand_path("app/*", __dir__)), ROOT)

  class Application < ::Rails::Application
    config.load_defaults ::Rails::VERSION::STRING.split(".").first(2).join(".")
    config.root = ROOT
    config.secret_key_base = "integration-only-not-a-secret-" * 8
    config.eager_load = false
    config.enable_reloading = true
    config.logger = Logger.new(File::NULL)
    config.hosts = ["www.example.com"]
    config.action_controller.allow_forgery_protection = true
    config.action_dispatch.show_exceptions = :none
    config.action_dispatch.cookies_same_site_protection = :lax
    config.session_store :cookie_store, key: "rpc_test"
  end

  APP = Application.initialize!
end
