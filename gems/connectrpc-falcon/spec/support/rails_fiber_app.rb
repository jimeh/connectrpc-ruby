# frozen_string_literal: true

# One Rails application per process, required only by the Rails fallback
# spec's before(:context), so other groups never load or boot it.
require "logger"
require "rails"
require "active_record/railtie"
require "action_controller/railtie"

module FalconRailsFixture
  class << self
    # Each request appends here: the app's own record, the CurrentAttributes
    # before_reset record and the executor completion record.
    attr_accessor :calls, :resets, :on_complete
  end
  self.calls = []
  self.resets = []

  class Current < ActiveSupport::CurrentAttributes
    attribute :value
    # Runs before the attributes reset, so it still sees the request's value.
    before_reset { FalconRailsFixture.resets << [Fiber.current, value] }
  end

  class ProbeController < ActionController::API
    def show
      Current.value = "request-#{params[:id]}"
      ActiveRecord::Base.connection.select_value("SELECT 1")
      FalconRailsFixture.calls << [Fiber.current, ActiveRecord::Base.connection_pool.query_cache_enabled]
      render plain: Current.value
    end
  end

  class Application < Rails::Application
    config.root = __dir__
    config.eager_load = false
    config.enable_reloading = false
    config.api_only = true
    config.logger = Logger.new(nil)
    config.secret_key_base = "falcon-rails-fixture-secret" * 4
    config.hosts.clear
    config.active_support.isolation_level = :fiber
  end

  # Boot and ActiveRecord's lazy load need some database configuration, with
  # no config/database.yml; the explicit connection below replaces it.
  previous_url = ENV.fetch("DATABASE_URL", nil)
  ENV["DATABASE_URL"] = "sqlite3::memory:"
  begin
    Application.initialize!
    # The reaper would return a dead fiber's connection and hide the leak.
    ActiveRecord::Base.establish_connection(adapter: "sqlite3", database: ":memory:", pool: 5,
                                            reaping_frequency: 0)
  ensure
    ENV["DATABASE_URL"] = previous_url
  end
  Application.routes.draw { get "/probe/:id", to: "falcon_rails_fixture/probe#show" }
  # Appended after boot, so it runs after the framework's completion hooks:
  # connection release, query-cache teardown and CurrentAttributes.clear_all.
  Application.executor.to_complete do
    pool = ActiveRecord::Base.connection_pool
    FalconRailsFixture.on_complete&.call([Fiber.current, pool.query_cache_enabled, Current.value])
  end
end
