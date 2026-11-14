# frozen_string_literal: true

require "rbconfig"

# The frozen release-job bundle (Sigstore signing). Its paths are passed to each
# child explicitly because Mise exports the root bundle settings.
module ReleaseTools
  ROOT = File.expand_path("..", __dir__)
  OUTPUT = File.join(ROOT, "vendor/sigstore")

  def self.environment
    {
      "BUNDLE_GEMFILE" => File.join(ROOT, "release/Gemfile"),
      "BUNDLE_PATH" => File.join(OUTPUT, "bundle"),
      "BUNDLE_USER_HOME" => File.join(OUTPUT, "bundler"),
      "BUNDLE_APP_CONFIG" => File.join(OUTPUT, "config"),
      "BUNDLE_CACHE_PATH" => File.join(OUTPUT, "cache"),
      "BUNDLE_FROZEN" => "true", "BUNDLE_IGNORE_CONFIG" => "true",
      "BUNDLE_DISABLE_SHARED_GEMS" => "true", "BUNDLE_COOLDOWN" => "7"
    }
  end

  def self.bundle(*arguments)
    [environment, RbConfig.ruby, "-S", "bundle", "_4.0.23_", *arguments]
  end
end
