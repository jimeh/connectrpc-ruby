# frozen_string_literal: true

require "rbconfig"

root = File.expand_path("../../..", __dir__)
middleware = File.join(root, "gems/connectrpc-rack")
environment = {
  "RUBOCOP_CACHE_ROOT" => File.join(root, "vendor/rack/rubocop"),
  "BUNDLE_GEMFILE" => File.join(middleware, "Gemfile"), "BUNDLE_IGNORE_CONFIG" => "true",
  "BUNDLE_FROZEN" => "true", "BUNDLE_COOLDOWN" => "7", "BUNDLE_VERSION" => "4.0.23",
  "BUNDLE_PATH" => File.join(root, "vendor/rack/bundle"), "BUNDLE_DISABLE_SHARED_GEMS" => "true",
  "BUNDLE_USER_HOME" => File.join(root, "vendor/rack/bundler")
}
command = ARGV.shift
arguments = case command
            when "setup" then ["install"]
            when "test" then ["exec", "rspec", *ARGV]
            when "check" then ["exec", "rubocop", ".", "--config", File.join(root, ".rubocop.yml")]
            when "build", "package" then ["exec", "ruby", "scripts/#{command}.rb", *ARGV]
            else abort "Usage: run.rb setup|test|check|build|package"
            end
puts "Rack middleware; Ruby #{RUBY_VERSION}; Bundler 4.0.23; Gemfile #{environment.fetch('BUNDLE_GEMFILE')}"
$stdout.flush
exec(environment, RbConfig.ruby, "-S", "bundle", "_4.0.23_", *arguments, chdir: middleware)
