# frozen_string_literal: true

require "rbconfig"
require_relative "../../../scripts/dependency_floors"

root = File.expand_path("../../..", __dir__)
middleware = File.join(root, "gems/connectrpc-rack")
environment = {
  "RUBOCOP_CACHE_ROOT" => File.join(root, "vendor/rack/rubocop"),
  "BUNDLE_GEMFILE" => DependencyFloors.gemfile(File.join(middleware, "Gemfile")),
  "BUNDLE_LOCKFILE" => nil, "BUNDLE_IGNORE_CONFIG" => "true",
  "BUNDLE_COOLDOWN" => "3", "BUNDLE_VERSION" => "4.0.23",
  "BUNDLE_PATH" => File.join(root, "vendor/rack/bundle"), "BUNDLE_DISABLE_SHARED_GEMS" => "true",
  "BUNDLE_USER_HOME" => File.join(root, "vendor/rack/bundler")
}
command = ARGV.shift
arguments = case command
            when "setup" then ["install"]
            when "floors" then ["exec", "ruby", File.join(root, "scripts/dependency_floors.rb")]
            when "exec" then ["exec", *ARGV]
            when "test" then ["exec", "rspec", *ARGV]
            when "check" then ["exec", "rubocop", ".", "--config", File.join(root, ".rubocop.yml")]
            when "build", "package" then ["exec", "ruby", "scripts/#{command}.rb", *ARGV]
            else abort "Usage: run.rb setup|floors|test|check|build|package|exec"
            end
unless command == "exec"
  puts "Rack middleware; Ruby #{RUBY_VERSION}; Bundler 4.0.23; Gemfile #{environment.fetch('BUNDLE_GEMFILE')}"
end
$stdout.flush
exec(environment, RbConfig.ruby, "-S", "bundle", "_4.0.23_", *arguments, chdir: middleware)
