# frozen_string_literal: true

require "rbconfig"
require_relative "../../../scripts/dependency_floors"

root = File.expand_path("../../..", __dir__)
package = File.join(root, "gems/connectrpc-health")
environment = {
  "RUBOCOP_CACHE_ROOT" => File.join(root, "vendor/health/rubocop"),
  "BUNDLE_GEMFILE" => DependencyFloors.gemfile(File.join(package, "Gemfile")),
  "BUNDLE_LOCKFILE" => nil, "BUNDLE_IGNORE_CONFIG" => "true",
  "BUNDLE_COOLDOWN" => "3", "BUNDLE_VERSION" => "4.0.23",
  "BUNDLE_PATH" => File.join(root, "vendor/health/bundle"), "BUNDLE_DISABLE_SHARED_GEMS" => "true",
  "BUNDLE_USER_HOME" => File.join(root, "vendor/health/bundler")
}
abort "Preinstall Bundler 4.0.23" if Gem::Specification.find_all_by_name("bundler", "= 4.0.23").empty?
command = ARGV.shift
arguments = case command
            when "setup" then ["install"]
            when "floors" then ["exec", "ruby", File.join(root, "scripts/dependency_floors.rb")]
            when "test" then ["exec", "rspec", *ARGV]
            when "check" then ["exec", "ruby", "scripts/check.rb"]
            when "build", "package", "interop" then ["exec", "ruby", "scripts/#{command}.rb", *ARGV]
            else abort "Usage: run.rb setup|floors|test|check|build|package|interop"
            end
puts "Health package; Ruby #{RUBY_VERSION}; Bundler 4.0.23; Gemfile #{environment.fetch('BUNDLE_GEMFILE')}"
$stdout.flush
exec(environment, RbConfig.ruby, "-S", "bundle", "_4.0.23_", *arguments, chdir: package)
