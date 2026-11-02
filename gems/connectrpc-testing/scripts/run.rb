# frozen_string_literal: true

require "rbconfig"

root = File.expand_path("../../..", __dir__)
package = File.join(root, "gems/connectrpc-testing")
environment = {
  "RUBOCOP_CACHE_ROOT" => File.join(root, "vendor/testing/rubocop"),
  "BUNDLE_GEMFILE" => File.join(package, "Gemfile"), "BUNDLE_IGNORE_CONFIG" => "true",
  "BUNDLE_FROZEN" => "true", "BUNDLE_COOLDOWN" => "7", "BUNDLE_VERSION" => "4.0.23",
  "BUNDLE_PATH" => File.join(root, "vendor/testing/bundle"), "BUNDLE_DISABLE_SHARED_GEMS" => "true",
  "BUNDLE_USER_HOME" => File.join(root, "vendor/testing/bundler")
}
abort "Preinstall Bundler 4.0.23" if Gem::Specification.find_all_by_name("bundler", "= 4.0.23").empty?
command = ARGV.shift
arguments = case command
            when "setup" then ["install"]
            when "test" then ["exec", "rspec", *ARGV]
            when "check" then ["exec", "ruby", "scripts/check.rb"]
            when "build", "package", "example" then ["exec", "ruby", "scripts/#{command}.rb", *ARGV]
            else abort "Usage: run.rb setup|test|check|build|package|example"
            end
puts "Testing package; Ruby #{RUBY_VERSION}; Bundler 4.0.23; Gemfile #{environment.fetch('BUNDLE_GEMFILE')}"
$stdout.flush
exec(environment, RbConfig.ruby, "-S", "bundle", "_4.0.23_", *arguments, chdir: package)
