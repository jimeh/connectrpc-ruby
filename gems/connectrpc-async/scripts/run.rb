# frozen_string_literal: true

require "rbconfig"
require_relative "../../../scripts/dependency_canary"

root = File.expand_path("../../..", __dir__)
backend = File.join(root, "gems/connectrpc-async")
gemfile = File.join(backend, "Gemfile")
command = ARGV.shift
begin
  DependencyCanary.allow!(command)
rescue RuntimeError => e
  abort e.message
end
environment = {
  "RUBOCOP_CACHE_ROOT" => File.join(root, "vendor/async/rubocop"),
  "BUNDLE_GEMFILE" => DependencyCanary.enabled? ? DependencyCanary.gemfile(gemfile) : DependencyFloors.gemfile(gemfile),
  "BUNDLE_LOCKFILE" => nil, "BUNDLE_IGNORE_CONFIG" => "true",
  "BUNDLE_COOLDOWN" => "3", "BUNDLE_VERSION" => "4.0.23",
  "BUNDLE_DISABLE_SHARED_GEMS" => "true",
  **DependencyCanary.bundle_settings(File.join(root, "vendor/async"))
}
arguments = case command
            when "grpc_interop" then ["exec", "ruby", File.join(root, "scripts/grpc_interop.rb"), *ARGV]
            when "grpc_keepalive_interop"
              ["exec", "ruby", File.join(root, "scripts/grpc_keepalive_interop.rb"), *ARGV]
            when "grpc_generation_check" then ["exec", "ruby", File.join(root, "scripts/build_grpc_peer.rb"), "--check"]
            when "web_conformance" then ["exec", "ruby", File.join(root, "scripts/web_conformance.rb"), *ARGV]
            when "conformance" then ["exec", "ruby", File.join(root, "scripts/native_conformance.rb"), *ARGV]
            when "setup" then ["install"]
            when "floors" then ["exec", "ruby", File.join(root, "scripts/dependency_floors.rb")]
            when "canary" then nil
            when "exec" then ["exec", *ARGV]
            when "test" then ["exec", "rspec", *ARGV]
            when "check" then ["exec", "rubocop", ".", "--config", File.join(root, ".rubocop.yml")]
            when "build", "package", "example", "interop", "streaming_interop", "http1_interop"
              ["exec", "ruby", "scripts/#{command}.rb", *ARGV]
            else abort "Usage: run.rb setup|floors|canary|test|check|build|package|example|interop|exec"
            end
unless command == "exec"
  puts "Async backend; Ruby #{RUBY_VERSION}; Bundler 4.0.23; Gemfile #{environment.fetch('BUNDLE_GEMFILE')}"
end
$stdout.flush
# The canary driver runs outside Bundler and starts its own bounded bundle processes.
exec(environment, RbConfig.ruby, File.join(root, "scripts/canary_driver.rb"), chdir: backend) if command == "canary"
exec(environment, RbConfig.ruby, "-S", "bundle", "_4.0.23_", *arguments, chdir: backend)
