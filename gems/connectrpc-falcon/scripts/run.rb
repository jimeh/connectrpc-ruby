# frozen_string_literal: true

require "rbconfig"

root = File.expand_path("../../..", __dir__)
backend = File.join(root, "gems/connectrpc-falcon")
environment = {
  "RUBOCOP_CACHE_ROOT" => File.join(root, "vendor/falcon/rubocop"),
  "BUNDLE_GEMFILE" => File.join(backend, "Gemfile"), "BUNDLE_IGNORE_CONFIG" => "true",
  "BUNDLE_FROZEN" => "true", "BUNDLE_COOLDOWN" => "7", "BUNDLE_VERSION" => "4.0.23",
  "BUNDLE_PATH" => File.join(root, "vendor/falcon/bundle"), "BUNDLE_DISABLE_SHARED_GEMS" => "true",
  "BUNDLE_USER_HOME" => File.join(root, "vendor/falcon/bundler")
}
command = ARGV.shift
arguments = case command
            when "grpc_interop" then ["exec", "ruby", File.join(root, "scripts/grpc_interop.rb"), *ARGV]
            when "conformance" then ["exec", "ruby", File.join(root, "scripts/native_conformance.rb"), *ARGV]
            when "setup" then ["install"]
            when "test" then ["exec", "rspec", *ARGV]
            when "check" then ["exec", "rubocop", ".", "--config", File.join(root, ".rubocop.yml"), *ARGV]
            when "build", "package", "example", "interop", "streaming_interop" then ["exec", "ruby",
                                                                                     "scripts/#{command}.rb", *ARGV]
            else abort "Usage: run.rb setup|test|check|build|package|example|interop"
            end
puts "Falcon backend; Ruby #{RUBY_VERSION}; Bundler 4.0.23; Gemfile #{environment.fetch('BUNDLE_GEMFILE')}"
$stdout.flush
exec(environment, RbConfig.ruby, "-S", "bundle", "_4.0.23_", *arguments, chdir: backend)
