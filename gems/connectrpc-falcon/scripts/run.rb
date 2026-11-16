# frozen_string_literal: true

require "rbconfig"
require_relative "../../../scripts/dependency_floors"

root = File.expand_path("../../..", __dir__)
backend = File.join(root, "gems/connectrpc-falcon")
environment = {
  "RUBOCOP_CACHE_ROOT" => File.join(root, "vendor/falcon/rubocop"),
  "BUNDLE_GEMFILE" => DependencyFloors.gemfile(File.join(backend, "Gemfile")),
  "BUNDLE_LOCKFILE" => nil, "BUNDLE_IGNORE_CONFIG" => "true",
  "BUNDLE_COOLDOWN" => "3", "BUNDLE_VERSION" => "4.0.23",
  "BUNDLE_PATH" => File.join(root, "vendor/falcon/bundle"), "BUNDLE_DISABLE_SHARED_GEMS" => "true",
  "BUNDLE_USER_HOME" => File.join(root, "vendor/falcon/bundler")
}
command = ARGV.shift
arguments = case command
            when "grpc_interop" then ["exec", "ruby", File.join(root, "scripts/grpc_interop.rb"), *ARGV]
            when "grpc_keepalive_interop"
              ["exec", "ruby", File.join(root, "scripts/grpc_keepalive_interop.rb"), *ARGV]
            when "web_conformance" then ["exec", "ruby", File.join(root, "scripts/web_conformance.rb"), *ARGV]
            when "conformance" then ["exec", "ruby", File.join(root, "scripts/native_conformance.rb"), *ARGV]
            when "setup" then ["install"]
            when "floors" then ["exec", "ruby", File.join(root, "scripts/dependency_floors.rb")]
            when "exec" then ["exec", *ARGV]
            when "test" then ["exec", "rspec", *ARGV]
            when "check" then ["exec", "rubocop", ".", "--config", File.join(root, ".rubocop.yml"), *ARGV]
            when "build", "package", "example", "interop", "streaming_interop" then ["exec", "ruby",
                                                                                     "scripts/#{command}.rb", *ARGV]
            else abort "Usage: run.rb setup|floors|test|check|build|package|example|interop|exec"
            end
unless command == "exec"
  puts "Falcon backend; Ruby #{RUBY_VERSION}; Bundler 4.0.23; Gemfile #{environment.fetch('BUNDLE_GEMFILE')}"
end
$stdout.flush
exec(environment, RbConfig.ruby, "-S", "bundle", "_4.0.23_", *arguments, chdir: backend)
