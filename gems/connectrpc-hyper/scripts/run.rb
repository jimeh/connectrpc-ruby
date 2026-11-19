# frozen_string_literal: true

require "rbconfig"
require_relative "../../../scripts/dependency_floors"
require_relative "bindgen"

root = File.expand_path("../../..", __dir__)
package = File.join(root, "gems/connectrpc-hyper")
environment = {
  "RUBOCOP_CACHE_ROOT" => File.join(root, "vendor/hyper/rubocop"),
  "BUNDLE_GEMFILE" => DependencyFloors.gemfile(File.join(package, "Gemfile")),
  "BUNDLE_LOCKFILE" => nil, "BUNDLE_IGNORE_CONFIG" => "true",
  "BUNDLE_COOLDOWN" => "3", "BUNDLE_VERSION" => "4.0.23",
  "BUNDLE_PATH" => File.join(root, "vendor/hyper/bundle"), "BUNDLE_DISABLE_SHARED_GEMS" => "true",
  "BUNDLE_USER_HOME" => File.join(root, "vendor/hyper/bundler"),
  # Development builds keep one extension per Ruby minor, as platform gems do.
  "CONNECTRPC_HYPER_LIB_DIR" => "lib/connectrpc/hyper/#{RUBY_VERSION[/\A\d+\.\d+/]}",
  "RB_SYS_CARGO_TARGET_DIR" => File.join(root, "vendor/hyper/target", RUBY_VERSION)
}
environment.merge!(HyperBindgen.environment)
abort "Preinstall Bundler 4.0.23" if Gem::Specification.find_all_by_name("bundler", "= 4.0.23").empty?
command = ARGV.shift
arguments = case command
            when "setup" then ["exec", "ruby", "scripts/compile.rb"]
            when "install" then ["install"]
            when "floors" then ["exec", "ruby", File.join(root, "scripts/dependency_floors.rb")]
            when "test" then ["exec", "rspec", *ARGV]
            when "check" then ["exec", "ruby", "scripts/check.rb"]
            when "build", "package", "interop", "cross" then ["exec", "ruby", "scripts/#{command}.rb", *ARGV]
            else abort "Usage: run.rb install|setup|floors|test|check|build|package|interop|cross"
            end
puts "Hyper package; Ruby #{RUBY_VERSION}; Bundler 4.0.23; Gemfile #{environment.fetch('BUNDLE_GEMFILE')}"
$stdout.flush
if %w[setup cross].include?(command)
  system(environment, RbConfig.ruby, "-S", "bundle", "_4.0.23_", "install", chdir: package, exception: true)
end
exec(environment, RbConfig.ruby, "-S", "bundle", "_4.0.23_", *arguments, chdir: package)
