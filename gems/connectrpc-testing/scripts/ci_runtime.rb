# frozen_string_literal: true

require "rbconfig"
gem "bundler", "= 4.0.23"
require "bundler"
root = File.expand_path("../../..", __dir__)
package = File.dirname(__dir__)
check = ->(condition, message) { abort "Testing CI runtime guard: #{message}" unless condition }
check.call(ARGV.empty?, "no arguments expected")
check.call(Dir.pwd == package, "unexpected working directory")
check.call(ENV.fetch("CI_EXPECTED_RUBY_VERSION") == RUBY_VERSION, "unexpected Ruby")
check.call(%w[3.4.12 4.0.9].include?(RUBY_VERSION), "unsupported Ruby")
cache = File.realpath(ENV.fetch("RUNNER_TOOL_CACHE"))
ruby = ENV.fetch("PATH").split(File::PATH_SEPARATOR).map { |entry| File.join(entry, "ruby") }
          .find { |entry| File.file?(entry) && File.executable?(entry) }
check.call(File.realpath(ruby) == File.realpath(RbConfig.ruby), "PATH Ruby differs from running Ruby")
check.call(File.realpath(RbConfig.ruby).start_with?("#{cache}/"), "Ruby outside action tool cache")
{
  "BUNDLE_IGNORE_CONFIG" => "true", "BUNDLE_FROZEN" => "true",
  "BUNDLE_DISABLE_SHARED_GEMS" => "true", "BUNDLE_VERSION" => "4.0.23",
  "BUNDLE_GEMFILE" => File.join(package, "Gemfile"),
  "BUNDLE_PATH" => File.join(root, "vendor/testing/bundle"),
  "BUNDLE_USER_HOME" => File.join(root, "vendor/testing/bundler"), "BUNDLE_COOLDOWN" => "7"
}.each { |key, value| check.call(ENV.fetch(key) == value, "unexpected #{key}") }
check.call(Bundler::VERSION == "4.0.23", "unexpected actual Bundler")
check.call(Bundler.settings[:path] == File.join(root, "vendor/testing/bundle"), "unexpected actual bundle path")
check.call(Bundler.default_gemfile.to_s == File.join(package, "Gemfile"), "unexpected actual Gemfile")
puts "Testing runtime guard passed: Ruby #{RUBY_VERSION}, Bundler #{Bundler::VERSION}, #{Bundler.bundle_path}"
