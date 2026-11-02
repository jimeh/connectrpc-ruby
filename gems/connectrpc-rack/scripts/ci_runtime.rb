# frozen_string_literal: true

require "rbconfig"
require "open3"
gem "bundler", "= 4.0.23"
require "bundler"
root = File.expand_path("../../..", __dir__)
middleware = File.join(root, "gems/connectrpc-rack")
check = ->(value, message) { abort "Rack CI runtime guard: #{message}" unless value }
check.call(ARGV.empty?, "no arguments expected")
check.call(Dir.pwd == middleware, "unexpected working directory")
{
  "BUNDLE_GEMFILE" => File.join(middleware, "Gemfile"),
  "BUNDLE_PATH" => File.join(root, "vendor/rack/bundle"),
  "BUNDLE_USER_HOME" => File.join(root, "vendor/rack/bundler"),
  "BUNDLE_COOLDOWN" => "7"
}.each { |key, value| check.call(ENV.fetch(key) == value, "unexpected #{key}") }
check.call(Bundler.settings[:path] == File.join(root, "vendor/rack/bundle"), "unexpected actual bundle path")
check.call(Bundler.default_gemfile.to_s == File.join(middleware, "Gemfile"), "unexpected actual Gemfile")
# Reuse the exact root runtime/PATH/frozen-config guard in a separate process;
# its root-bundle assertion remains intact for all nine core and Rails lanes.
environment = { "BUNDLE_PATH" => File.join(root, "vendor/bundle"), "BUNDLE_GEMFILE" => File.join(root, "Gemfile") }
system(environment, RbConfig.ruby, File.join(root, "scripts/ci_runtime.rb"), exception: true)
puts "Rack bundle path: #{Bundler.bundle_path}; Gemfile: #{Bundler.default_gemfile}"
