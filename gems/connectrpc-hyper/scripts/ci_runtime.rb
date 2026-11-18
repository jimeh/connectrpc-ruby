# frozen_string_literal: true

require "rbconfig"
require "open3"
gem "bundler", "= 4.0.23"
require "bundler"
root = File.expand_path("../../..", __dir__)
backend = File.join(root, "gems/connectrpc-hyper")
check = ->(value, message) { abort "Hyper CI runtime guard: #{message}" unless value }
check.call(ARGV.empty?, "no arguments expected")
check.call(Dir.pwd == backend, "unexpected working directory")
{
  "BUNDLE_GEMFILE" => File.join(backend, "Gemfile"),
  "BUNDLE_PATH" => File.join(root, "vendor/hyper/bundle"),
  "BUNDLE_USER_HOME" => File.join(root, "vendor/hyper/bundler"),
  "BUNDLE_COOLDOWN" => "3"
}.each { |key, value| check.call(ENV.fetch(key) == value, "unexpected #{key}") }
check.call(Bundler.settings[:path] == File.join(root, "vendor/hyper/bundle"), "unexpected actual bundle path")
check.call(Bundler.default_gemfile.to_s == File.join(backend, "Gemfile"), "unexpected actual Gemfile")
rustc = IO.popen(%w[rustc --version], err: File::NULL, &:read).to_s
check.call(rustc.start_with?("rustc 1.98.0 "), "unexpected Rust toolchain #{rustc.strip.inspect}")
# Reuse the exact root runtime/PATH/bundle-config guard in a separate process;
# its root-bundle assertion remains intact for all nine core and Rails lanes.
environment = { "BUNDLE_PATH" => File.join(root, "vendor/bundle"), "BUNDLE_GEMFILE" => File.join(root, "Gemfile") }
system(environment, RbConfig.ruby, File.join(root, "scripts/ci_runtime.rb"), exception: true)
puts "Hyper bundle path: #{Bundler.bundle_path}; Gemfile: #{Bundler.default_gemfile}; #{rustc.strip}"
