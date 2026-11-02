# frozen_string_literal: true

require "bundler"
require_relative "harness"

root = File.expand_path("..", __dir__)
working = File.join(root, "vendor/rbs/check", RUBY_VERSION)
FileUtils.mkdir_p(working)
%w[consumers nominal].each { |name| FileUtils.cp_r(File.join(__dir__, name), working) }
Bundler.with_unbundled_env do
  environment = {
    "BUNDLE_GEMFILE" => File.join(root, "Gemfile"), "BUNDLE_PATH" => File.join(root, "vendor/bundle"),
    "BUNDLE_APP_CONFIG" => File.join(root, "vendor/rbs/root-config"),
    "BUNDLE_USER_HOME" => File.join(root, "vendor/rbs/bundler"), "BUNDLE_FROZEN" => "true"
  }
  system(environment, RbConfig.ruby, "-S", "bundle", "_4.0.23_", "exec", RbConfig.ruby,
         File.join(__dir__, "generate.rb"), File.join(working, "generation"), exception: true)
  system(environment, RbConfig.ruby, "-S", "bundle", "_4.0.23_", "exec", RbConfig.ruby,
         "-I", File.join(root, "lib"), "-I", File.join(working, "generation/protoc"),
         File.join(__dir__, "runtime.rb"), exception: true)
end
RBSHarness.check(working: working, core: File.join(root, "sig"), bridge: File.join(root, "rbs/google_protobuf_4_36"),
                 generated: File.join(working, "generation/protoc"))
