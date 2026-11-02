# frozen_string_literal: true

require "fileutils"
require "rbconfig"

root = File.expand_path("..", __dir__)
output = File.join(root, "vendor/rbs")
FileUtils.mkdir_p(File.join(output, "tmp"))
environment = {
  "BUNDLE_GEMFILE" => File.join(__dir__, "Gemfile"),
  "BUNDLE_PATH" => File.join(output, "bundle"),
  "BUNDLE_USER_HOME" => File.join(output, "bundler"),
  "BUNDLE_APP_CONFIG" => File.join(output, "config"),
  "BUNDLE_CACHE_PATH" => File.join(output, "cache"),
  "BUNDLE_FROZEN" => "true", "BUNDLE_IGNORE_CONFIG" => "true",
  "BUNDLE_DISABLE_SHARED_GEMS" => "true", "BUNDLE_COOLDOWN" => "7",
  "XDG_CACHE_HOME" => File.join(output, "xdg"), "TMPDIR" => File.join(output, "tmp"),
  "BUF_CACHE_DIR" => File.join(output, "buf")
}
environment.delete("TMPDIR") if ARGV == ["package"]
command = case ARGV
          when ["setup"] then ["install"]
          when ["check"] then ["exec", RbConfig.ruby, File.join(__dir__, "check.rb")]
          when ["package"] then ["exec", RbConfig.ruby, File.join(__dir__, "package.rb")]
          else abort "Usage: typecheck/run.rb setup|check|package"
          end
exec(environment, RbConfig.ruby, "-S", "bundle", "_4.0.23_", *command)
