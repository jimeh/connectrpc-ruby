# frozen_string_literal: true

require "fileutils"
require "tmpdir"
require "rbconfig"
require_relative "support/activation"
root = File.expand_path("../../..", __dir__)
Dir.mktmpdir("example-", File.join(root, "vendor/async")) do |working|
  FileUtils.cp(File.join(__dir__, "integration.rb"), working)
  FileUtils.cp(File.join(__dir__, "../spec/support/helpers.rb"), File.join(working, "helpers.rb"))
  FileUtils.cp_r(File.join(root, "spec/fixtures/generation/echo"), File.join(working, "generated"))
  system(Activation.environment(installed: false), RbConfig.ruby, File.join(working, "integration.rb"),
         chdir: working, exception: true)
end
