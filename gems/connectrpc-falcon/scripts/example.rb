# frozen_string_literal: true

require "fileutils"
require "tmpdir"
require "rbconfig"
root = File.expand_path("../../..", __dir__)
Dir.mktmpdir("connectrpc-falcon-example-") do |directory|
  FileUtils.cp(File.join(__dir__, "integration.rb"), directory)
  FileUtils.cp(File.join(root, "gems/connectrpc-async/spec/support/helpers.rb"), File.join(directory, "helpers.rb"))
  FileUtils.cp_r(File.join(root, "spec/fixtures/generation/echo"), File.join(directory, "generated"))
  system(RbConfig.ruby, File.join(directory, "integration.rb"), File.expand_path("../examples/falcon.rb", __dir__),
         exception: true)
end
