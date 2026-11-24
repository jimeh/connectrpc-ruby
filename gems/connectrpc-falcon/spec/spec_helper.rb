# frozen_string_literal: true

require "simplecov"
SimpleCov.root(File.expand_path("../../..", __dir__))
SimpleCov.coverage_dir("coverage/falcon/#{RUBY_VERSION}")
SimpleCov.start do
  enable_coverage :branch
  merging false
  skip "/spec/"
  skip "/scripts/"
  cover "gems/connectrpc-falcon/lib/**/*.rb"
  cover "gems/connectrpc-async/lib/**/*.rb"
end

require "connectrpc/falcon"
require "connectrpc/bindings/google_protobuf"
require "tmpdir"
$LOAD_PATH.unshift(File.expand_path("../../../spec/fixtures/generation/echo", __dir__))
require "echo_connect"
require_relative "../../connectrpc-async/spec/support/helpers"
require_relative "support/helpers"

# Ruby warns once per process on first IO::Buffer use, which io-stream's
# buffered reads trigger. Warn here, so the warning never lands in whichever
# example's captured stderr runs first (#132).
IO::Buffer.new(1).free

RSpec.configure do |config|
  config.include AsyncHelpers
  config.include TeardownHelpers
  config.include FalconHelpers
  config.order = :random
  config.disable_monkey_patching!
end
