# frozen_string_literal: true

require "simplecov"
SimpleCov.start do
  root File.expand_path("..", __dir__)
  coverage_dir File.expand_path("../../../coverage/testing/#{RUBY_VERSION}", __dir__)
  enable_coverage :branch
  cover "lib/**/*.rb"
  merging false
end

require "connectrpc/testing"
require "connectrpc/bindings/google_protobuf"
$LOAD_PATH.unshift(File.expand_path("../../../spec/fixtures/generation/echo", __dir__))
require "echo_connect"

RSpec.configure do |config|
  config.order = :random
  config.disable_monkey_patching!
end
