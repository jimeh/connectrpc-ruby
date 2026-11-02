# frozen_string_literal: true

require "simplecov"
SimpleCov.root(File.expand_path("../../..", __dir__))
SimpleCov.coverage_dir("coverage/rack/#{RUBY_VERSION}")
SimpleCov.start do
  enable_coverage :branch
  merging false
  skip "/spec/"
  skip "/scripts/"
  cover "gems/connectrpc-rack/lib/**/*.rb"
end

require "connectrpc/rack"

RSpec.configure do |config|
  config.order = :random
  config.disable_monkey_patching!
  config.expect_with(:rspec) { |expectations| expectations.syntax = :expect }
end
