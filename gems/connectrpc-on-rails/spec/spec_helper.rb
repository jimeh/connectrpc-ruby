# frozen_string_literal: true

require "simplecov"

SimpleCov.start do
  root File.expand_path("..", __dir__)
  coverage_dir File.join(ENV.fetch("CONNECTRPC_ROOT"), "vendor/rails/coverage")
  enable_coverage :branch
  cover "lib/**/*.rb"
  merging false
end

require "bundler"
require "connectrpc-on-rails"
require_relative "support/application"

abort "Unexpected Bundler" unless Bundler::VERSION == "4.0.23"
abort "Wrong Rails lane: #{Rails.version}" unless Rails.version.start_with?("#{ENV.fetch('CONNECTRPC_RAILS_LANE')}.")
# Set only by the lowest-allowed bundle, which must load the exact Action Pack floor.
exact = ENV.fetch("CONNECTRPC_RAILS_VERSION", nil)
abort "Wrong Rails floor: #{Rails.version}, expected #{exact}" if exact && Rails.version != exact
puts "Actual Rails #{Rails.version}; Ruby #{RUBY_VERSION}; Bundler #{Bundler::VERSION}"
puts "Gemfile #{ENV.fetch('BUNDLE_GEMFILE')}"

RSpec.configure do |config|
  config.order = :random
  config.disable_monkey_patching!
  config.expect_with(:rspec) { |expectations| expectations.syntax = :expect }
end
