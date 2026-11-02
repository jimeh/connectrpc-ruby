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
require "connectrpc/rails"
require_relative "support/application"

abort "Unexpected Bundler" unless Bundler::VERSION == "4.0.23"
abort "Wrong Rails lane: #{Rails.version}" unless Rails.version == ENV.fetch("CONNECTRPC_RAILS_VERSION")
puts "Actual Rails #{Rails.version}; Ruby #{RUBY_VERSION}; Bundler #{Bundler::VERSION}"
puts "Gemfile #{ENV.fetch('BUNDLE_GEMFILE')}"

RSpec.configure do |config|
  config.order = :random
  config.disable_monkey_patching!
  config.expect_with(:rspec) { |expectations| expectations.syntax = :expect }
end
