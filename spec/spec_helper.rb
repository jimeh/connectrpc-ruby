# frozen_string_literal: true

require "simplecov"

SimpleCov.start do
  enable_coverage :branch
  cover "lib/**/*.rb"
  merging false
end

# The bundled HTML formatter omits branch summaries when there are no branches.
SimpleCov.at_exit do
  result = SimpleCov.result
  result.format!
  puts "Branch coverage: 0 / 0 (no branches)" if result.total_branches.zero?
end

require "connectrpc"

RSpec.configure do |config|
  config.order = :random
  config.disable_monkey_patching!
  config.expect_with(:rspec) { |expectations| expectations.syntax = :expect }
end
