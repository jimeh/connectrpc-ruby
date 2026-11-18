# frozen_string_literal: true

module ConnectRPC
  module Async
    module Internal
      # Teardown runs every step whatever an earlier one raised, then reports
      # one escape.
      module Teardown
        # Returns what the block raised, interruptions included.
        def self.capture
          yield
          nil
        rescue Exception => e # rubocop:disable Lint/RescueException -- teardown reports every escape once it finishes
          e
        end

        # The first interruption wins over an ordinary failure, as in
        # CompletionJoin.
        def self.first(failures)
          failures = failures.compact
          failures.find { |error| !error.is_a?(StandardError) } || failures.first
        end
      end
    end
  end
end
