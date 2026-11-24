# frozen_string_literal: true

module ConnectRPC
  module Async
    module Internal
      # Teardown runs every step whatever an earlier one raised. A step's
      # StandardError never becomes the caller's outcome: it is written as a
      # warning that names only its class, since a message can carry
      # addresses or credentials. Only an interruption of the task running
      # teardown, such as Async::Cancel, is raised, once every step has run.
      module Teardown
        # Runs one step. Returns its interruption, or nil.
        def self.capture
          yield
          nil
        rescue Exception => e # rubocop:disable Lint/RescueException -- the runner raises an interruption after its remaining steps
          escape(e)
        end

        # Reports a StandardError and returns nil. Returns any other error,
        # an interruption, unchanged.
        def self.escape(error)
          return error unless error.is_a?(StandardError)

          warn "ConnectRPC teardown error: #{error.class}"
          nil
        rescue StandardError
          nil # A broken diagnostic stream must not stop a later step.
        rescue Exception => e # rubocop:disable Lint/RescueException -- an interrupted warning is still the runner's to raise
          e
        end

        # Raises the first interruption the steps returned.
        def self.finish(escapes)
          interruption = escapes.compact.first
          raise interruption if interruption
        end

        # Runs a whole teardown body, as a cleanup task does.
        def self.run(&) = finish([capture(&)])
      end
    end
  end
end
