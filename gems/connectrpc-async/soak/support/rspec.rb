# frozen_string_literal: true

module Soak
  # Example helpers and RSpec configuration shared by both package campaigns.
  module RSpecSetup
    module Example
      # Runs one campaign on a reactor the harness owns: no example timeout or
      # child cancellation hides a leak.
      def campaign(name, **info, &)
        Sync { |task| Campaign.new(name, task: task, info: info).conduct(&) }
      end
    end

    module_function

    def configure
      RSpec.configure do |config|
        config.include AsyncHelpers
        config.include ConnectionPeer
        config.include ThreadedHelpers
        config.include KeepaliveClient
        config.include Example
        config.disable_monkey_patching!
        config.order = :random
        config.seed = Soak.seed
        # A regular expression matched against example descriptions.
        only = ENV.fetch("CONNECTRPC_SOAK_ONLY", "")
        config.filter_run_including(full_description: Regexp.new(only)) unless only.empty?
        config.before(:suite) do
          Soak.report.environment(package: Soak.package, seed: Soak.seed, rounds: Soak.rounds)
          RSpecSetup.trap_deadline
        end
      end
    end

    # The supervisor sends TERM at the run deadline. Record where everything
    # is, then exit; the supervisor kills the process group after its cleanup
    # budget if this hangs.
    def trap_deadline
      Signal.trap("TERM") do
        Thread.new do
          Soak.report.write(:diagnostics, reason: "deadline", scenario: Soak.current, **Report.diagnostics)
        ensure
          Process.exit!(124)
        end
      end
    end
  end
end
