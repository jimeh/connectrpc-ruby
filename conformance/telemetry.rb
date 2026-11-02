# frozen_string_literal: true

require "json"

module Conformance
  module Telemetry
    def self.record(event, **fields)
      target = ENV.fetch("CONFORMANCE_TELEMETRY", nil)
      return unless target

      File.open(target, "a") do |file|
        file.flock(File::LOCK_EX)
        file.puts(JSON.generate({ event: event, pid: Process.pid, monotonic: Process.clock_gettime(Process::CLOCK_MONOTONIC) }
                                 .merge(fields)))
      end
    end
  end
end
