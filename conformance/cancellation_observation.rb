# frozen_string_literal: true

require_relative "telemetry"

module Conformance
  # One observer owns the entire real Client invocation and its nested transport
  # scopes. A zero count is meaningful only after this lifetime has closed.
  class CancellationObservation
    def initialize(name)
      @name = name
      @counts = %i[transport setup exchange].to_h { |phase| [phase, { entries: 0, exits: 0 }] }
    end

    def observe(phase, **admission)
      @counts.fetch(phase)[:entries] += 1 unless phase == :call
      Telemetry.record("#{phase}_started", name: @name, **admission)
      yield
    rescue StandardError => e
      error = e
      raise
    ensure
      @counts.fetch(phase)[:exits] += 1 unless phase == :call
      fields = terminal(error)
      if phase == :call
        @counts.each { |key, counts| counts.each { |edge, count| fields["#{key}_#{edge}"] = count } }
        fields[:observer_complete] = true
      end
      Telemetry.record("#{phase}_finished", name: @name, **fields)
    end

    private

    def terminal(error)
      cause = error&.cause
      { code: error.is_a?(ConnectRPC::Error) ? error.code : nil,
        cause: cause&.class&.name, error_class: error&.class&.name }
    end
  end
end
