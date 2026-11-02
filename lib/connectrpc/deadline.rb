# frozen_string_literal: true

module ConnectRPC
  class Deadline
    # Bound native time conversions without shortening the logical deadline.
    MAX_NATIVE_WAIT = 9_999_999.999

    def initialize(timeout, clock: -> { Process.clock_gettime(Process::CLOCK_MONOTONIC) })
      unless timeout.is_a?(Numeric) && timeout.real? && timeout.finite? && timeout.positive?
        raise ArgumentError, "Timeout must be finite positive seconds"
      end

      @clock = clock
      @expires_at = @clock.call + timeout
    end

    def remaining
      [@expires_at - @clock.call, 0.0].max
    end

    def expired?
      remaining.zero?
    end

    def native_timeout
      [remaining, MAX_NATIVE_WAIT].min
    end

    def check!
      raise Error.new(:deadline_exceeded, "Deadline exceeded") if expired?
    end

    def timeout_ms
      check!
      [remaining * 1000, 9_999_999_999].min.ceil.clamp(1, 9_999_999_999).to_s
    end
  end

  class Context
    attr_reader :request_metadata, :response_metadata, :response_trailers, :deadline

    def initialize(request_metadata: {}, deadline: nil)
      @request_metadata = Metadata.new(request_metadata)
      @response_metadata = Metadata.new
      @response_trailers = Metadata.new
      @deadline = deadline
    end

    def cancelled?
      deadline&.expired? || false
    end

    def check!
      deadline&.check!
    end
  end
end
