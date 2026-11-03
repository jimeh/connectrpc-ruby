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

    def self.expired
      deadline = allocate
      deadline.instance_variable_set(:@clock, -> { Process.clock_gettime(Process::CLOCK_MONOTONIC) })
      deadline.instance_variable_set(:@expires_at, Process.clock_gettime(Process::CLOCK_MONOTONIC))
      deadline
    end
    private_class_method :expired

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
      raise Error.new(:deadline_exceeded, "Deadline exceeded"), cause: nil if expired?
    end

    def timeout_ms
      check!
      [remaining * 1000, 9_999_999_999].min.ceil.clamp(1, 9_999_999_999).to_s
    end
  end

  class Context
    attr_reader :request_metadata, :response_metadata, :response_trailers, :deadline

    def initialize(request_metadata: {}, deadline: nil, lifecycle: nil, cancellation: nil)
      @cancellation = CallCancellation.new(deadline: deadline, cancellation: cancellation)
      @request_metadata = Metadata.new(request_metadata)
      @response_metadata = Metadata.new
      @response_trailers = Metadata.new
      @deadline = deadline
      @lifecycle = lifecycle
    rescue StandardError
      @cancellation&.close
      raise
    end

    # Framework invokers record escaping application failures before normalizing them.
    def record_failure(error)
      @lifecycle&.record_failure(error)
      nil
    end

    def cancelled?
      @cancellation.cancelled?
    end

    def check!
      @cancellation.check!
    end

    def cancellation_reason = @cancellation.reason

    def commit! = @cancellation.commit!
    def close = @cancellation.close
  end
end
