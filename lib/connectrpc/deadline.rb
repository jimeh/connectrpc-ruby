# frozen_string_literal: true

module ConnectRPC
  class Deadline
    # Bound native time conversions without shortening the logical deadline.
    MAX_NATIVE_WAIT = 9_999_999.999
    # Deadlines on this shared default clock compare by absolute expiry.
    MONOTONIC = -> { Process.clock_gettime(Process::CLOCK_MONOTONIC) }
    private_constant :MONOTONIC

    def initialize(timeout, clock: MONOTONIC)
      # Zero and negative budgets are already expired, as in grpc-go.
      unless timeout.is_a?(Numeric) && timeout.real? && timeout.finite?
        raise ArgumentError, "Timeout must be finite real seconds"
      end

      @clock = clock
      @expires_at = @clock.call + timeout
    end

    def self.expired
      deadline = allocate
      deadline.instance_variable_set(:@clock, MONOTONIC)
      deadline.instance_variable_set(:@expires_at, MONOTONIC.call)
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

    protected

    attr_reader :expires_at

    private

    # The earlier of this deadline and a timeout budget started now. The budget
    # reads this deadline's clock, so both expiries share one timeline and no
    # gap between clock reads can reorder them; a tie selects the budget.
    def bounded_by(timeout)
      budget = Deadline.new(timeout, clock: @clock)
      expires_at < budget.expires_at ? self : budget
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

    # Runs once when the call is cancelled or its deadline is observed expired:
    # synchronously in the cancelling thread, or immediately when already
    # cancelled. Listeners must be short and nonblocking; StandardError is
    # isolated. Unregister the returned registration when no longer needed.
    def on_cancel(&) = @cancellation.on_cancel(&)

    def commit! = @cancellation.commit!
    def close = @cancellation.close
  end
end
