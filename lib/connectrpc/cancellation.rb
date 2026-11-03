# frozen_string_literal: true

module ConnectRPC
  # A listener can be unregistered until invocation is claimed. It never waits
  # for a running listener, including when called by that listener itself.
  class CancellationRegistration
    def initialize(owner, &callback)
      @owner = owner
      @callback = callback
      @mutex = Mutex.new
      @state = :registered
    end

    def unregister
      # @type var removed: bool
      removed = @mutex.synchronize do
        next false unless @state == :registered

        @state = :unregistered
        @callback = nil
        true
      end
      @owner.remove(self) if removed
      removed
    end

    # Keep invocation, isolation and reference release in one ensure.
    def invoke # rubocop:disable Metrics/MethodLength
      # @type var callback: (^() -> void)?
      callback = @mutex.synchronize do
        next unless @state == :registered

        @state = :running
        selected = @callback
        @callback = nil
        selected
      end
      return unless callback

      begin
        callback.call
      rescue StandardError
        # One application listener must not prevent cancellation of other calls.
        nil
      ensure
        @mutex.synchronize { @state = :finished }
      end
    end
  end

  # Internal notification state shared by a source and its read-only token.
  class CancellationSignal
    def initialize
      @mutex = Mutex.new
      @cancelled = false
      @registrations = []
    end

    def cancelled? = @mutex.synchronize { @cancelled }

    def cancel # rubocop:disable Naming/PredicateMethod
      pending = @mutex.synchronize do
        return false if @cancelled

        @cancelled = true
        selected = @registrations
        @registrations = []
        selected
      end
      pending.each(&:invoke)
      true
    end

    def on_cancel(&)
      registration = CancellationRegistration.new(self, &)
      immediate = @mutex.synchronize do
        @registrations << registration unless @cancelled
        @cancelled
      end
      registration.invoke if immediate
      registration
    end

    def remove(registration)
      @mutex.synchronize { @registrations.delete(registration) }
      nil
    end
  end

  # Permanent, shareable cancellation. Only the source can initiate it.
  class CancellationSource
    attr_reader :token

    def initialize
      @signal = CancellationSignal.new
      @token = CancellationToken.new(@signal)
    end

    def cancel = @signal.cancel
  end

  class CancellationToken
    def initialize(signal)
      @signal = signal
    end

    def cancelled? = @signal.cancelled?

    def reason
      :canceled if cancelled?
    end

    def check!
      raise Error.new(:canceled, "Call canceled"), cause: nil if cancelled?
    end

    # Runs in the cancelling thread, or synchronously when already cancelled.
    # Listeners must be short and nonblocking. StandardError is isolated.
    def on_cancel(&) = @signal.on_cancel(&)
  end

  # One owner arbitrates a call's terminal outcome. Source cancellation requests
  # acceptance; it never completes another call that shares the source.
  class CallCancellation
    # Direct transports accept a token and deadline separately. Core clients
    # already supply their arbiter, which must stay open through message decode.
    def self.with_request(request) # rubocop:disable Metrics/MethodLength
      cancellation = request.cancellation
      return yield request unless cancellation

      case cancellation
      when CallCancellation then return yield request
      end

      state = new(deadline: request.deadline, cancellation: cancellation)
      outgoing = request.dup
      outgoing.cancellation = state
      begin
        result = yield outgoing
        state.commit!
        result
      rescue StandardError
        state.commit!
        raise
      ensure
        state.close
      end
    end

    def initialize(deadline: nil, cancellation: nil)
      @deadline = deadline
      @source = cancellation
      @mutex = Mutex.new
      @terminal = nil
      @signal = CancellationSignal.new
      @registration = cancellation&.on_cancel { reason }
    end

    def reason
      observe(false)
    end

    def cancelled? = !reason.nil?

    def check!
      raise_reason(reason)
    end

    def on_cancel(&)
      reason
      @signal.on_cancel(&)
    end

    def commit!
      raise_reason(observe(true))
    ensure
      close
    end

    def close
      @registration&.unregister
      @registration = nil
      nil
    end

    private

    # Keep the terminal read, precedence and acceptance adjacent.
    # rubocop:disable Metrics/MethodLength, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
    def observe(commit)
      terminal = @mutex.synchronize { @terminal }
      return terminal == :finished ? nil : terminal if terminal

      # Clocks and extension SPI methods can run application code. Sample them
      # outside the mutex; the serialized acceptance below owns the decision.
      # @type var observed: cancellation_reason?
      observed = @deadline&.expired? ? :deadline_exceeded : @source&.reason
      # A read-only extension query may yield while the real deadline expires.
      observed = :deadline_exceeded if @deadline&.expired?
      # @type var accepted: :finished | cancellation_reason | nil
      finished = :finished #: :finished
      accepted = @mutex.synchronize do
        @terminal ||= observed || (finished if commit)
        @terminal
      end
      reason = accepted == :finished ? nil : accepted
      @signal.cancel if reason
      reason
    end

    # rubocop:enable Metrics/MethodLength, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity

    def raise_reason(reason)
      return unless reason

      message = reason == :canceled ? "Call canceled" : "Deadline exceeded"
      raise Error.new(reason, message), cause: nil
    end
  end
end
