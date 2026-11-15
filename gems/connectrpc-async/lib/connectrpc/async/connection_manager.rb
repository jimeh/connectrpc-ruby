# frozen_string_literal: true

module ConnectRPC
  module Async
    module Internal
      # Reactor-owned HTTP/2 client connections for one Transport: stream
      # accounting, one shared attempt at a time, backoff, admission bytes and
      # published state. Attempts run under the transport lifetime, outside any
      # RPC task or lifecycle; waiting calls only observe them.
      class ConnectionManager
        UNAVAILABLE = "HTTP/2 transport admission is closed or full"
        CLOSING = "HTTP/2 transport is closing"
        TRANSIENT = "HTTP/2 connection is in transient failure"
        STOPPED = "HTTP/2 connection setup stopped"
        TERMINAL = %i[closing closed].freeze

        # One shared attempt. Its waiters read the recorded outcome and error.
        class Attempt
          attr_accessor :task, :resource, :outcome, :error
          attr_reader :started_at

          def initialize(started_at)
            @started_at = started_at
          end

          def failed? = !@outcome.nil? && @outcome != :ready
        end
        private_constant :Attempt

        # An acquisition waiter. A fail-fast waiter joins at most one attempt.
        Waiter = Struct.new(:wait_for_ready, :joined)
        private_constant :Waiter

        attr_reader :state, :pending_bytes

        def initialize(policy:, observer:, connection_limit:, connect_timeout:, lifetime:, progress:, build:,
                       setup:, abort_setup:)
          @policy = policy
          @observer = observer
          @connection_limit = connection_limit
          @connect_timeout = connect_timeout
          @lifetime = lifetime
          @progress = progress
          @build = build
          @setup = setup
          @abort_setup = abort_setup
          @connections = {}.compare_by_identity
          @waiters = {}.compare_by_identity
          @attempt = nil
          @failures = 0
          @timer = nil
          @retry_at = nil
          @pending_bytes = 0
          @state = :idle
          @closing = false
          @dispatching = nil
          @clock = MonotonicClock
          @random = ::Random
        end

        # Stream usage summed over every accounted connection.
        def usage = @connections.each_value.sum

        def connections = @connections.dup

        def check_reentry!
          return unless @dispatching.equal?(Fiber.current)

          raise LifecycleError, "Transport calls and close are not allowed from a connection observer"
        end

        # Reserves request bytes for an admitted call. A full count, an
        # exhausted byte budget or an oversized call fails immediately.
        def admit(bytes, full:)
          limit = @policy.max_pending_bytes
          if full || (limit && @pending_bytes + bytes > limit)
            emit(:admission_rejected)
            raise Error.new(:unavailable, UNAVAILABLE), cause: nil
          end
          @pending_bytes += bytes
          bytes
        end

        def release_bytes(bytes)
          @pending_bytes -= bytes
        end

        def acquire(wait_for_ready: @policy.wait_for_ready)
          waiter = Waiter.new(wait_for_ready, nil)
          @waiters[waiter] = true
          loop do
            raise Error.new(:unavailable, CLOSING), cause: nil if @closing

            if (connection = available)
              @connections[connection] += 1
              return connection
            end
            joined = waiter.joined
            if joined&.failed?
              # Fail-fast waiters wait for one attempt, then only for capacity.
              raise joined.error unless usable_connection?
            elsif @attempt
              waiter.joined = @attempt unless wait_for_ready
            elsif @retry_at
              raise Error.new(:unavailable, TRANSIENT), cause: nil unless wait_for_ready || usable_connection?
            elsif slot_free?
              attempt = start_attempt
              waiter.joined = attempt unless wait_for_ready
              next
            end
            @progress.wait
          end
        ensure
          @waiters.delete(waiter)
        end

        # Exactly-once lease release. A connection that stopped accepting
        # streams retires when its last lease is released.
        def release(connection)
          usage = @connections[connection]
          return unless usage
          raise "Trying to release an unacquired HTTP/2 connection" if usage.zero?

          usage = @connections[connection] = usage - 1
          if usage.zero? && !usable?(connection)
            retire(connection)
          else
            @progress.signal
          end
          nil
        end

        # Removes the slot from accounting and wakes waiters before the
        # potentially blocking close joins the connection's retirement.
        def retire(connection)
          return unless @connections.key?(connection)

          @connections.delete(connection)
          reconcile
          connection.close
          nil
        end

        def usable?(connection)
          !connection.resource&.finished? && connection.reusable?
        end

        def close
          return if @closing

          @closing = true
          publish(:closing)
          Internal.cancel(@timer)
          @timer = @retry_at = nil
          Internal.cancel(@attempt&.task)
          @progress.signal
        end

        def finish
          close
          @connections.delete_if { |_, usage| usage.zero? }
          publish(:closed)
        end

        private

        def available
          @connections.each do |connection, usage|
            return connection if usage < connection.maximum_concurrent_streams && usable?(connection)
          end
          nil
        end

        def usable_connection? = @connections.each_key.any? { |connection| usable?(connection) }
        def slot_free? = @connections.size + (@attempt ? 1 : 0) < @connection_limit
        def demand? = @waiters.each_key.any? { |waiter| waiter.wait_for_ready || !waiter.joined&.failed? }

        def current_state
          return @state if TERMINAL.include?(@state)
          return :ready if usable_connection?
          return :connecting if @attempt
          return :transient_failure if @retry_at

          :idle
        end

        # Starts a demand-driven attempt when one is allowed, then republishes.
        def reconcile
          unless @closing
            start_attempt if !@attempt && !@retry_at && slot_free? && demand?
            publish(current_state)
          end
          @progress.signal
        end

        def connection_unusable(resource)
          connection = resource.connection
          return unless connection && @connections.key?(connection)

          @connections.delete(connection) if @connections[connection].zero?
          reconcile
        end

        # Publishes the attempt before running its eager task, so a waiter
        # never misses an attempt that finishes before it waits.
        def start_attempt
          attempt = Attempt.new(@clock.now)
          @attempt = attempt
          resource = attempt.resource = @build.call
          resource.on_unusable = method(:connection_unusable)
          publish(current_state)
          attempt.task = ::Async::Task.new(@lifetime, finished: false) { run_attempt(attempt) }
          attempt.task.run
          attempt
        end

        def run_attempt(attempt)
          resource = attempt.resource
          outcome = ::Async::Promise.new
          setup = ::Async::Task.new(@lifetime, finished: false) do
            @setup.call(resource)
            outcome.resolve(nil)
          rescue StandardError => e
            outcome.resolve(e)
          ensure
            outcome.resolve(IOError.new(STOPPED))
          end
          resource.task = setup
          setup.run
          error = wait_setup(outcome, Internal.now + @connect_timeout)
          connection = resource.connection
          # A connection that died before registration never becomes ready.
          error ||= IOError.new("HTTP/2 connection closed before readiness") unless connection && usable?(connection)
          return abandon(attempt) if @closing

          error ? fail_attempt(attempt, error) : succeed(attempt, connection)
        rescue ::Async::TimeoutError => e
          @closing ? abandon(attempt) : fail_attempt(attempt, e)
        rescue ::Async::Cancel
          abandon(attempt)
          raise
        end

        def wait_setup(outcome, expires)
          loop do
            remaining = expires - Internal.now
            raise ::Async::TimeoutError, "HTTP/2 connection setup timed out" if remaining <= 0

            begin
              return outcome.wait(timeout: [remaining, MAX_WAIT].min)
            rescue ::Async::TimeoutError
              raise if Internal.now >= expires
            end
          end
        end

        def succeed(attempt, connection)
          @connections[connection] = 0
          @failures = 0
          attempt.outcome = :ready
          @attempt = nil
          publish(current_state)
          emit(:attempt_finished, outcome: :ready, duration: @clock.now - attempt.started_at)
        ensure
          @progress.signal
        end

        def fail_attempt(attempt, error)
          # Raw-first abort; this can wait for the setup task to retire.
          @abort_setup.call(attempt.resource)
          return abandon(attempt) if @closing

          @failures += 1
          attempt.error = error
          attempt.outcome = classify(error)
          @attempt = nil
          arm_timer if @policy.backoff
          publish(current_state)
          emit(:attempt_finished, outcome: attempt.outcome, duration: @clock.now - attempt.started_at)
        ensure
          @progress.signal
        end

        # Close won: no registration, timer or state transition.
        def abandon(attempt)
          @abort_setup.call(attempt.resource)
        ensure
          attempt.error ||= Error.new(:unavailable, CLOSING)
          attempt.outcome ||= :closed
          @attempt = nil if @attempt.equal?(attempt)
          @progress.signal
        end

        def classify(error)
          case error
          when Errno::ECONNREFUSED then :refused
          when ::Async::TimeoutError, Errno::ETIMEDOUT then :timeout
          when OpenSSL::SSL::SSLError then :tls
          when ::Protocol::HTTP2::Error then :protocol
          when EOFError, Errno::ECONNRESET, Errno::EPIPE, IOError then :closed
          else :other
          end
        end

        def arm_timer
          backoff = @policy.backoff
          delay = [backoff.base * (backoff.multiplier.to_f**(@failures - 1)), backoff.max].min *
                  (1 - (@random.rand * backoff.jitter))
          @retry_at = @clock.now + delay
          timer = ::Async::Task.new(@lifetime, finished: false) do
            @clock.sleep(delay)
            if @timer.equal?(timer)
              @timer = @retry_at = nil
              reconcile
            end
          end
          @timer = timer
          timer.run
        end

        def publish(state)
          previous = @state
          return if previous == state

          @state = state
          emit(:state_changed, previous_state: previous)
        end

        # Observers run synchronously after bookkeeping and must return quickly.
        def emit(type, previous_state: nil, outcome: nil, duration: nil)
          return unless @observer

          event = ConnectionEvent.new(
            type: type, state: @state, previous_state: previous_state, outcome: outcome, duration: duration,
            active: @connections.each_key.count { |connection| usable?(connection) },
            draining: @connections.each_key.count { |connection| !usable?(connection) },
            pending_calls: @waiters.size, pending_bytes: @pending_bytes
          )
          dispatching = @dispatching
          @dispatching = Fiber.current
          begin
            @observer.call(event)
          rescue StandardError
            nil
          ensure
            @dispatching = dispatching
          end
        end
      end
    end
  end
end
