# frozen_string_literal: true

module ConnectRPC
  module Async
    module Internal
      # Reactor-owned HTTP/2 client connections for one Transport: stream
      # accounting, one shared attempt at a time, backoff, admission bytes and
      # published state. Attempts run under the transport lifetime, outside any
      # RPC task or lifecycle; waiting calls only observe them.
      #
      # Live sockets never exceed `connection_limit + max_draining`: the
      # active cap counts accounted connections outside the draining set, the
      # attempt in progress and removed connections whose raw socket has not
      # yet failed; the draining set holds at most `max_draining` connections.
      # A failed socket is shut down in both directions; retirement closes its
      # descriptor once the connection's tasks have joined.
      class ConnectionManager
        FULL = "HTTP/2 transport admission is full"
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
          @draining = {}.compare_by_identity
          @reserved = {}.compare_by_identity
          @drain_timers = {}.compare_by_identity
          @waiters = {}.compare_by_identity
          @attempt = nil
          @failures = 0
          @timer = nil
          @retry_at = nil
          @pending_bytes = 0
          @state = :idle
          @closing = false
          @dispatching = nil
          # Active, draining and pending calls as the last emitted event carried them.
          @published_counts = [0, 0, 0]
          @clock = MonotonicClock
          @random = ::Random
          # Raised for later connections when a peer reports too many PINGs.
          @keepalive_time = policy.keepalive&.time
        end

        # Stream usage summed over every accounted connection.
        def usage = @connections.each_value.sum

        def connections = @connections.dup
        def retry_refused? = @policy.retry_refused

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
            raise Error.new(:unavailable, FULL), cause: nil
          end
          @pending_bytes += bytes
          bytes
        end

        def release_bytes(bytes)
          @pending_bytes -= bytes
        end

        # A published count change can yield to the observer, so every pass
        # that publishes rechecks availability before waiting or leasing: a
        # wakeup during the publication is not lost, and no lease is held
        # across it. A published waiter leaves before its lease is taken.
        def acquire(wait_for_ready: @policy.wait_for_ready)
          waiter = Waiter.new(wait_for_ready, nil)
          loop do
            @waiters[waiter] = true
            raise Error.new(:unavailable, CLOSING), cause: nil if @closing

            if (connection = available)
              @waiters.delete(waiter)
              next if @published_counts.last != @waiters.size && publish_counts

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
            next if publish_counts

            @progress.wait
          end
        ensure
          # Only exits without a lease still hold the waiter.
          publish_counts if @waiters.delete(waiter)
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
        # potentially blocking close joins the connection's retirement. The
        # slot stays reserved until the close has failed its raw socket.
        def retire(connection)
          return unless @connections.key?(connection)

          begin
            remove(connection)
            reconcile
          ensure
            connection.close
          end
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
          # Close's own grace now bounds every drain.
          @drain_timers.each_value { |timer| Internal.cancel(timer) }
          @drain_timers.clear
          Internal.cancel(@attempt&.task)
          @progress.signal
        end

        def finish
          close
          @connections.select { |_, usage| usage.zero? }.each_key { |connection| remove(connection) }
          publish(:closed)
          publish_counts
        end

        private

        def available
          @connections.each do |connection, usage|
            return connection if usage < connection.maximum_concurrent_streams && usable?(connection)
          end
          nil
        end

        def usable_connection? = @connections.each_key.any? { |connection| usable?(connection) }
        def slot_free? = @connections.size - @draining.size + @reserved.size + (@attempt ? 1 : 0) < @connection_limit
        def demand? = @waiters.each_key.any? { |waiter| waiter.wait_for_ready || !waiter.joined&.failed? }

        def current_state
          return @state if TERMINAL.include?(@state)
          return :ready if usable_connection?
          return :connecting if @attempt
          return :transient_failure if @retry_at

          :idle
        end

        # Starts a demand-driven attempt when one is allowed and no connection
        # has spare capacity, then republishes. Woken waiters take that capacity.
        def reconcile
          unless @closing
            promote
            start_attempt if !@attempt && !@retry_at && slot_free? && demand? && !available
            publish(current_state)
          end
          publish_counts
          @progress.signal
        end

        # Called on GOAWAY and on resource abort, which fails raw TCP first. A
        # removed connection's reservation ends once its socket has failed.
        def connection_unusable(resource)
          calm(resource)
          connection = resource.connection
          return unless connection

          if @reserved.key?(connection)
            return unless resource.finished?

            @reserved.delete(connection)
            return reconcile
          end
          return unless @connections.key?(connection)

          if @connections[connection].zero?
            begin
              remove(connection)
              reconcile
            ensure
              # Close actively rather than relying on the reader, so the
              # reservation always ends.
              connection.close unless resource.finished?
            end
            return
          end
          if graceful_draining?(connection)
            arm_drain_timer(connection)
            @draining[connection] = true if @draining.size < @policy.max_draining
          end
          reconcile
        end

        # A graceful GOAWAY with streams left to finish. An error GOAWAY
        # closes the protocol connection and a local abort finishes the
        # resource before this notification runs.
        def graceful_draining?(connection)
          !@closing && @connections[connection]&.positive? && connection.goaway_received? &&
            !connection.closed? && !connection.resource.finished?
        end

        # Every removal from accounting ends draining membership and the drain
        # timer, and reserves the slot until the raw socket fails. Outside
        # transport close, it is the one place a ready connection is reported
        # closed, with the first reason its resource recorded.
        def remove(connection)
          @connections.delete(connection)
          @draining.delete(connection)
          Internal.cancel(@drain_timers.delete(connection))
          @reserved[connection] = true unless connection.resource.finished?
          emit(:connection_closed, outcome: connection.resource.close_reason) unless @closing
        end

        # Like grpc-go, a too_many_pings GOAWAY at least doubles the PING
        # interval of later connections relative to the offending one's.
        def calm(resource)
          time = resource.keepalive&.time
          @keepalive_time = [@keepalive_time, 2 * time].max if time && resource.calmed?
        end

        def keep_alive(connection)
          keepalive = @policy.keepalive
          return unless keepalive

          connection.resource.keep_alive(@lifetime, clock: @clock, time: @keepalive_time, timeout: keepalive.timeout,
                                                    always: keepalive.permit_without_calls)
        end

        # A connection that kept its active slot because the draining set was
        # full joins the set once a member leaves it.
        def promote
          @connections.each_key do |connection|
            break if @draining.size >= @policy.max_draining
            next if @draining.key?(connection)

            @draining[connection] = true if graceful_draining?(connection)
          end
        end

        # One timer per draining connection; a repeated GOAWAY does not
        # restart it. Expiry aborts the connection's remaining streams.
        def arm_drain_timer(connection)
          timeout = @policy.drain_timeout
          return if !timeout || @closing || @drain_timers.key?(connection)

          timer = ::Async::Task.new(@lifetime, finished: false) do
            @clock.sleep(timeout)
            if @drain_timers[connection].equal?(timer)
              @drain_timers.delete(connection)
              if @connections.key?(connection)
                connection.resource.record_close(:drain_timeout)
                connection.resource.abort
              end
            end
          end
          @drain_timers[connection] = timer
          timer.run
        end

        # Publishes the attempt before running its eager task, so a waiter
        # never misses an attempt that finishes before it waits. The task
        # exists first: close can cancel it while an observer yields, and an
        # interrupted publisher still runs or abandons it.
        def start_attempt
          attempt = Attempt.new(@clock.now)
          @attempt = attempt
          resource = attempt.resource = @build.call
          resource.on_unusable = method(:connection_unusable)
          task = attempt.task = ::Async::Task.new(@lifetime, finished: false) { run_attempt(attempt) }
          begin
            publish(current_state)
          ensure
            task.finished? ? abandon(attempt) : task.run
          end
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
          Internal.wait_event(outcome, expires)
        rescue Expired
          raise Expired, "HTTP/2 connection setup timed out"
        end

        def succeed(attempt, connection)
          @connections[connection] = 0
          keep_alive(connection)
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

        # Emits counts_changed when active, draining or pending calls differ
        # from the last emitted event's, and returns whether it emitted.
        # Pending bytes change with every call and never trigger it.
        def publish_counts # rubocop:disable Naming/PredicateMethod -- publishes, then reports whether it did
          return false unless @observer && counts != @published_counts

          emit(:counts_changed)
          true
        end

        def counts
          usable = @connections.each_key.count { |connection| usable?(connection) }
          [usable, @connections.size - usable, @waiters.size]
        end

        # Observers run synchronously after bookkeeping and must return quickly.
        def emit(type, previous_state: nil, outcome: nil, duration: nil)
          return unless @observer

          active, draining, pending_calls = @published_counts = counts
          event = ConnectionEvent.new(
            type: type, state: @state, previous_state: previous_state, outcome: outcome, duration: duration,
            active: active, draining: draining, pending_calls: pending_calls, pending_bytes: @pending_bytes
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
