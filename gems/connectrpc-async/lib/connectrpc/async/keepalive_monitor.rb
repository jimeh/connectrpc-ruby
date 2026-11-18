# frozen_string_literal: true

module ConnectRPC
  module Async
    module Internal
      # PING liveness for one HTTP/2 connection, run as a resource maintenance
      # task. Only the ACK carrying the outstanding probe's payload proves the
      # peer alive; other frames only defer the next probe, so a peer that
      # writes but never reads cannot pass. `always` monitors without work in
      # progress; otherwise the monitor is dormant until the resource is busy.
      class KeepaliveMonitor
        REASON = :keepalive_timeout

        attr_reader :time

        def initialize(resource, clock:, time:, timeout:, always:)
          @resource = resource
          @clock = clock
          @time = time
          @timeout = timeout
          @always = always
          @read_at = clock.now
          @wake = ::Async::Condition.new
          @sequence = 0
          @probe = nil
        end

        def start(parent)
          task = ::Async::Task.new(parent, finished: false) { run }
          @resource.maintain(task)
          task.run
          task
        end

        # Called by the reader before a frame is applied, so a synchronously
        # blocked reply to that frame cannot hide its receipt.
        def read!
          @read_at = @clock.now
        end

        # The ACK counts only if it was read before the deadline, even when the
        # deadline's timer has not run yet.
        def acknowledged(payload)
          expected, deadline, outcome = @probe
          outcome.resolve(:acknowledged) if outcome && expected == payload && @read_at < deadline
        end

        def wake = @wake.signal

        private

        def run
          loop do
            @wake.wait until @always || @resource.busy?
            remaining = @read_at + @time - @clock.now
            next @clock.sleep(remaining) if remaining.positive?
            break unless probe
          end
        end

        # The deadline is fixed before the writer competes for the frame guard.
        # The supervisor waits outside the guard; expiry or a failed write
        # records the reason, then aborts raw-first, which also ends a writer
        # stuck behind or inside another frame. After a timely ACK the writer
        # is left to finish as a child of this task, which retirement still
        # captures; a later probe queues behind it under its own deadline. A
        # write that finds the peer gone during a drain stops the monitor and,
        # as for control frames, leaves the decision to the reader.
        def probe
          @sequence += 1
          payload = [@sequence].pack("Q>")
          deadline = @clock.now + @timeout
          outcome = ::Async::Promise.new
          @probe = [payload, deadline, outcome]
          parent = ::Async::Task.current
          failure = nil
          writer = ::Async::Task.new(parent, finished: false) do
            @resource.connection.send_ping(payload)
          rescue StandardError => e
            failure = e
            outcome.resolve(:failed)
          end
          timer = ::Async::Task.new(parent, finished: false) do
            remaining = deadline - @clock.now
            @clock.sleep(remaining) if remaining.positive?
            outcome.resolve(:expired)
          end
          writer.run
          timer.run
          result = outcome.wait
          return true if result == :acknowledged
          return false if result == :failed && @resource.outlives_peer?(failure)

          @resource.record_close(REASON)
          @resource.abort(IOError.new("HTTP/2 keepalive PING was not acknowledged"))
          false
        ensure
          @probe = nil
          Internal.cancel(timer)
        end
      end
    end
  end
end
