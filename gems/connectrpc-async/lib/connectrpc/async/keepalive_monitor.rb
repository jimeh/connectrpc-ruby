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

        def acknowledged(payload)
          expected, outcome = @probe
          outcome.resolve(:acknowledged) if outcome && expected == payload
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
        # stuck behind or inside another frame.
        def probe
          @sequence += 1
          payload = [@sequence].pack("Q>")
          deadline = @clock.now + @timeout
          outcome = ::Async::Promise.new
          @probe = [payload, outcome]
          parent = ::Async::Task.current
          writer = ::Async::Task.new(parent, finished: false) do
            @resource.connection.send_ping(payload)
          rescue StandardError
            outcome.resolve(:failed)
          end
          timer = ::Async::Task.new(parent, finished: false) do
            remaining = deadline - @clock.now
            @clock.sleep(remaining) if remaining.positive?
            outcome.resolve(:expired)
          end
          writer.run
          timer.run
          if outcome.wait == :acknowledged
            Internal.cancel(timer)
            Internal.join(writer)
            return true
          end

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
