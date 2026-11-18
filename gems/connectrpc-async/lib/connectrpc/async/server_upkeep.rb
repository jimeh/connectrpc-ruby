# frozen_string_literal: true

module ConnectRPC
  module Async
    module Internal
      # Writes one server connection's control frames (PING ACKs, GOAWAYs,
      # drain PINGs and refusals) off its reader, so the reader never blocks
      # writing its own replies. Frames queue without yielding and are written
      # in order. Each must be written within `timeout` of being queued,
      # including any wait behind another writer's frame, and its own write,
      # once it holds the frame guard, within the cleanup allowance. Otherwise
      # the connection fails raw-first. A frame write that `writes` times
      # keeps its own bound: a control frame waiting behind it fails the
      # connection no earlier than that write's deadline. The frames queued
      # behind a frame written late that way waited for the same write, so
      # their deadlines count again from when it was written. At most
      # MAX_PENDING frames wait; a peer that outruns the writer, such as a
      # PING flood with enforcement off, fails the connection rather than
      # growing the queue or blocking the reader.
      #
      # Both bounds are kept by long-lived tasks, so delivering a frame only
      # records and signals state and never yields to arm or disarm a timer.
      # Under CPU starvation each yield costs a reactor iteration, which
      # could outlast a bound such as the too_many_pings GOAWAY's.
      class ControlWriter
        MAX_PENDING = 1024

        def initialize(resource, clock:, timeout:, writes: nil)
          @resource = resource
          @clock = clock
          @timeout = timeout
          @writes = writes
          @queue = ::Async::Queue.new
          # The queued frames' deadlines, oldest first.
          @queued = []
          @resumed = nil
          @stall = WriteWatchdog.new(resource, clock: clock, timeout: CLEANUP_SECONDS,
                                               message: "HTTP/2 control frame write stalled")
          @step = [timeout, writes&.timeout].compact.min
          # The frame being delivered, if any: its number and when the watch
          # first checks it.
          @serial = 0
          @check_at = nil
          @delivering = ::Async::Condition.new
        end

        # Starting each task can yield. A connection that starts retiring
        # meanwhile gets no more tasks.
        def start(parent)
          task = ::Async::Task.new(parent, finished: false) { run }
          @resource.maintain(task, control: true)
          task.run
          unless @resource.finished?
            watcher = ::Async::Task.new(parent, finished: false) { watch }
            @resource.maintain(watcher, control: true)
            watcher.run
          end
          @stall.start(parent) unless @resource.finished?
          task
        end

        # The promise resolves true once the frame is written, or false if the
        # writer stopped or failed first, or the queue was full.
        def write(frame)
          written = ::Async::Promise.new
          if @queue.size >= MAX_PENDING
            written.resolve(false)
            @resource.abort(IOError.new("HTTP/2 control frames outran their writer"))
          else
            deadline = @clock.now + @timeout
            @queued << deadline
            @queue.enqueue([frame, deadline, written])
          end
          written
        end

        private

        # Once the peer left a drain, the drain alone ends the connection, so a
        # failed write stops only this writer. Such a write can still run: a
        # writer already due to run sees the departure's cancellation only
        # after it. A write can also find the peer gone before the reader
        # does. As in grpc-go's loopy writer, that write then stops only this
        # writer, and the reader, which meets the same end after reading what
        # the peer sent, decides whether the drain outlives the peer.
        def run
          written = nil
          loop do
            frame, deadline, written = @queue.dequeue
            @queued.shift
            deadline = [deadline, @resumed + @timeout].max if @resumed
            deliver(frame, deadline)
            now = @clock.now
            @resumed = now if now > deadline
            written.resolve(true)
          end
        rescue StandardError => e
          @resource.abort(e) unless @resource.peer_departed? || @resource.outlives_peer?(e)
        ensure
          written&.resolve(false)
          @queue.dequeue.last.resolve(false) until @queue.empty?
          @queued.clear
        end

        # Earlier frames meet earlier deadlines, so time spent queued behind
        # them is already within this frame's bound, and only the frame being
        # delivered needs watching. A frame written past its deadline waited
        # for a timed write, or its own bounded write crossed the deadline;
        # either way the frames behind it waited too, so `run` restarts their
        # deadlines from that write. Both bounds are disarmed before the guard
        # is released, because release hands control to the next waiter
        # before this task resumes.
        def deliver(frame, deadline)
          @check_at = first_check(deadline)
          @serial += 1
          @delivering.signal
          connection = @resource.connection
          connection.synchronize do
            @stall.write_started
            begin
              connection.write_frame(frame)
            ensure
              @stall.write_finished
              @check_at = nil
            end
          end
        ensure
          @check_at = nil
        end

        # A frame is first checked at its deadline. One dequeued past it is
        # checked now, as the watch would only see it after the write: it
        # fails at once unless it waits out a timed write, which is then
        # checked when that write's bound ends.
        def first_check(deadline)
          now = @clock.now
          return deadline if deadline > now

          remaining = @writes&.remaining
          raise IOError, "HTTP/2 control frame was not written in time" unless remaining&.positive?

          now + remaining
        end

        # Fails the connection when the frame being delivered is still
        # undelivered at its first check and after each timed write still
        # running then.
        def watch
          loop do
            @delivering.wait until @check_at
            serial = @serial
            next unless undelivered_after?(serial, @check_at - @clock.now) && outlasts_writes?(serial)

            break @resource.abort(IOError.new("HTTP/2 control frame was not written in time"))
          end
        end

        # An expired deadline first waits out each frame write that `writes`
        # still times, since that write has its own bound.
        def outlasts_writes?(serial)
          while (remaining = @writes&.remaining)&.positive?
            return false unless undelivered_after?(serial, remaining)
          end
          true
        end

        def delivering?(serial) = @serial == serial && !@check_at.nil?

        # Sleeps up to `seconds` and returns whether the frame `serial` is
        # still being delivered. Writing that frame cannot cut a sleep short,
        # so no step passes the next frame's first check:
        # - a queued frame's deadline still ahead caps the step, and a frame
        #   queued during the step has a deadline at least `timeout` away;
        # - a frame written past its deadline restarts the next one's deadline
        #   `timeout` after that write;
        # - a next frame already past its deadline when dequeued fails at once,
        #   unless it is first checked when a timed write's bound ends. That
        #   write started after this frame was written, so the step, at most
        #   the write bound, ends by then;
        # - a queued frame whose own deadline has passed can still get a later,
        #   restarted one. A frame being delivered before its deadline has one
        #   no later than that, so `left` caps the step; past its deadline, the
        #   cases above apply.
        def undelivered_after?(serial, seconds)
          due = @clock.now + seconds
          while (left = due - (now = @clock.now)).positive?
            queued = @queued.find { |queued_deadline| queued_deadline > now }
            @clock.sleep([left, @step, queued && (queued - now)].compact.min)
            return false unless delivering?(serial)
          end
          delivering?(serial)
        end
      end

      # Bounds each frame write of one server connection, from holding the
      # frame guard until its flush finishes, to `timeout`. A frame write
      # defers cancellation, so a peer that stops reading can only be ended
      # by failing the connection raw-first. The bound applies to each frame
      # or header block, not to a whole response, and continuous progress
      # within one frame does not extend it.
      class WriteWatchdog
        attr_reader :timeout

        def initialize(resource, clock:, timeout:, message: "HTTP/2 frame write did not finish in time")
          @resource = resource
          @clock = clock
          @timeout = timeout
          @message = message
          @started = ::Async::Condition.new
          @serial = 0
          @started_at = nil
        end

        # A control task: it outlives drains until the final GOAWAY is
        # written, and retirement captures and joins it.
        def start(parent)
          task = ::Async::Task.new(parent, finished: false) { run }
          @resource.maintain(task, control: true)
          task.run
          task
        end

        # Called for every frame write; neither yields.
        def write_started
          @serial += 1
          @started_at = @clock.now
          @started.signal
        end

        def write_finished
          @started_at = nil
        end

        # Seconds left before the frame write in progress, if any, misses its
        # bound.
        def remaining = @started_at && (@started_at + @timeout - @clock.now)

        private

        def run
          loop do
            @started.wait until @started_at
            serial = @serial
            remaining = @started_at + @timeout - @clock.now
            @clock.sleep(remaining) if remaining.positive?
            next unless @started_at && @serial == serial

            break @resource.abort(IOError.new(@message))
          end
        end
      end

      # grpc-go's client PING policy. A PING closer than `min_time` to the
      # previous one, or 2 hours without calls unless permitted, is a strike;
      # a server HEADERS or DATA write forgives the next PING and clears the
      # strikes. The third strike is a violation; the first PING never strikes.
      class PingStrikes
        MAX_STRIKES = 2
        WITHOUT_CALLS = 7200

        def initialize(clock:, min_time:, permit_without_calls:)
          @clock = clock
          @min_time = min_time
          @permit_without_calls = permit_without_calls
          @strikes = 0
          @last = nil
          @reset = false
        end

        def reset = @reset = true

        def violation?(calls:)
          now = @clock.now
          last = @last
          @last = now
          if @reset
            @reset = false
            @strikes = 0
            return false
          end
          interval = calls || @permit_without_calls ? @min_time : WITHOUT_CALLS
          @strikes += 1 if last && last + interval > now
          @strikes > MAX_STRIKES
        end
      end

      # Server-side upkeep of one HTTP/2 connection: its control frames and
      # stream refusals, the frame write bound, answering and policing client
      # PINGs, server keepalive PINGs, and the graceful idle, age and close
      # drains. Every timer uses the session's clock.
      #
      # A drain mirrors grpc-go: a heads-up GOAWAY(2^31-1) and a drain PING,
      # then, on that PING's ACK or after 5 seconds, a cutoff GOAWAY naming the
      # highest stream seen. Streams opened after the cutoff are refused with
      # REFUSED_STREAM; earlier ones dispatch normally. When the accepted work
      # finishes, the connection half-closes and discards input until EOF.
      # Session close and TLS retirement take over once the cutoff is written.
      class ServerUpkeep
        DRAIN_PING = [1, 6, 1, 8, 0, 3, 3, 9].pack("C*").freeze
        MAX_STREAM_ID = 0x7fff_ffff
        ACK_SECONDS = 5
        EOF_SECONDS = 1
        # ServerKeepalive's default timeout, bounding control frames without
        # server keepalive.
        CONTROL_SECONDS = 20

        attr_reader :cutoff

        def initialize(resource, parent:, clock:, random:, keepalive:, idle_timeout:, write_timeout: nil)
          @resource = resource
          @parent = parent
          @clock = clock
          @random = random
          @keepalive = keepalive
          @idle_timeout = idle_timeout
          @watchdog = WriteWatchdog.new(resource, clock: clock, timeout: write_timeout) if write_timeout
          @writer = ControlWriter.new(resource, clock: clock, timeout: keepalive&.timeout || CONTROL_SECONDS,
                                                writes: @watchdog)
          if keepalive&.min_time
            @strikes = PingStrikes.new(clock: clock, min_time: keepalive.min_time,
                                       permit_without_calls: keepalive.permit_without_calls)
          end
          @idle_since = clock.now
          @cutoff = nil
          # Resolves true once a cutoff GOAWAY is written, by a drain or a
          # too_many_pings decision, or false if neither can write one.
          @cutoff_written = ::Async::Promise.new
          @drain = nil
          @acknowledged = nil
          @violated = false
        end

        # Starting each task can yield. A connection that starts retiring or
        # draining meanwhile, after its upkeep was stopped, gets no more.
        def start
          @writer.start(@parent)
          @watchdog.start(@parent) if @watchdog && !@resource.finished?
          if (time = @keepalive&.time) && upkeep?
            @resource.keep_alive(@parent, clock: @clock, time: time, timeout: @keepalive.timeout, always: true)
          end
          maintain { track_idle } if @idle_timeout && upkeep?
          maintain { watch_idle } if @idle_timeout && upkeep?
          maintain { watch_age(@keepalive.max_connection_age) } if @keepalive&.max_connection_age && upkeep?
          self
        end

        # Without server keepalive, client PINGs keep the stack's own reply.
        def answers_pings? = !@keepalive.nil?

        # Whether a too_many_pings decision is ending the connection.
        def violated? = @violated

        # A client PING, decided before anything can yield; the reader never
        # writes its ACK or the GOAWAY itself.
        def ping(frame)
          violation = !@violated && @strikes&.violation?(calls: !@resource.connection.streams.empty?)
          @writer.write(frame.acknowledge)
          violate if violation
        end

        def acknowledged(payload)
          @acknowledged&.resolve(true) if payload == DRAIN_PING
        end

        def stream_written = @strikes&.reset

        def write_started = @watchdog&.write_started
        def write_finished = @watchdog&.write_finished

        # Idle time counts from when the last stream and handler finished, not
        # from other frames, so PINGs cannot extend it.
        def stream_opened = @idle_since = nil

        def refuse?(stream_id) = !@cutoff.nil? && stream_id > @cutoff

        # The reader already decoded the refused stream's header block, and
        # created no stream for it; only its reset is sent.
        def refuse(stream_id) = write_reset(stream_id, ::Protocol::HTTP2::Error::REFUSED_STREAM)

        # Resets an open stream without yielding: the stream closes at once
        # and its reset is queued. The promise resolves once it is written.
        def reset(stream, code)
          written = write_reset(stream.id, code)
          stream.close!(code)
          written
        end

        # Starts a drain unless one is running or the connection is already
        # ending. Returns the cutoff promise; a drain that cannot start leaves
        # it to a running drain or too_many_pings decision, or resolves false.
        def drain(reason)
          unless @drain || @violated
            if @resource.finished? || @resource.finishing?
              @cutoff_written.resolve(false)
            else
              @drain = maintain { run_drain(reason) }
            end
          end
          @cutoff_written
        end

        private

        def upkeep? = !@resource.finished? && !@resource.finishing? && !@resource.draining

        def maintain(supervisor: false, &block)
          task = ::Async::Task.new(@parent, finished: false) { |current| block.call(current) }
          @resource.maintain(task, supervisor: supervisor)
          task.run
          task
        end

        # A handler reports its last change before it finishes, so emptiness
        # is checked after each change is delivered.
        def track_idle
          loop do
            @resource.progress.wait
            @idle_since ||= @clock.now if @resource.empty?
          end
        end

        def watch_idle
          remaining = @idle_timeout
          loop do
            @clock.sleep(remaining)
            idle_since = @idle_since
            remaining = idle_since ? idle_since + @idle_timeout - @clock.now : @idle_timeout
            break drain("max_idle") unless remaining.positive?
          end
        end

        # The grace period starts at age expiry, whatever the drain's progress.
        def watch_age(age)
          @clock.sleep(age * (0.9 + (0.2 * @random.rand)))
          drain("max_age")
          grace = @keepalive.max_connection_age_grace
          return unless grace

          @clock.sleep(grace)
          @resource.abort(IOError.new("HTTP/2 connection exceeded its maximum age"))
        end

        def run_drain(reason)
          acknowledged = @acknowledged = ::Async::Promise.new
          @writer.write(goaway(MAX_STREAM_ID, ::Protocol::HTTP2::Error::NO_ERROR, reason))
          ping = ::Protocol::HTTP2::PingFrame.new
          ping.pack(DRAIN_PING)
          # The ACK fallback counts from the PING reaching the wire, so the
          # peer always sees the heads-up before the cutoff is frozen.
          return unless @writer.write(ping).wait

          timer = ::Async::Task.new(::Async::Task.current, finished: false) do
            @clock.sleep(ACK_SECONDS)
            acknowledged.resolve(false)
          end
          timer.run
          acknowledged.wait
          Internal.cancel(timer)
          @cutoff ||= @resource.connection.remote_stream_id
          written = @writer.write(goaway(@cutoff, ::Protocol::HTTP2::Error::NO_ERROR, reason)).wait
          @cutoff_written.resolve(written)
          return unless written

          @resource.progress.wait until @resource.empty?
          @resource.quiesce(@parent, clock: @clock, within: EOF_SECONDS)
        ensure
          Internal.cancel(timer)
          @cutoff_written.resolve(false)
        end

        # Like grpc-go, the GOAWAY ends the connection even while streams run.
        # It must be written within the cleanup allowance of this decision.
        # Its supervisor keeps that bound and the EOF bound below even when a
        # session drain takes over and stops ordinary upkeep.
        def violate
          @violated = true
          @cutoff ||= @resource.connection.remote_stream_id
          written = @writer.write(goaway(@cutoff, ::Protocol::HTTP2::Error::ENHANCE_YOUR_CALM, "too_many_pings"))
          maintain(supervisor: true) do |task|
            timer = ::Async::Task.new(task, finished: false) do
              @clock.sleep(CLEANUP_SECONDS)
              @resource.abort(IOError.new("HTTP/2 too_many_pings GOAWAY was not written"))
            end
            timer.run
            sent = written.wait
            @cutoff_written.resolve(sent)
            next unless sent

            Internal.cancel(timer)
            next if @resource.quiesce(@parent, clock: @clock, within: EOF_SECONDS)

            # An existing drain owns the connection under its own grace; the
            # violation still ends it within the EOF bound.
            @clock.sleep(EOF_SECONDS)
            @resource.abort(IOError.new("HTTP/2 connection ended after too_many_pings"))
          ensure
            Internal.cancel(timer)
            @cutoff_written.resolve(false)
          end
        end

        def write_reset(stream_id, code)
          frame = ::Protocol::HTTP2::ResetStreamFrame.new(stream_id)
          frame.pack(code)
          @writer.write(frame)
        end

        def goaway(last_stream_id, code, debug)
          frame = ::Protocol::HTTP2::GoawayFrame.new
          frame.pack(last_stream_id, code, debug)
          frame
        end
      end
    end
  end
end
