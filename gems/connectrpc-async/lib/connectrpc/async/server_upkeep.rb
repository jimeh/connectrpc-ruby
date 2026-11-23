# frozen_string_literal: true

module ConnectRPC
  module Async
    module Internal
      # Bounds a server control frame's own write, once it holds the frame
      # guard, and the too_many_pings GOAWAY. A small control frame write
      # finishes without yielding unless the send buffer is full, so a reactor
      # stall alone cannot expire it, unlike the client CLEANUP_SECONDS.
      CONTROL_WRITE_SECONDS = 0.1

      # Writes one server connection's control frames (PING ACKs, GOAWAYs,
      # drain PINGs, refusals, connection credit and receive-window growth)
      # off its reader, so the reader never blocks
      # writing its own replies. Frames queue without yielding and are written
      # in order. Each must be written within `timeout` of being queued,
      # including any wait behind another writer's frame, and its own write,
      # once it holds the frame guard, within CONTROL_WRITE_SECONDS. Otherwise
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
          @stall = WriteWatchdog.new(resource, clock: clock, timeout: CONTROL_WRITE_SECONDS,
                                               message: "HTTP/2 control frame write stalled")
          @step = [timeout, writes&.timeout].compact.min
          # The frame being delivered, if any: its number and when the watch
          # first checks it.
          @serial = 0
          @check_at = nil
          @delivering = ::Async::Condition.new
          @stopped = false
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
        # writer stopped or failed first, or the queue was full. A frame for a
        # writer that already stopped resolves false at once.
        def write(frame)
          written = ::Async::Promise.new
          if @stopped
            written.resolve(false)
          elsif @queue.size >= MAX_PENDING
            written.resolve(false)
            @resource.abort(IOError.new("HTTP/2 control frames outran their writer"))
          else
            deadline = @clock.now + @timeout
            @queued << deadline
            @queue.enqueue([frame, deadline, written])
          end
          written
        end

        # Whether the writer stopped, dropping its queued frames. A stopped
        # writer never restarts.
        def stopped? = @stopped

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
          @stopped = true
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
      # or header block, not to a whole response.
      #
      # A write blocked on a full send buffer finishes only once the kernel
      # takes the rest of its bytes, after the peer drained a large part of
      # the buffer: about 950 KB on Linux with the default 4 MiB `tcp_wmem`
      # maximum. A slow but live reader could miss a fixed deadline. With
      # `progress`, on Linux, a write still in progress at its deadline is
      # renewed for another `timeout` when the socket's TCP_INFO shows the
      # peer acknowledged new data since the previous sample while unsent
      # data was queued. The connection is then aborted one to two bounds
      # after the later of the peer's last such progress and the blocked
      # write's start, like grpc-go's TCP_USER_TIMEOUT, which any progress
      # resets. Elsewhere, or without a sample, each write keeps a fixed
      # deadline.
      #
      # Samples are taken only by this task, when it first sees a write in
      # progress and at that write's deadline, and by `remaining` once the
      # deadline has passed. Starting or finishing a write never samples.
      class WriteWatchdog
        # Linux's struct tcp_info is read only on Linux: FreeBSD also defines
        # TCP_INFO, with a different layout.
        TCP_INFO = (::Socket::TCP_INFO if RUBY_PLATFORM.include?("linux") && defined?(::Socket::TCP_INFO))
        # Offsets in <linux/tcp.h>'s struct tcp_info: tcpi_bytes_acked (u64,
        # Linux 4.1+) and tcpi_notsent_bytes (u32, Linux 4.6+). Older kernels
        # return a shorter struct, which gives no sample.
        BYTES_ACKED_OFFSET = 120
        NOTSENT_BYTES_OFFSET = 144
        TCP_INFO_MINIMUM = NOTSENT_BYTES_OFFSET + 4

        attr_reader :timeout

        def initialize(resource, clock:, timeout:, message: "HTTP/2 frame write did not finish in time",
                       progress: false)
          @resource = resource
          @clock = clock
          @timeout = timeout
          @message = message
          @progress = progress
          @started = ::Async::Condition.new
          @serial = 0
          @deadline = nil
          @acked = nil
        end

        # A control task: it outlives drains until the final GOAWAY is
        # written, and retirement captures and joins it.
        def start(parent)
          task = ::Async::Task.new(parent, finished: false) { run }
          @resource.maintain(task, control: true)
          task.run
          task
        end

        # Called for every frame write; neither yields nor samples.
        def write_started
          @serial += 1
          @deadline = @clock.now + @timeout
          @started.signal
        end

        def write_finished
          @deadline = nil
        end

        # Seconds left before the frame write in progress, if any, misses its
        # bound. A deadline that has passed is renewed first if the peer made
        # progress; only the watchdog task aborts.
        def remaining
          return unless @deadline

          left = @deadline - @clock.now
          left.positive? ? left : renew - @clock.now
        end

        private

        def run
          armed = nil
          loop do
            @started.wait until @deadline
            unless armed == @serial
              armed = @serial
              @deadline = [@deadline, @clock.now + @timeout].max if progressed?
            end
            left = remaining
            next @clock.sleep(left) if left.positive?

            break @resource.abort(IOError.new(@message))
          end
        end

        # Renews a passed deadline if the peer made progress; returns it.
        def renew
          @deadline = @clock.now + @timeout if progressed?
          @deadline
        end

        # Whether the peer acknowledged new data since the previous sample
        # while unsent data is queued, so the peer is what holds the write.
        # Every sample becomes the next one's baseline.
        def progressed?
          return false unless @progress && (sample = tcp_info)

          acked, unsent = sample
          previous = @acked
          @acked = acked
          !previous.nil? && acked > previous && unsent.positive?
        end

        # [tcpi_bytes_acked, tcpi_notsent_bytes], or nil without a sample,
        # such as on a closed or non-TCP socket, or a hosting SPI socket
        # without getsockopt. The struct is in native byte order.
        def tcp_info
          raw = @resource.raw
          return unless TCP_INFO && raw.respond_to?(:getsockopt)

          data = raw.getsockopt(::Socket::IPPROTO_TCP, TCP_INFO).data
          return if data.bytesize < TCP_INFO_MINIMUM

          [data.unpack1("Q", offset: BYTES_ACKED_OFFSET), data.unpack1("L", offset: NOTSENT_BYTES_OFFSET)]
        rescue SystemCallError, IOError
          nil
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
          @watchdog = WriteWatchdog.new(resource, clock: clock, timeout: write_timeout, progress: true) if write_timeout
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

        # Connection credit and the receive-window estimator's WINDOW_UPDATE,
        # SETTINGS and PING frames, queued in order without yielding.
        def send_receive_window(frames) = frames.each { |frame| @writer.write(frame) }

        # Whether the control writer stopped. Frames queued to it are dropped.
        def writer_stopped? = @writer.stopped?

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
        # It must be written within CONTROL_WRITE_SECONDS of this decision.
        # Its supervisor keeps that bound and the EOF bound below even when a
        # session drain takes over and stops ordinary upkeep.
        def violate
          @violated = true
          @cutoff ||= @resource.connection.remote_stream_id
          written = @writer.write(goaway(@cutoff, ::Protocol::HTTP2::Error::ENHANCE_YOUR_CALM, "too_many_pings"))
          maintain(supervisor: true) do |task|
            timer = ::Async::Task.new(task, finished: false) do
              @clock.sleep(CONTROL_WRITE_SECONDS)
              @resource.abort(IOError.new("HTTP/2 too_many_pings GOAWAY was not written"))
            end
            timer.run
            sent = written.wait
            @cutoff_written.resolve(sent)
            # A GOAWAY a stopped writer refused or dropped still ends the
            # connection at this bound, unless a drain already wrote its final
            # GOAWAY and owns the connection; then the EOF bound below applies.
            next timer.wait unless sent || @resource.finishing?

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
