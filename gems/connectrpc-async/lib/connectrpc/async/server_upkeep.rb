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
      # the connection fails raw-first.
      class ControlWriter
        def initialize(resource, clock:, timeout:)
          @resource = resource
          @clock = clock
          @timeout = timeout
          @queue = ::Async::Queue.new
        end

        def start(parent)
          task = ::Async::Task.new(parent, finished: false) { run }
          @resource.maintain(task, control: true)
          task.run
          task
        end

        # The promise resolves true once the frame is written, or false if the
        # writer stopped or failed first.
        def write(frame)
          written = ::Async::Promise.new
          @queue.enqueue([frame, @clock.now + @timeout, written])
          written
        end

        private

        def run
          written = nil
          loop do
            frame, deadline, written = @queue.dequeue
            deliver(frame, deadline)
            written.resolve(true)
          end
        rescue StandardError => e
          @resource.abort(e)
        ensure
          written&.resolve(false)
          @queue.dequeue.last.resolve(false) until @queue.empty?
        end

        # Earlier frames meet earlier deadlines, so time spent queued behind
        # them is already within this frame's bound. Both watchdogs stop
        # before the guard is released, because release hands control to the
        # next waiter before this task resumes.
        def deliver(frame, deadline)
          connection = @resource.connection
          queued = watchdog(deadline - @clock.now, "HTTP/2 control frame was not written in time")
          connection.synchronize do
            stalled = watchdog(CLEANUP_SECONDS, "HTTP/2 control frame write stalled")
            begin
              connection.write_frame(frame)
            ensure
              Internal.cancel(stalled)
              Internal.cancel(queued)
            end
          end
        ensure
          Internal.cancel(queued)
        end

        def watchdog(seconds, message)
          timer = ::Async::Task.new(::Async::Task.current, finished: false) do
            @clock.sleep(seconds) if seconds.positive?
            @resource.abort(IOError.new(message))
          end
          timer.run
          timer
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

      # Server-side upkeep of one HTTP/2 connection: answering and policing
      # client PINGs, server keepalive PINGs, and the graceful idle and age
      # drains. Every timer uses the session's clock.
      #
      # A drain mirrors grpc-go: a heads-up GOAWAY(2^31-1) and a drain PING,
      # then, on that PING's ACK or after 5 seconds, a cutoff GOAWAY naming the
      # highest stream seen. Streams opened after the cutoff are refused with
      # REFUSED_STREAM; earlier ones dispatch normally. When the accepted work
      # finishes, the connection half-closes and discards input until EOF.
      class ServerUpkeep
        DRAIN_PING = [1, 6, 1, 8, 0, 3, 3, 9].pack("C*").freeze
        MAX_STREAM_ID = 0x7fff_ffff
        ACK_SECONDS = 5
        EOF_SECONDS = 1
        # ServerKeepalive's default timeout, bounding control frames when only
        # idle_timeout drains the connection.
        CONTROL_SECONDS = 20

        attr_reader :cutoff

        def initialize(resource, parent:, clock:, random:, keepalive:, idle_timeout:)
          @resource = resource
          @parent = parent
          @clock = clock
          @random = random
          @keepalive = keepalive
          @idle_timeout = idle_timeout
          @writer = ControlWriter.new(resource, clock: clock, timeout: keepalive&.timeout || CONTROL_SECONDS)
          if keepalive&.min_time
            @strikes = PingStrikes.new(clock: clock, min_time: keepalive.min_time,
                                       permit_without_calls: keepalive.permit_without_calls)
          end
          @idle_since = clock.now
          @cutoff = nil
          @drain = nil
          @acknowledged = nil
          @violated = false
        end

        # Starting each task can yield. A connection that starts retiring or
        # draining meanwhile, after its upkeep was stopped, gets no more.
        def start
          @writer.start(@parent)
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

        # Idle time counts from when the last stream and handler finished, not
        # from other frames, so PINGs cannot extend it.
        def stream_opened = @idle_since = nil

        def refuse?(stream_id) = !@cutoff.nil? && stream_id > @cutoff

        # The stream already decoded its header block; only its reset is sent.
        def refuse(stream)
          frame = ::Protocol::HTTP2::ResetStreamFrame.new(stream.id)
          frame.pack(::Protocol::HTTP2::Error::REFUSED_STREAM)
          @writer.write(frame)
          stream.close!(::Protocol::HTTP2::Error::REFUSED_STREAM)
        end

        def drain(reason)
          return if @drain || @violated || @resource.finished? || @resource.finishing?

          @drain = maintain { run_drain(reason) }
        end

        private

        def upkeep? = !@resource.finished? && !@resource.finishing? && !@resource.draining

        def maintain(&block)
          task = ::Async::Task.new(@parent, finished: false) { |current| block.call(current) }
          @resource.maintain(task)
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
          @writer.write(ping)
          timer = ::Async::Task.new(::Async::Task.current, finished: false) do
            @clock.sleep(ACK_SECONDS)
            acknowledged.resolve(false)
          end
          timer.run
          acknowledged.wait
          Internal.cancel(timer)
          @cutoff ||= @resource.connection.remote_stream_id
          return unless @writer.write(goaway(@cutoff, ::Protocol::HTTP2::Error::NO_ERROR, reason)).wait

          @resource.progress.wait until @resource.empty?
          @resource.quiesce(@parent, clock: @clock, within: EOF_SECONDS)
        ensure
          Internal.cancel(timer)
        end

        # Like grpc-go, the GOAWAY ends the connection even while streams run.
        # It must be written within the cleanup allowance of this decision.
        def violate
          @violated = true
          @cutoff ||= @resource.connection.remote_stream_id
          written = @writer.write(goaway(@cutoff, ::Protocol::HTTP2::Error::ENHANCE_YOUR_CALM, "too_many_pings"))
          maintain do |task|
            timer = ::Async::Task.new(task, finished: false) do
              @clock.sleep(CLEANUP_SECONDS)
              @resource.abort(IOError.new("HTTP/2 too_many_pings GOAWAY was not written"))
            end
            timer.run
            next unless written.wait

            Internal.cancel(timer)
            next if @resource.quiesce(@parent, clock: @clock, within: EOF_SECONDS)

            # An existing drain owns the connection under its own grace; the
            # violation still ends it within the EOF bound.
            @clock.sleep(EOF_SECONDS)
            @resource.abort(IOError.new("HTTP/2 connection ended after too_many_pings"))
          ensure
            Internal.cancel(timer)
          end
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
