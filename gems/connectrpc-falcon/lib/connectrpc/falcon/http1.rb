# frozen_string_literal: true

require "English"
require_relative "completion_join"

require "async/http/protocol/http1/server"

module ConnectRPC
  module Falcon
    # The pinned HTTP/1 writer flushes framing after body EOF, before close
    # callbacks. Track that state without inspecting application payload bytes.
    class HTTP1ResponseBody < ::Protocol::HTTP::Body::Wrapper
      attr_reader :eof

      def read
        chunk = super
        @eof = true if chunk.nil?
        chunk
      end

      def empty?
        empty = super
        @eof = true if empty
        empty
      end
    end
    private_constant :HTTP1ResponseBody

    class HTTP1Connection < ::Async::HTTP::Protocol::HTTP1::Server
      attr_accessor :resource
      attr_writer :rejected_input, :retiring_raw

      def close_read
        # The framing parser can close input itself before raising EOFError.
        @rejected_input&.abort_read
        @retiring_raw.close if @retiring_raw && !@retiring_raw.closed?
        super
      end

      def next_request
        return if @resource.draining || @resource.finished?

        request = super
        if request
          fields = request.headers.header
          if fields.any? do |key, value|
            key.casecmp?("content-type") && value.downcase.start_with?("application/grpc-web")
          end &&
             fields.sum { |key, value| key.bytesize + value.bytesize + 32 } > GRPCProtocol::FIELD_LIMIT
            raise Error.new(:resource_exhausted, "Web field block exceeds byte limit"), cause: nil
          end
        end
        # Retirement can happen while the upstream parser is blocked.
        return if @resource.draining || @resource.finished?

        @resource.active = true if request
        @resource.changed
        request
      end

      def each(...)
        super
      ensure
        # Header failures and cancellation can bypass write_body entirely.
        begin
          @resource.exchange&.finish(false, $ERROR_INFO)
        ensure
          @resource.exchange = nil
          begin
            @resource.input_retirement&.close(false)
          ensure
            @resource.input_retirement = nil
            @resource.failed! if @resource.active
            @resource.active = false
            @resource.changed
          end
        end
      end

      # Match the pinned writer's positional arguments.
      # rubocop:disable-next Style/OptionalBooleanParameter
      def write_body(version, body, head = false, trailer = nil)
        if @resource.input_retirement
          @response_eof = head || body.nil?
          body = @response_body = HTTP1ResponseBody.new(body) if body
        end
        super
        written = true
      rescue StandardError
        @resource.failed!
        raise
      ensure
        @response_body = nil
        @response_eof = false
        # Upstream returns after final fixed/chunked bytes AND body callbacks.
        begin
          @resource.exchange&.finish(!!written, $ERROR_INFO)
        ensure
          begin
            @resource.input_retirement&.close(!!written)
          ensure
            @resource.input_retirement = nil
            @resource.exchange = nil
            @resource.active = false
            @resource.changed
          end
        end
      end

      private

      def flush_stream
        result = super
        if @response_eof || @response_body&.eof
          @resource.input_retirement&.final_flushed
          @response_eof = false
          @response_body = nil
        end
        result
      end
    end
    private_constant :HTTP1Connection

    # Connect-only transfer from the original processor to the connection task.
    # Framing completion never relies on empty?, which also becomes true on error.
    class HTTP1StreamInput
      attr_writer :input

      def initialize(body, raw:, connection:)
        @body = body
        @framed = body&.body
        @raw = raw
        @connection = connection
      end

      def on_close(&completion)
        @completion = completion
      end

      def framed_complete?
        case @framed
        when nil then true
        when ::Protocol::HTTP1::Body::Fixed then @framed.remaining.zero?
        when ::Protocol::HTTP1::Body::Chunked then !@framed.length.nil?
        else false
        end
      end

      def head_resolved(status)
        @retiring = status == 505 || !framed_complete?
        @connection.persistent = false if @retiring
      end

      def processor_finished
        @discarded = @input&.release_remainder || 0
        @input = nil
        @body&.close if framed_complete?
        @processed = true
      end

      def final_flushed
        return if @close_expires

        @close_expires = now + 0.5
      end

      def close(delivered)
        return if @closed

        @closed = true
        begin
          if delivered && @processed && @retiring
            retire if @close_expires
          elsif !delivered || !@processed
            abort_read
          end
        rescue StandardError
          # Hygiene cannot change an already observed RPC delivery outcome.
          abort_read
        ensure
          begin
            # A retired connection is never reused, even after complete framing.
            abort_read if @retiring || !delivered || !@processed
          ensure
            begin
              @body&.close
            ensure
              @completion&.call
            end
          end
        end
      end

      private

      def retire
        # HTTP completion is already observed. TLS cleanup must not precede
        # this raw TCP half-close or extend the one final-flush budget.
        @raw.shutdown(Socket::SHUT_WR)
        begin
          remaining = @close_expires - now
          ::Async::Task.current.with_timeout(remaining) { discard } if remaining.positive?
        rescue StandardError
          # A late framing/timeout failure cannot revise completed delivery.
          nil
        end
        remaining = @close_expires - now
        ::Async::Task.current.sleep(remaining) if remaining.positive? && !@raw.closed?
      end

      def discard
        # Chunked EOF closes the buffered/TLS read side before raising. Guard
        # only post-delivery reads, without recursively closing the connection.
        @connection.retiring_raw = @raw
        expires = now + 0.1
        until framed_complete?
          return if 262_144 - @discarded < 65_536

          remaining = expires - now
          return if remaining <= 0

          # Pinned framed reads have no length argument and return <=64 KiB.
          # Reserve a whole block before each read, including adapter remainder.
          chunk = ::Async::Task.current.with_timeout(remaining) { @body.read }
          @discarded += chunk.bytesize if chunk
        end
      ensure
        @connection.retiring_raw = nil
      end

      def abort_read
        @raw.close unless @raw.closed?
        # Finishable.close must still resolve after raw-first teardown. Closing
        # connection state first prevents framed close_read on an already shut IO.
        @connection.close
      rescue IOError, SystemCallError
        nil
      end

      def now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
    private_constant :HTTP1StreamInput

    # One request's original framing and deadline, owned only by native Web H1.
    class HTTP1RejectedInput
      def initialize(body, raw:, connection:, headers:)
        @body = body
        @raw = raw
        @connection = connection
        # Capture before processing. Invalid fields remain core's validation
        # responsibility and must not change its validation order.
        @deadline = GRPCProtocol.request_deadline(headers)
      rescue Error
        @deadline = nil
      end

      def close(error = $ERROR_INFO)
        return if @closed

        @closed = true
        begin
          @connection.rejected_input = self
          raise error if error

          if @body && !@body.empty?
            @expires = now + 0.1
            @expires = [@expires, now + @deadline.remaining].min if @deadline
            until @body.empty?
              remaining = @expires - now
              raise ::Async::TimeoutError, "Rejected Web input did not finish" if remaining <= 0

              # Pinned Fixed/Chunked reads are <=64 KiB and preserve framing,
              # including a core input adapter's discarded readahead remainder.
              ::Async::Task.current.with_timeout(remaining) { @body.read }
            end
          end
          finished = true
        ensure
          # Cancellation and malformed/expired input must fail raw TCP before
          # Finishable/body or TLS cleanup can yield. Never signal RPC cancellation.
          begin
            abort_read unless finished
          ensure
            begin
              @body&.close
            ensure
              @connection.rejected_input = nil
            end
          end
        end
      end

      def abort_read
        @raw.close unless @raw.closed?
      end

      private

      def now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
    private_constant :HTTP1RejectedInput

    # Serial HTTP/1 ownership. It deliberately has no HTTP/2 stream/GOAWAY API.
    class HTTP1Resource
      attr_accessor :raw, :socket, :task, :snapshot, :ready, :draining, :active, :exchange, :input_retirement
      attr_reader :connection, :generation, :progress, :activity_at

      def initialize(stream, progress:, generation:)
        @connection = HTTP1Connection.new(stream, "HTTP/1.1")
        @connection.resource = self
        @shared_progress = progress
        @progress = ::Async::Condition.new
        @generation = generation
        @tasks = []
        @retirement_members = {}
        @cancelled_roots = {}
        @activity_at = now
        @active = @ready = @draining = @aborted = @finishing = false
      end

      def changed
        @activity_at = now
        @progress.signal
        @shared_progress.signal
      end

      def track(task)
        raise IOError, "Connection is retiring" if @aborted

        @tasks.reject!(&:finished?)
        @tasks << task
        task
      end

      def empty? = !@active && @tasks.all?(&:finished?)
      def finished? = @aborted
      def finishing? = @finishing || @draining

      def failed!
        @failed = true
      end

      def web_input(body, headers:)
        HTTP1RejectedInput.new(body, raw: @raw, connection: @connection, headers: headers)
      end

      def prepare_stream_input(body)
        @input_retirement = HTTP1StreamInput.new(body, raw: @raw, connection: @connection)
      end

      def abort
        return if @cleanup&.current?

        capture_retirement
        owned = owns_task?(::Async::Task.current)
        launch = !@cleanup
        if launch
          @aborted = true
          @cleanup = ::Async::Task.new(@lifetime, transient: true, finished: false) { retire }
        end
        if owned
          @cleanup.run if launch
        else
          join(@cleanup, failure: true) { @cleanup.run if launch }
        end
        nil
      end

      def drain(parent, expires)
        return wait(@drain_task, expires) if @drain_task

        if @aborted
          abort
          return true
        end

        @draining = true
        unless @ready
          abort
          return true
        end
        @drain_task = parent.async(finished: false) do
          @progress.wait until empty? || @aborted
          next false if @aborted || @failed

          @finishing = true
          # Cancel an idle parser only after the admitted response and callback
          # complete. Raw half-close preserves the HTTP wire-completion boundary.
          @raw.shutdown(Socket::SHUT_WR)
          cancel_root(@task)
          join(@task)
          loop { @raw.readpartial(16_384) }
        rescue EOFError
          true
        rescue StandardError
          false
        ensure
          abort
        end
        wait(@drain_task, expires)
      rescue ::Async::TimeoutError
        abort
        false
      end

      private

      def now = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      def wait(task, expires)
        remaining = expires - now
        raise ::Async::TimeoutError if remaining <= 0

        ::Async::Task.current.with_timeout(remaining) { task.wait }
      end

      def retirement_parent=(parent)
        @lifetime = parent
      end

      def retirement_completed? = !!@retirement_completed

      def retirement_roots = [*@tasks, @task].compact.uniq

      def capture_retirement
        retirement_roots.each { |task| capture_task(task) }
        members = @retirement_members.keys
        members.each { |task| capture_task(task) }
      end

      def capture_task(task)
        @retirement_members[task] = true
        task.children&.to_a&.each { |child| capture_task(child) unless child.transient? }
      end

      def owns_task?(task)
        capture_retirement
        while task
          return true if @retirement_members.key?(task)
          return false if task.transient?

          task = task.parent
        end
        false
      end

      def cancel_root(task)
        return if !task || @cancelled_roots.key?(task) || task.finished?

        mark_cancelled(task)
        task.cancel
        # Pinned HTTP/1 defers cancellation through response/body callbacks.
        # A deferred request still needs its one immediate forced delivery.
        task.cancel if task.cancel_deferred?
      end

      def mark_cancelled(task)
        @cancelled_roots[task] = true
        task.children&.to_a&.each { |child| mark_cancelled(child) unless child.transient? }
      end

      def retire
        @raw.close unless @raw.closed?
        # Best-effort scheduling hint only. A return proves no unwind or
        # completion; the retained-task joins below establish retirement.
        ::Async::Task.current.yield
        loop do
          capture_retirement
          roots = retirement_roots
          roots.reject! do |task|
            ancestor = task
            covered = false
            until ancestor.transient? || !(ancestor = ancestor.parent)
              covered ||= roots.include?(ancestor)
            end
            covered
          end
          roots.each { |task| cancel_root(task) }
          @retirement_members.keys.reverse_each { |task| join(task) }
          capture_retirement
          break if @retirement_members.keys.all?(&:finished?)
        end
        begin
          @connection.close
        rescue StandardError
          nil
        end
        @retirement_completed = true
        changed
      end

      # Local counterpart of the private Async helper; no shared public utility.
      def join(task, failure: false)
        return unless task

        CompletionJoin.call(task, failure: failure) { yield if block_given? }
      end
    end
    private_constant :HTTP1Resource
  end
end
