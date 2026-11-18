# frozen_string_literal: true

require_relative "completion_join"

require "async/http/protocol/http1/server"

module ConnectRPC
  module Falcon
    # protocol-http1 v0.37.1+ closes the body only after the response is fully
    # written and flushed ("Defer `body.close` in `write_chunked_body`,
    # `write_fixed_length_body`, and `write_body_and_close`"). A clean close
    # after EOF or HEAD therefore marks the final flush, before close callbacks.
    # EOF tracking never inspects application payload bytes.
    class HTTP1ResponseBody < ::Protocol::HTTP::Body::Wrapper
      def initialize(body, head:, &final_flush)
        super(body)
        @head = head
        @final_flush = final_flush
      end

      # The writer rescues only StandardError, so cancellation during the final
      # flush closes with a nil error. Grace may start, but write_body raises
      # and the owner is closed undelivered, which ignores grace.
      def close(error = nil)
        final_flush = @final_flush
        @final_flush = nil
        final_flush.call if final_flush && error.nil? && (@eof || @head)
      ensure
        super
      end

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
        # Forced retirement has failed raw TCP and closes it, the TLS socket
        # and this stream once every task has left them, so only the read
        # state ends here.
        return end_read if @resource&.send(:raw_failed?)

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

      # This listener never switches protocols, so an Upgrade offer is ignored
      # (RFC 9110 section 7.8). Upstream reads an offering request's body to
      # connection close; frame it normally instead so the response can be
      # sent. The connection still closes afterwards: a proxy that tunnels on
      # the client's offer alone would forward later bytes uninspected.
      def read_request_body(method, headers)
        return super if method == ::Protocol::HTTP::Methods::CONNECT || !headers[::Protocol::HTTP1::UPGRADE]

        @persistent = false
        read_body(headers)
      end

      # Neither status writer may emit 101, whichever route reaches it.
      # RackGuard turns the raise into 501; earlier listener checks keep the
      # server's own 101 responses from getting here.
      def write_response(version, status, headers, reason = nil)
        raise Unsupported, "Protocol switching is unsupported" if Integer(status, exception: false) == 101

        super
      end

      def write_interim_response(version, status, headers, reason = nil)
        raise Unsupported, "Protocol switching is unsupported" if Integer(status, exception: false) == 101

        super
      end

      def each(...)
        super
      rescue Exception => e # rubocop:disable Lint/RescueException -- cancellation also ends the exchange
        error = e
        raise
      ensure
        # Header failures and cancellation can bypass write_body entirely.
        begin
          @resource.exchange&.finish(false, error)
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

      # Match the writer's positional arguments. Unreleased protocol-http1
      # adds a fifth `request` argument (socketry/protocol-http1#65).
      # rubocop:disable-next Style/OptionalBooleanParameter
      def write_body(version, body, head = false, trailer = nil, *rest)
        if (retirement = @resource.input_retirement) && body
          body = HTTP1ResponseBody.new(body, head: head) { retirement.final_flushed }
        end
        super
        # write_empty_body flushes a nil body but has no body to close.
        retirement&.final_flushed unless body
        written = true
      rescue Exception => e # rubocop:disable Lint/RescueException -- cancellation also ends the exchange
        error = e
        @resource.failed! if e.is_a?(StandardError)
        raise
      ensure
        # Upstream returns after final fixed/chunked bytes AND body callbacks.
        # async-http's fail_request writes from its own rescue clause, so the
        # ambient exception is not this response's.
        begin
          @resource.exchange&.finish(!!written, error)
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

      def end_read
        return if @state == :closed

        @persistent = false
        receive_end_stream! if %i[open half_closed_local].include?(@state)
      end
    end
    private_constant :HTTP1Connection

    # Connect-only transfer from the original processor to the connection task.
    # Framing completion never relies on empty?, which also becomes true on error.
    class HTTP1StreamInput
      attr_writer :input

      def initialize(body, raw:, connection:, resource: nil)
        @body = body
        @framed = body&.body
        @raw = raw
        @connection = connection
        @resource = resource
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
        ::Async::Task.current.sleep(remaining) if remaining.positive? && !@raw.closed? && !forced?
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

          # protocol-http1's framed reads have no length argument and return <=64 KiB.
          # Reserve a whole block before each read, including adapter remainder.
          chunk = ::Async::Task.current.with_timeout(remaining) { @body.read }
          @discarded += chunk.bytesize if chunk
        end
      ensure
        @connection.retiring_raw = nil
      end

      # Forced retirement has already failed raw TCP and closes it once every
      # task has left it; the connection's own close_read ends framed input.
      def abort_read
        return if forced?

        @raw.close unless @raw.closed?
        # Finishable.close must still resolve after raw-first teardown. Closing
        # connection state first prevents framed close_read on an already shut IO.
        @connection.close
      rescue IOError, SystemCallError
        nil
      end

      def forced? = !!@resource&.send(:raw_failed?)

      def now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
    private_constant :HTTP1StreamInput

    # One request's original framing and deadline, owned only by native Web H1.
    class HTTP1RejectedInput
      def initialize(body, raw:, connection:, headers:, resource: nil)
        @body = body
        @raw = raw
        @connection = connection
        @resource = resource
        # Capture before processing. Invalid fields remain core's validation
        # responsibility and must not change its validation order.
        @deadline = GRPCProtocol.request_deadline(headers)
      rescue Error
        @deadline = nil
      end

      def close(error = nil)
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

              # protocol-http1's Fixed/Chunked reads are <=64 KiB and preserve framing,
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

      # Forced retirement has already failed raw TCP and closes it once every
      # task has left it.
      def abort_read
        @raw.close unless @raw.closed? || @resource&.send(:raw_failed?)
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
        HTTP1RejectedInput.new(body, raw: @raw, connection: @connection, headers: headers, resource: self)
      end

      def prepare_stream_input(body)
        @input_retirement = HTTP1StreamInput.new(body, raw: @raw, connection: @connection, resource: self)
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
          discard
        rescue EOFError
          # A local raw failure also surfaces as EOF.
          !@raw_failed
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

      # Discards unread input until EOF, so the peer receives the complete
      # response rather than a reset. This read is not a retirement member;
      # retirement closes raw only after it has left raw.
      def discard
        raise EOFError if @raw_failed

        @discarding = true
        begin
          loop { @raw.readpartial(16_384) }
        ensure
          @discarding = false
          changed
        end
      end

      # Fails the peer at once without closing the descriptor, as the HTTP/2
      # resource does: on Ruby 4, IO#close interrupts every fiber blocked on
      # the socket, even after it has left the read. Retirement closes the
      # descriptor once its tasks have all left it.
      def fail_raw
        @raw_failed = true
        @raw.shutdown(Socket::SHUT_RDWR) if @raw && !@raw.closed?
      rescue IOError, SystemCallError
        nil
      end

      def raw_failed? = !!@raw_failed

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
        # Failing raw TCP precedes any protocol, buffered stream or TLS
        # disposal.
        fail_raw
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
        # A drain's discard read is not a member, but the shutdown ends it
        # with EOF. The stream and TLS socket below close raw too, so every
        # close waits for that read to leave raw.
        @progress.wait while @discarding
        begin
          @connection.close
        rescue StandardError
          nil
        ensure
          begin
            @socket&.close
          rescue StandardError
            nil
          end
        end
        close_raw
        @retirement_completed = true
        changed
      ensure
        # An interrupted retirement still releases the descriptor, although
        # a fiber still blocked on raw may then get a late close interrupt.
        close_raw
      end

      def close_raw
        @raw.close if @raw && !@raw.closed?
      rescue IOError, SystemCallError
        nil
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
