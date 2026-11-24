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
      def initialize(body, head:, retirement:)
        super(body)
        @head = head
        @retirement = retirement
      end

      # The writer rescues only StandardError, so cancellation during the final
      # flush closes with a nil error. Grace may start, but write_body raises
      # and the owner is closed undelivered, which ignores grace.
      def close(error = nil)
        retirement = @retirement
        @retirement = nil
        retirement.final_flushed if retirement && error.nil? && (@eof || @head)
      ensure
        super
      end

      # Output drained during the input read comes first. A head published
      # before any output could not wait for the input remainder, so it is
      # read once the first output chunk, or EOF, shows processing has
      # stopped reading input.
      def read
        chunk = @retirement&.prefetched? ? @retirement.next_prefetched : super
        chunk = @retirement.read_first(chunk) if @retirement
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
      attr_reader :request_fields, :request_expires

      def close_read
        # Forced retirement has failed raw TCP and closes it, the TLS socket
        # and this stream once every task has left them, so only the read
        # state ends here.
        return end_read if @resource&.send(:raw_failed?)

        super
      end

      def next_request
        return if @resource.draining || @resource.finished?

        request = super
        @request_fields = request&.headers&.header
        # The Connect deadline for an unread remainder starts with the request.
        @request_expires = HTTP1StreamInput.connect_expires(@request_fields)
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

        @resource.input_retirement&.read_first
        super
      end

      def write_interim_response(version, status, headers, reason = nil)
        raise Unsupported, "Protocol switching is unsupported" if Integer(status, exception: false) == 101

        super
      end

      def each(task: ::Async::Task.current)
        super do |request, connection|
          response = yield(request, connection)
          if (retirement = @resource.input_retirement)
            retirement.response_body = response&.body
          elsif response && request.body
            @resource.prepare_unary_input(request, response.status)
          end
          response
        end
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
          body = HTTP1ResponseBody.new(body, head: head, retirement: retirement)
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

    # Unread HTTP/1 input and the connection's retirement after the reply,
    # for Connect streaming and unary input, which the connection task reads,
    # and rejected Web input, which its processor reads before releasing the
    # response. Framing completion never relies on empty?, which also becomes
    # true on error.
    class HTTP1StreamInput
      # net/http's maxPostHandlerReadBytes.
      READ_LIMIT = 262_144

      attr_writer :input, :response_body

      # Core rejects an invalid timeout itself; only a valid one bounds the read.
      def self.connect_expires(fields)
        timeouts = (fields || []).filter_map { |key, value| value if key.casecmp?("connect-timeout-ms") }
        return unless timeouts.size == 1 && /\A[0-9]{1,10}\z/.match?(timeouts.first) && timeouts.first.to_i.positive?

        Process.clock_gettime(Process::CLOCK_MONOTONIC) + (timeouts.first.to_i / 1000.0)
      end

      # A reusable connection stays open when the read reaches EOF within
      # the limit, as net/http keeps it; Connect streaming never reuses one.
      def initialize(body, raw:, connection:, resource: nil, fields: nil, request_body_timeout: nil, expires: nil,
                     reuse: false)
        @body = body
        @framed = body&.body
        @raw = raw
        @connection = connection
        @resource = resource
        @request_body_timeout = request_body_timeout
        @expires = expires
        @reuse = reuse
        @discarded = 0
        # net/http never reads the body of an Expect: 100-continue request it
        # has not answered, and this server never sends 100 Continue.
        @expect_continue = (fields || []).any? do |key, value|
          key.casecmp?("expect") && value.strip.casecmp?("100-continue")
        end
      end

      def on_close(&completion)
        @completion = completion
      end

      def self.complete?(framed)
        case framed
        when nil then true
        when ::Protocol::HTTP1::Body::Fixed then framed.remaining.zero?
        when ::Protocol::HTTP1::Body::Chunked then !framed.length.nil?
        else false
        end
      end

      def framed_complete? = self.class.complete?(@framed)

      def head_resolved(status)
        @retiring = status == 505 || !framed_complete?
        # A bidi 505 peer may wait for the response before sending more, as
        # net/http leaves a full-duplex body unread.
        @read_pending = @retiring && status != 505 && !@expect_continue
        # A reusable connection decides after its read.
        @connection.persistent = false if @retiring && !(@reuse && @read_pending)
      end

      def retiring? = !!@retiring

      def processor_finished
        @input&.release_remainder
        @input = nil
        @body&.close if framed_complete?
        @processed = true
      end

      # Reads the unread remainder before the response, as net/http does
      # before writing its head, so a client that finishes its upload before
      # reading neither deadlocks nor fails a late write on close. For Connect
      # input only the connection task reads here, once the original processor
      # has stopped reading: it has finished, or it has produced response
      # output, which core does only after a handler's last input read. A head published
      # without output, such as early client-stream headers, is written
      # first, and the read waits for the first output chunk or EOF, which
      # is returned unchanged.
      def read_first(output = nil)
        return output unless @read_pending && (@processed || output || output_queued?)

        @read_pending = false
        if read_needed?
          @processed ? read_remainder : drain_output { read_remainder }
        end
        settle_reuse if @reuse
        output
      end

      def prefetched? = !!@prefetched&.any?

      def next_prefetched = @prefetched.shift

      def final_flushed
        return if @close_expires

        @close_expires = now + 0.5
      end

      # Whether the delivered response's post-flush wait is all that remains.
      def lingering? = !!@lingering

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

      # Like net/http's rstAvoidanceDelay, the half-closed connection waits
      # without reading, so later input stays unread until the close.
      def retire
        # HTTP completion is already observed. TLS cleanup must not precede
        # this raw TCP half-close or extend the one final-flush budget.
        @raw.shutdown(Socket::SHUT_WR)
        remaining = @close_expires - now
        return unless remaining.positive? && !@raw.closed? && !forced?

        # A drain counts the exchange as finished from here.
        @lingering = true
        @resource&.changed
        ::Async::Task.current.sleep(remaining)
      end

      # A declared remainder at or above the limit is not read, as on net/http.
      def read_needed?
        return false if forced? || framed_complete?
        return false if @framed.is_a?(::Protocol::HTTP1::Body::Fixed) && @framed.remaining >= READ_LIMIT

        !(@expires && @expires <= now)
      end

      # request_body_timeout bounds each read wait and the Connect deadline
      # bounds the whole read, as ReadTimeout does for net/http. Reads stop
      # once they reach the limit. A chunked read returns at most 64 KiB, so
      # the last one can pass the limit by less than that.
      def read_remainder
        until framed_complete? || @discarded >= READ_LIMIT
          wait = @request_body_timeout
          if @expires
            remaining = @expires - now
            return if remaining <= 0

            wait = wait ? [wait, remaining].min : remaining
          end
          chunk = wait ? ::Async::Task.current.with_timeout(wait) { @body.read } : @body.read
          return unless chunk

          @discarded += chunk.bytesize
        end
      rescue StandardError
        # A stalled, expired or malformed remainder still receives the
        # response, and the connection retires after it, as on net/http.
        nil
      end

      # Like net/http, a reusable connection stays open only when the read
      # reached EOF; otherwise its head says it closes after the reply.
      def settle_reuse
        if framed_complete?
          @retiring = false
        else
          @connection.persistent = false
        end
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

      # The processor's output write blocks until its queued fragment is
      # read, and its RPC deadline would then fail it mid-message. Drain that
      # output into memory while the input read runs. Once processing has
      # stopped reading input, its remaining output is the response message
      # and terminal it already holds: H1 serves no bidi, and core reads a
      # server stream's input to EOF before its handler runs, so only a
      # client stream's single result or an error terminal can remain. The drain reads Async's OutputBody
      # itself, so it never waits for processing completion or lifecycle
      # observers; the wrappers above it only observe close. A failed output
      # raises again from the response body's next live read.
      def drain_output
        source = output_body
        return yield unless source

        @prefetched ||= []
        drain = ::Async::Task.current.async do
          while (chunk = source.read)
            @prefetched << chunk
          end
        rescue StandardError
          nil
        end
        begin
          yield
        ensure
          # A pending OutputBody#read takes no fragment until it wakes.
          drain.stop
        end
      end

      def output_body
        body = @response_body
        body = body.body while body.is_a?(::Protocol::HTTP::Body::Wrapper)
        body if body.respond_to?(:queued_bytes)
      end

      # Async's OutputBody holds one fragment until the connection task reads it.
      def output_queued? = !!output_body&.queued_bytes&.positive?

      def now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
    private_constant :HTTP1StreamInput

    # One request's original framing and deadline, owned only by native Web H1.
    # Its processor reads the unread remainder as Connect input does, before
    # releasing the staged terminal and rejection head.
    class HTTP1RejectedInput
      def initialize(body, raw:, connection:, headers:, resource: nil, fields: nil, request_body_timeout: nil)
        @body = body
        @raw = raw
        @connection = connection
        @resource = resource
        @fields = fields
        @request_body_timeout = request_body_timeout
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
          raise error if error

          if @body && (retirement = read_remainder)
            # A retiring connection's owner closes the body after the reply.
            @resource.input_retirement = retained = retirement
          end
          finished = true
        ensure
          # Cancellation and owner retirement must fail raw TCP before
          # Finishable/body or TLS cleanup can yield. Never signal RPC
          # cancellation.
          begin
            abort_read unless finished
          ensure
            @body&.close unless retained
          end
        end
      end

      # Forced retirement has already failed raw TCP and closes it once every
      # task has left it.
      def abort_read
        @raw.close unless @raw.closed? || @resource&.send(:raw_failed?)
      end

      private

      # protocol-http1's Fixed/Chunked reads are <=64 KiB and preserve framing,
      # including a core input adapter's discarded readahead remainder.
      # Returns the owner that retires the connection after the reply when
      # the remainder stays unfinished.
      def read_remainder
        expires = now + @deadline.remaining if @deadline
        input = HTTP1StreamInput.new(@body, raw: @raw, connection: @connection, resource: @resource, fields: @fields,
                                            request_body_timeout: @request_body_timeout, expires: expires,
                                            reuse: true)
        input.processor_finished
        input.head_resolved(nil)
        input.read_first
        raise IOError, "Connection retired" if @resource&.send(:raw_failed?)

        input if input.retiring?
      end

      def now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
    private_constant :HTTP1RejectedInput

    # Serial HTTP/1 ownership. It deliberately has no HTTP/2 stream/GOAWAY API.
    class HTTP1Resource
      attr_accessor :raw, :socket, :task, :snapshot, :ready, :draining, :active, :exchange, :input_retirement
      attr_reader :connection, :generation, :progress, :activity_at

      def initialize(stream, progress:, generation:, request_body_timeout: nil, unary_route: nil)
        @request_body_timeout = request_body_timeout
        @unary_route = unary_route
        @connection = HTTP1Connection.new(stream, "HTTP/1.1")
        @connection.resource = self
        @shared_progress = progress
        @progress = ::Async::Condition.new
        @generation = generation
        @tasks = []
        @retirement_members = {}
        @cancelled_roots = {}
        @forced_roots = {}
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
        HTTP1RejectedInput.new(body, raw: @raw, connection: @connection, headers: headers, resource: self,
                                     fields: @connection.request_fields,
                                     request_body_timeout: @request_body_timeout)
      end

      def prepare_stream_input(body)
        @input_retirement = HTTP1StreamInput.new(body, raw: @raw, connection: @connection, resource: self,
                                                       fields: @connection.request_fields,
                                                       request_body_timeout: @request_body_timeout,
                                                       expires: @connection.request_expires)
      end

      # A Connect unary response is whole once processing finishes. Its
      # unfinished input is read before the head, as net/http does after a
      # handler returns, and the connection stays reusable only if the read
      # reaches EOF within the limit.
      def prepare_unary_input(request, status)
        return if HTTP1StreamInput.complete?(request.body.body) || !@unary_route&.call(request)

        input = HTTP1StreamInput.new(request.body, raw: @raw, connection: @connection, resource: self,
                                                   fields: @connection.request_fields,
                                                   request_body_timeout: @request_body_timeout,
                                                   expires: @connection.request_expires, reuse: true)
        input.processor_finished
        input.head_resolved(status)
        @input_retirement = input
      end

      # An interruptible caller raises an interruption at once and leaves
      # retirement to the cleanup task.
      def abort(interruptible: false)
        return if @cleanup&.current?

        capture_retirement
        owned = owns_task?(::Async::Task.current)
        launch = !@cleanup
        if launch
          @aborted = true
          # The cleanup task reports its own failure; no joiner raises it.
          @cleanup = ::Async::Task.new(@lifetime, transient: true, finished: false) { Teardown.run { retire } }
        end
        if owned
          @cleanup.run if launch
        else
          join(@cleanup, interruptible: interruptible) { @cleanup.run if launch }
        end
        nil
      end

      # A caller gets true when the exchange finished by its own `expires`,
      # however the peer leaves afterwards or the connection then retires.
      # A delivered response counts as finished during its post-flush wait.
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
          @progress.wait until exchange_finished? || @aborted
          next false if @aborted || @failed

          @streams_finished_at = now unless @raw_failed
          @progress.wait until empty? || @aborted
          next !@streams_finished_at.nil? if @aborted

          @finishing = true
          # Cancel an idle parser only after the admitted response and callback
          # complete. Raw half-close preserves the HTTP wire-completion boundary.
          @raw.shutdown(Socket::SHUT_WR)
          cancel_root(@task)
          join(@task)
          discard
        rescue EOFError
          # A local raw failure also surfaces as EOF.
          !@streams_finished_at.nil? || !@raw_failed
        rescue StandardError
          !@streams_finished_at.nil?
        ensure
          abort
        end
        wait(@drain_task, expires)
      rescue ::Async::TimeoutError
        # Decided before the abort, whose forced teardown ends the exchange.
        finished = !@streams_finished_at.nil? && @streams_finished_at <= expires
        abort
        finished
      end

      private

      def now = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      def exchange_finished?
        @tasks.all?(&:finished?) && (!@active || !!@input_retirement&.lingering?)
      end

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
        force_deferred(task)
      end

      # Pinned HTTP/1 defers cancellation through response/body callbacks.
      # A deferred request still needs its one immediate forced delivery.
      def force_deferred(task)
        return if @forced_roots.key?(task) || task.finished? || !task.cancel_deferred?

        @forced_roots[task] = true
        task.cancel
      end

      def mark_cancelled(task)
        @cancelled_roots[task] = true
        task.children&.to_a&.each { |child| mark_cancelled(child) unless child.transient? }
      end

      def retire
        # Failing raw TCP precedes any protocol, buffered stream or TLS
        # disposal.
        fail_raw
        # An interruption of this cleanup task is retained at every step below
        # and skips no later one, so retirement still completes and publishes
        # before it is raised.
        # Best-effort scheduling hint only. A return proves no unwind or
        # completion; the retained-task joins below establish retirement.
        escapes = [Teardown.capture { ::Async::Task.current.yield }]
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
          roots.each do |task|
            next unless (interruption = Teardown.capture { cancel_root(task) })

            # Async delivers a cancel by transferring to the task, so this
            # interruption surfaced after delivery and needs no new cancel.
            # It can still skip forcing a deferred request.
            escapes.push(interruption, Teardown.capture { force_deferred(task) })
          end
          # A join retains an interruption until its member has settled. A
          # join's own failure, such as joining the current task, still
          # escapes: retrying that member would never end.
          @retirement_members.keys.reverse_each do |task|
            join(task)
          rescue StandardError
            raise
          rescue Exception => e # rubocop:disable Lint/RescueException -- the member has settled; retirement continues
            escapes << e
          end
          capture_retirement
          # A settled member can stay unfinished, with no signal, until Async
          # clears its fiber; finished? here would spin without yielding.
          break if @retirement_members.keys.all? { |task| CompletionJoin.settled?(task) }
        end
        # A drain's discard read is not a member, but the shutdown ends it
        # with EOF. The stream and TLS socket below close raw too, so every
        # close waits for that read to leave raw.
        escapes << Teardown.capture { @progress.wait } while @discarding
        # A failed or interrupted disposal step, the final raw close included,
        # is reported or retained and skips neither a later one nor
        # retirement's completion.
        escapes << Teardown.capture { @connection.close }
        escapes << Teardown.capture { @socket&.close }
        escapes << Teardown.capture { @raw.close if @raw && !@raw.closed? }
        @retirement_completed = true
        changed
        Teardown.finish(escapes)
      ensure
        # A retirement that ends early, such as on a failed join, still
        # releases the descriptor, although a fiber still blocked on raw may
        # then get a late close interrupt.
        close_raw
      end

      def close_raw
        @raw.close if @raw && !@raw.closed?
      rescue IOError, SystemCallError
        nil
      end

      # Local counterpart of the private Async helper; no shared public utility.
      def join(task, interruptible: false)
        return unless task

        CompletionJoin.call(task, interruptible: interruptible) { yield if block_given? }
      end
    end
    private_constant :HTTP1Resource
  end
end
