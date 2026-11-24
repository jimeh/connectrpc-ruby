# frozen_string_literal: true

module ConnectRPC
  module Async
    # Connect server streaming and native gRPC unary/server streaming for
    # ordinary threads. The caller's stream block stays on its thread; the
    # owner's reactor runs the real exchange and serves one command at a time.
    class ThreadedTransport
      RETIRED = "HTTP/2 exchange is retired"
      FAILED = "HTTP/2 transport failed"
      ABANDONED = "Stream abandoned"
      private_constant :RETIRED, :FAILED, :ABANDONED

      # One stream's mailbox. Admission, settle and release follow Call; the
      # command queue, reply slot and flags change only under the admission lock.
      # +shared_deadline+ is the deadline bound every wait shares once an
      # operation ended past the deadline without the owner's answer and
      # failed with deadline_exceeded.
      class Stream < Call
        attr_reader :grpc, :commands
        attr_accessor :seq, :reply, :busy, :broken, :exited, :aborted, :finished, :shared_deadline

        def initialize(request, source, proxy, grpc)
          super(request, 0, source, proxy)
          @grpc = grpc
          @commands = Thread::Queue.new
          # Sequence 0 is the reactor's open reply.
          @seq = 0
          @reply = nil
          @busy = @broken = @exited = @aborted = @finished = false
          @shared_deadline = nil
        end
      end
      private_constant :Stream

      # Forwards reactor lifecycle use only while the caller is blocked on this
      # stream, so it never races the caller's own use of the handle.
      class StreamLifecycle < LifecycleProxy
        def initialize(handle)
          super
          @open = false
        end

        def resume = gate(true)
        def suspend = gate(false)

        def activate(&)
          handle = @lock.synchronize { @handle if @open }
          handle ? handle.activate(&) : yield
        end

        def record_failure(error)
          @lock.synchronize { @handle.record_failure(error) if @open && @handle }
          nil
        end

        def finish(**)
          @lock.synchronize { @handle.finish(**) if @open && @handle }
          nil
        end

        private

        def gate(open)
          @lock.synchronize { @open = open }
          nil
        end
      end
      private_constant :StreamLifecycle

      # The caller's exchange. Each operation posts one command and waits for
      # its reply; abort only enqueues, because core calls it while unwinding.
      class StreamProxy
        def initialize(front, stream, request)
          @front = front
          @stream = stream
          @request = request
        end

        # A cached head is returned only while the exchange is still usable.
        def response_head(timeout: nil)
          @front.send(:check_usable, @stream, @request) if @response_head
          head(timeout)
        end

        # The head and the read each get their own timeout, as on the engine.
        # The read operation itself checks usability.
        def read(length, timeout: nil)
          head(timeout)
          operate(:read, length, timeout: timeout)
        end

        def write(bytes)
          operate(:write, bytes.dup.freeze)
          nil
        end

        def close_send
          operate(:close_send)
          nil
        end

        def finish(timeout: nil)
          operate(:finish, timeout: timeout)
          nil
        end

        def abort = @front.send(:abort_stream, @stream)

        private

        def head(timeout)
          return @response_head if @response_head

          @response_head = operate(:response_head, timeout: timeout)
        end

        def operate(name, *arguments, timeout: nil)
          @front.send(:stream_operation, @stream, @request, [name, arguments, timeout])
        end
      end
      private_constant :StreamProxy

      class GRPCStreamProxy < StreamProxy
        def response_trailers = operate(:response_trailers)
      end
      private_constant :GRPCStreamProxy

      class Engine
        private

        def serve_stream(stream)
          outcome = nil
          begin
            unless @admission.lock.synchronize { stream.caller_done || stream.reactor_done }
              if stream.grpc
                @inner.open_grpc_stream(stream.request) { |exchange| relay(stream, exchange) }
              else
                @inner.open_stream(stream.request) { |exchange| relay(stream, exchange) }
              end
              outcome = [:result, nil]
            end
          rescue StandardError => e
            outcome = [:error, e]
          ensure
            outcome ||= [:error, Error.new(:unavailable, STOPPED)]
            transaction { settle(stream, outcome) }
          end
        end

        # Serves the caller's commands until it exits normally or breaks the
        # stream. A break raises so the engine resets the stream.
        def relay(stream, exchange)
          post(stream, 0, [:ok, nil])
          loop do
            case (command = stream.commands.pop)
            when :exit then return nil
            when :abort then exchange.abort
            when :cancel
              exchange.abort
              raise Error.new(:canceled, ABANDONED), cause: nil
            else
              seq, name, arguments, timeout = command
              post(stream, seq, forward(exchange, name, arguments, timeout))
            end
          end
        end

        def forward(exchange, name, arguments, timeout)
          value = case name
                  when :response_head then caller_head(exchange.response_head(timeout: timeout))
                  when :read then exchange.read(arguments.first, timeout: timeout)
                  when :write then exchange.write(arguments.first)
                  when :close_send then exchange.close_send
                  when :finish then exchange.finish(timeout: timeout)
                  else
                    trailers = exchange.response_trailers
                    trailers && GRPCProtocol.snapshot(trailers)
                  end
          [:ok, value]
        rescue StandardError => e
          [:error, e]
        end

        # Header maps cross as frozen copies; a GRPCHead is already one.
        def caller_head(head)
          return head unless head.is_a?(StreamHead)

          StreamHead.new(status: head.status, headers: GRPCProtocol.snapshot(head.headers))
        end

        # A reply for a command the caller no longer waits on is dropped.
        def post(stream, seq, reply)
          transaction do
            next if stream.broken || stream.caller_done || stream.seq != seq

            stream.reply = [seq, reply]
            stream.signal.broadcast
          end
        end
      end

      private

      def perform_stream(request, grpc:)
        raise ArgumentError, "Request URL must match the transport origin" unless same_origin?(request.url)

        check_request!(request)
        stream = prepare_stream(request, grpc)
        caller_mask(:never) do
          stream.proxy&.resume
          admit(stream)
          registration = nil
          begin
            registration = request.cancellation&.on_cancel { cancel_requested(stream) if owned?(stream) }
            await_open(stream, request)
            exchange = (grpc ? GRPCStreamProxy : StreamProxy).new(self, stream, request)
            begin
              # On an ordinary thread the block runs interruptible even inside a
              # caller's never-interrupt region, while this method's bookkeeping
              # stays masked.
              value = caller_mask(:immediate) { yield exchange }
            rescue Error
              # Only a remote error core raises after finish waits; a Timeout or
              # other interrupt keeps the no-wait path below.
              exit_after_error(stream, request) if owned?(stream)
              raise
            end
            raise Error.new(:unavailable, RETIRED), cause: nil unless owned?(stream)

            exit_stream(stream, request)
            value
          ensure
            registration&.unregister
            leave_stream(stream)
          end
        end
      end

      # A forked child never touches the parent's stream. Only this caller
      # takes or abandons, so caller_done is stable here. Operations waiting on
      # other threads then wake and observe the retirement.
      def leave_stream(stream)
        return unless owned?(stream)

        unless stream.caller_done
          break_stream(stream)
          abandon(stream)
        end
        stream.engine.admission.lock.synchronize { stream.signal.broadcast }
      end

      # Inherited engine state belongs to the parent process.
      def owned?(stream) = stream.engine.pid == Process.pid

      def prepare_stream(request, grpc)
        headers = GRPCProtocol.snapshot(request.headers)
        # URI#dup shares its component strings with the caller; parse a copy.
        url = URI.parse(request.url.to_s).freeze
        source = CancellationSource.new
        proxy = StreamLifecycle.new(request.lifecycle) if request.lifecycle
        outgoing = StreamRequest.new(url: url, http_method: request.http_method, headers: headers,
                                     deadline: request.deadline, lifecycle: proxy, cancellation: source.token)
        Stream.new(outgoing, source, proxy, grpc)
      end

      # Waits for the reactor to yield the real exchange, so dispatch failures
      # raise before the block, as on Transport.
      def await_open(stream, request)
        status, = await_stream(stream, request, seq: 0)
        raise_outcome(take(stream), request) if status == :result
        expired!(request, status) unless status == :reply
        stream.proxy&.suspend
      end

      # Posts the terminal command with the lifecycle gate open for the engine's
      # late cleanup reporting, then takes the engine's actual outcome. A broken
      # stream already has :cancel queued and never reports success.
      def exit_stream(stream, request)
        post_exit(stream)
        status, = await_stream(stream, request)
        expired!(request, status) unless status == :result
        broken = stream.broken
        tag, error = take(stream)
        check_request!(request)
        raise error if tag == :error
        raise Error.new(:canceled, ABANDONED), cause: nil if broken
      end

      # Opens the gate under the admission lock, so an operation finishing on
      # another thread either suspends before the exit or sees it and leaves
      # the gate to terminal cleanup.
      def post_exit(stream)
        stream.engine.admission.lock.synchronize do
          stream.proxy&.resume
          stream.exited = true
          stream.commands.push(:exit) unless stream.broken
        end
      end

      # Core raises a remote error after the stream finished, so the block
      # exits with it. The caller waits for the engine's outcome, bounded by the
      # cleanup allowance, so admission and late delivery facts are settled
      # before the original error returns. On expiry the ensure breaks and
      # abandons the stream; the engine's outcome itself is never raised.
      def exit_after_error(stream, request)
        return unless stream.engine.admission.lock.synchronize { stream.finished && !stream.broken }

        post_exit(stream)
        take(stream) if await_stream(stream, request, timeout: 0).first == :result
      end

      def check_usable(stream, request)
        retired = !owned?(stream) ||
                  stream.engine.admission.lock.synchronize { stream.broken || stream.exited || stream.caller_done }
        return unless retired

        check_request!(request)
        raise Error.new(:unavailable, RETIRED), cause: nil
      end

      def stream_operation(stream, request, command)
        raise Error.new(:unavailable, RETIRED), cause: nil unless owned?(stream)

        check_owner!
        caller_mask(:never) do
          seq = begin_operation(stream, request, command)
          answered = false
          begin
            status, reply = await_stream(stream, request, seq: seq, timeout: command.last)
            answered = %i[reply result retired].include?(status)
            expire_operation(stream, request, status, reply) unless answered
            raise_outcome(settled(stream), request) if status == :result
            if status == :retired
              check_request!(request)
              raise Error.new(:unavailable, RETIRED), cause: nil
            end

            tag, value = reply
            if tag == :error
              check_request!(request)
              raise value
            end
            stream.engine.admission.lock.synchronize { stream.finished = true } if command.first == :finish

            value
          ensure
            stream.engine.admission.lock.synchronize do
              stream.busy = false
              suspend_operation(stream)
            end
            break_stream(stream) unless answered
          end
        end
      end

      # Raises an unanswered operation's expiry. One read of core's arbiter,
      # on the deadline's own clock and outside the admission lock, picks the
      # error; when that is deadline_exceeded the stream first shares the
      # operation's deadline bound, +bound+. A clock failure while reading
      # the arbiter propagates and shares nothing.
      def expire_operation(stream, request, status, bound)
        error = expiry_error(request, status)
        if bound && error.code == "deadline_exceeded"
          stream.engine.admission.lock.synchronize { share_deadline(stream, bound) }
        end
        raise error, cause: nil
      end

      # Settled outcomes are raised outside the lock: arbitration can run
      # cancellation listeners that take it.
      def begin_operation(stream, request, command)
        check_request!(request)
        stream.proxy&.resume
        seq = stream.engine.admission.lock.synchronize do
          raise StreamStateError, "Overlapping stream operation" if stream.busy
          raise Error.new(:unavailable, RETIRED), cause: nil if stream.broken || stream.exited || stream.caller_done
          next if stream.result

          stream.busy = true
          stream.commands.push([stream.seq += 1, *command])
          stream.seq
        end
        raise_outcome(settled(stream), request) unless seq
        seq
      rescue StandardError
        # A concurrent caller's operation may still be in flight with the gate open.
        stream.engine.admission.lock.synchronize { suspend_operation(stream) unless stream.busy }
        raise
      end

      # Called under the admission lock. Once the scope exited, terminal
      # cleanup owns the open gate and only take or abandon detaches it.
      def suspend_operation(stream)
        stream.proxy&.suspend unless stream.exited
      end

      def settled(stream) = stream.engine.admission.lock.synchronize { stream.result }

      # Enqueue-only and idempotent; never waits for a stalled owner.
      def abort_stream(stream)
        return unless owned?(stream)

        stream.engine.admission.lock.synchronize do
          unless stream.aborted || stream.broken || stream.exited || stream.caller_done || stream.result
            stream.aborted = true
            stream.commands.push(:abort)
          end
        end
        nil
      end

      # Fires the interrupt first, so a reactor blocked in an operation wakes
      # and resets the stream; :cancel ends a reactor waiting for a command.
      def break_stream(stream)
        first = stream.engine.admission.lock.synchronize do
          next false if stream.broken

          stream.broken = true
          stream.reply = nil
          true
        end
        return unless first

        stream.source.cancel
        stream.commands.push(:cancel)
        stream.proxy&.detach
      end

      # Waits for the reply tagged +seq+ or the terminal outcome and returns
      # [status, reply], or [:deadline or :timeout, deadline bound] on those
      # expiries, the bound nil without a deadline. A reply is taken in the
      # same critical section that selects it, so a concurrent break cannot
      # withdraw it. Expiry is the
      # deadline or the operation timeout plus the cleanup allowance, or the
      # cancellation allowance; nil bounds wait until a signal or interrupt. An
      # operation also ends once the scope took or abandoned the stream.
      def await_stream(stream, request, seq: nil, timeout: nil)
        now = Internal.now
        sampled = now + request.deadline.remaining + Internal::CLEANUP_SECONDS if request.deadline
        expires = now + timeout + Internal::CLEANUP_SECONDS if timeout
        lock = stream.engine.admission.lock
        lock.synchronize do
          loop do
            if seq && stream.reply&.first == seq
              _, reply = stream.reply
              stream.reply = nil
              return [:reply, reply]
            end
            return [:result] if stream.result
            return [:retired] if seq && stream.caller_done

            now = Internal.now
            deadline = [stream.shared_deadline, sampled].compact.min
            return [:canceled] if stream.cancel_expires && stream.cancel_expires <= now

            return [:deadline, deadline] if deadline && deadline <= now
            return [:timeout, deadline] if expires && expires <= now

            limit = [deadline, expires, stream.cancel_expires].compact.min
            caller_mask(:immediate) do
              stream.signal.wait(lock, limit && [limit - now, Internal::MAX_WAIT].min)
            end
          end
        end
      end

      # Called under the admission lock when an unanswered operation's expiry
      # is deadline_exceeded. Every later or concurrent wait on the stream
      # then ends by the same deadline bound, so a stalled owner costs the
      # stream one allowance past the deadline rather than one per wait.
      def share_deadline(stream, deadline)
        return if stream.shared_deadline

        stream.shared_deadline = deadline
        stream.signal.broadcast
      end

      def expired!(request, status)
        raise expiry_error(request, status), cause: nil
      end

      # An accepted cancellation or deadline keeps its own reason; otherwise
      # the expired bound picks the code. Reads core's arbiter once, as
      # check_request! does.
      def expiry_error(request, status)
        cancellation = request.cancellation
        reason = cancellation ? cancellation.reason : (:deadline_exceeded if request.deadline&.expired?)
        reason ||= :canceled if status == :canceled
        reason ||= :deadline_exceeded if status == :deadline
        return Error.new(:canceled, "Call canceled") if reason == :canceled
        return Error.new(:deadline_exceeded, "Deadline exceeded") if reason == :deadline_exceeded

        Error.new(:unavailable, FAILED)
      end

      # A terminal outcome observed while opening or during an operation.
      def raise_outcome(outcome, request)
        tag, error = outcome
        check_request!(request)
        raise error if tag == :error

        raise Error.new(:unavailable, STOPPED), cause: nil
      end
    end
  end
end
