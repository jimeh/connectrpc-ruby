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
      class Stream < Call
        attr_reader :grpc, :commands
        attr_accessor :seq, :reply, :busy, :broken, :exited, :aborted

        def initialize(request, source, proxy, grpc)
          super(request, 0, source, proxy)
          @grpc = grpc
          @commands = Thread::Queue.new
          # Sequence 0 is the reactor's open reply.
          @seq = 0
          @reply = nil
          @busy = @broken = @exited = @aborted = false
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

        def response_head(timeout: nil)
          @response_head ||= operate(:response_head, timeout: timeout)
        end

        # The head and the read each get their own timeout, as on the engine.
        def read(length, timeout: nil)
          response_head(timeout: timeout)
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
        Thread.handle_interrupt(Object => :never) do
          stream.proxy&.resume
          admit(stream)
          registration = nil
          begin
            registration = request.cancellation&.on_cancel { cancel_requested(stream) }
            await_open(stream, request)
            exchange = (grpc ? GRPCStreamProxy : StreamProxy).new(self, stream, request)
            # The block runs interruptible even inside a caller's never-interrupt
            # region; this method's bookkeeping stays masked.
            value = Thread.handle_interrupt(Object => :immediate) { yield exchange }
            exit_stream(stream, request)
            value
          ensure
            registration&.unregister
            # Only this caller takes or abandons, so caller_done is stable here.
            unless stream.caller_done
              break_stream(stream)
              abandon(stream)
            end
          end
        end
      end

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
        status = await_stream(stream, request, seq: 0)
        raise_outcome(take(stream), request) if status == :result
        expired!(request, status) unless status == :reply
        take_reply(stream)
      end

      # Posts the terminal command with the lifecycle gate open for the engine's
      # late cleanup reporting, then takes the engine's actual outcome. A broken
      # stream already has :cancel queued and never reports success.
      def exit_stream(stream, request)
        stream.proxy&.resume
        stream.engine.admission.lock.synchronize do
          stream.exited = true
          stream.commands.push(:exit) unless stream.broken
        end
        status = await_stream(stream, request)
        expired!(request, status) unless status == :result
        broken = stream.broken
        tag, error = take(stream)
        check_request!(request)
        raise error if tag == :error
        raise Error.new(:canceled, ABANDONED), cause: nil if broken
      end

      def stream_operation(stream, request, command)
        raise Error.new(:unavailable, RETIRED), cause: nil unless stream.engine.pid == Process.pid

        check_owner!
        Thread.handle_interrupt(Object => :never) do
          seq = begin_operation(stream, request, command)
          answered = false
          begin
            status = await_stream(stream, request, seq: seq, timeout: command.last)
            answered = %i[reply result].include?(status)
            expired!(request, status) unless answered
            raise_outcome(settled(stream), request) if status == :result

            tag, value = take_reply(stream)
            raise value if tag == :error

            value
          ensure
            stream.proxy&.suspend
            stream.engine.admission.lock.synchronize { stream.busy = false }
            break_stream(stream) unless answered
          end
        end
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
        stream.proxy&.suspend unless stream.engine.admission.lock.synchronize { stream.busy }
        raise
      end

      def settled(stream) = stream.engine.admission.lock.synchronize { stream.result }

      # Enqueue-only and idempotent; never waits for a stalled owner.
      def abort_stream(stream)
        return unless stream.engine.pid == Process.pid

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

      def take_reply(stream)
        stream.engine.admission.lock.synchronize do
          _, reply = stream.reply
          stream.reply = nil
          reply
        end
      ensure
        stream.proxy&.suspend
      end

      # Waits for the reply tagged +seq+ or the terminal outcome. Expiry is the
      # deadline or the operation timeout plus the cleanup allowance, or the
      # cancellation allowance; nil bounds wait until a signal or interrupt.
      def await_stream(stream, request, seq: nil, timeout: nil)
        now = Internal.now
        deadline = now + request.deadline.remaining + Internal::CLEANUP_SECONDS if request.deadline
        expires = now + timeout + Internal::CLEANUP_SECONDS if timeout
        lock = stream.engine.admission.lock
        lock.synchronize do
          loop do
            return :reply if seq && stream.reply&.first == seq
            return :result if stream.result

            now = Internal.now
            return :canceled if stream.cancel_expires && stream.cancel_expires <= now
            return :deadline if deadline && deadline <= now
            return :timeout if expires && expires <= now

            limit = [deadline, expires, stream.cancel_expires].compact.min
            Thread.handle_interrupt(Object => :immediate) do
              stream.signal.wait(lock, limit && [limit - now, Internal::MAX_WAIT].min)
            end
          end
        end
      end

      # An accepted cancellation or deadline keeps its own reason; otherwise
      # the expired bound picks the code.
      def expired!(request, status)
        check_request!(request)
        raise Error.new(:canceled, "Call canceled"), cause: nil if status == :canceled
        raise Error.new(:deadline_exceeded, "Deadline exceeded"), cause: nil if status == :deadline

        raise Error.new(:unavailable, FAILED), cause: nil
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
