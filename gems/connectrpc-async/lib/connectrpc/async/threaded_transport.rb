# frozen_string_literal: true

require "uri"

module ConnectRPC
  module Async
    # Connect unary and server streaming, and native gRPC unary and server
    # streaming, over the HTTP/2 Transport for ordinary threads. One owner
    # thread per process runs an Async reactor and the inner Transport; callers
    # hand each call to it through a queue and wait on a per-call mailbox.
    class ThreadedTransport
      UNAVAILABLE = "HTTP/2 transport admission is closed or full"
      STOPPED = "HTTP/2 transport owner stopped"
      OWNER = :connectrpc_async_threaded_owner
      # Bounds front retries when each new owner stops before admitting the call.
      START_ATTEMPTS = 3
      private_constant :UNAVAILABLE, :STOPPED, :OWNER, :START_ATTEMPTS

      # Count and byte admission for one process. Engines that replace a failed
      # owner share it, so a stopped engine's mailboxes stay charged.
      class Admission
        attr_reader :pid, :lock
        attr_accessor :count, :bytes, :rejected, :notice

        def initialize(pid)
          @pid = pid
          @lock = Mutex.new
          @count = 0
          @bytes = 0
          @rejected = 0
          @notice = nil
        end
      end
      private_constant :Admission

      # One call's mailbox. The reactor and the caller each finish once; the
      # second to finish releases admission. Guarded by the admission lock.
      class Call
        attr_reader :request, :bytes, :source, :proxy, :signal
        attr_accessor :engine, :result, :reactor_done, :caller_done, :cancel_expires

        def initialize(request, bytes, source, proxy)
          @request = request
          @bytes = bytes
          @source = source
          @proxy = proxy
          @signal = ConditionVariable.new
          @reactor_done = false
          @caller_done = false
        end
      end
      private_constant :Call

      # Forwards the reactor's lifecycle use to the caller's handle until the
      # caller takes or abandons the call. The lock never spans an activation.
      class LifecycleProxy
        def initialize(handle)
          @handle = handle
          @lock = Mutex.new
        end

        def activate(&)
          handle = @lock.synchronize { @handle }
          handle ? handle.activate(&) : yield
        end

        def record_failure(error)
          @lock.synchronize { @handle&.record_failure(error) }
          nil
        end

        def detach
          @lock.synchronize { @handle = nil }
          nil
        end
      end
      private_constant :LifecycleProxy

      # The H2 Transport, given the front's compiled TLS snapshot so the owner
      # never rereads TLS files.
      class Inner < Transport
        # Until Transport takes a snapshot, super builds and discards a default
        # client context for HTTPS (system roots only); user TLS files are never
        # reread.
        def initialize(snapshot:, **)
          super(**)
          @snapshot = snapshot
        end

        # Reports admission rejected by the front through the manager's observer.
        def reject_admission(count)
          count.times do
            @manager.admit(0, full: true)
          rescue Error
            nil
          end
        end
      end
      private_constant :Inner

      # One owner thread, its reactor and inner Transport, for one process.
      class Engine
        attr_reader :pid, :admission, :calls, :thread, :inner
        attr_accessor :state

        def initialize(front, admission, origin:, snapshot:, options:)
          @front = front
          @pid = admission.pid
          @admission = admission
          @origin = origin
          @snapshot = snapshot
          @options = options
          @inbox = Thread::Queue.new
          @calls = {}.compare_by_identity
          @state = :running
          @inner = nil
          @thread = nil
        end

        def start
          @thread = Thread.new { own }
          @thread.name = "connectrpc-async #{@origin.host}:#{@origin.port}"
          self
        end

        # Admission and the inbox push are atomic with engine state, so no call
        # follows the close command and a stopped engine admits nothing.
        def admit(call, max_requests, max_bytes)
          @admission.lock.synchronize do
            return false if @state == :stopped
            raise Error.new(:unavailable, UNAVAILABLE), cause: nil unless @state == :running

            if @admission.count >= max_requests || (max_bytes && @admission.bytes + call.bytes > max_bytes)
              @admission.rejected += 1
              unless @admission.notice
                @admission.notice = self
                @inbox.push(:rejected)
              end
              raise Error.new(:unavailable, UNAVAILABLE), cause: nil
            end
            @admission.count += 1
            @admission.bytes += call.bytes
            call.engine = self
            @calls[call] = true
            @inbox.push(call)
            true
          end
        end

        def request_close(expires)
          @admission.lock.synchronize do
            return false unless @state == :running

            @state = :closing
            @inbox.push([:close, expires])
            true
          end
        end

        # Test hook only: runs a Proc inline on the owner's dispatcher fiber.
        def run_on_owner(command) = @inbox.push(command)

        # Lock held. The first reactor outcome wins; a forced close result
        # already delivered to the caller is kept.
        def settle(call, outcome)
          return if call.reactor_done

          call.reactor_done = true
          if call.caller_done
            release(call)
          else
            call.result ||= outcome
            call.signal.signal
          end
        end

        # Lock held.
        def release(call)
          @admission.count -= 1
          @admission.bytes -= call.bytes
          @calls.delete(call)
        end

        # Wakes every waiting caller of a stalled owner with unavailable and
        # fires each unfinished call's interrupt.
        def force_unavailable
          sources = @admission.lock.synchronize do
            @calls.each_key.filter_map do |call|
              unless call.caller_done || call.result
                call.result = [:error, Error.new(:unavailable, STOPPED)]
                call.signal.signal
              end
              call.source unless call.reactor_done
            end
          end
          sources.each(&:cancel)
        end

        private

        # Owner-side admission and mailbox transactions defer Thread#kill and
        # Thread#raise, also while this fiber waits for the lock, so a dying
        # owner never leaves a call half settled or admission half released.
        # Bodies never block; the reactor's own waits stay interruptible.
        def transaction(&) = Thread.handle_interrupt(Object => :never) { @admission.lock.synchronize(&) }

        def own
          Thread.current.report_on_exception = false
          Thread.current.thread_variable_set(OWNER, @front)
          outcome = nil
          begin
            # The creator's masked region is inherited; the reactor must accept
            # kill and raise so process exit and owner failure stay prompt.
            Thread.handle_interrupt(Object => :immediate) { outcome = run_reactor }
          rescue Exception # rubocop:disable Lint/RescueException -- every owner failure is reported to callers
            nil
          ensure
            finalize(outcome)
          end
        end

        def run_reactor
          Sync do |task|
            @inner = Inner.new(origin: @origin.to_s, snapshot: @snapshot, **@options)
            dispatch(task)
          ensure
            # The reactor returns only after every child finishes. Cancelling the
            # inner lifetime aborts its owned connections.
            task.children&.to_a&.each { |child| Internal.cancel(child) }
          end
        end

        def dispatch(task)
          loop do
            command = @inbox.pop
            case command
            when Stream then task.async(finished: false) { serve_stream(command) }
            when Call then task.async(finished: false) { serve(command) }
            when :rejected then emit_rejections
            when Proc then command.call
            else return @inner.close(grace: [command.last - Internal.now, 0].max)
            end
          end
        end

        def serve(call)
          outcome = nil
          begin
            skip = @admission.lock.synchronize { call.caller_done || call.reactor_done }
            outcome = [:response, @inner.call(call.request)] unless skip
          rescue StandardError => e
            outcome = [:error, e]
          ensure
            outcome ||= [:error, Error.new(:unavailable, STOPPED)]
            transaction { settle(call, outcome) }
          end
        end

        def emit_rejections
          count = transaction do
            @admission.notice = nil
            rejected = @admission.rejected
            @admission.rejected = 0
            rejected
          end
          @inner.reject_admission(count)
        end

        # Owner death must not leave a caller waiting. Engine bookkeeping
        # finishes before the front lock publishes any close outcome, and an
        # interrupt cannot stop either part.
        def finalize(outcome)
          Thread.handle_interrupt(Object => :never) do
            transaction do
              @state = :stopped
              @admission.notice = nil if @admission.notice.equal?(self)
              @calls.dup.each_key { |call| settle(call, [:error, Error.new(:unavailable, STOPPED)]) }
            end
            @front.send(:owner_finished, self, outcome)
          end
        end
      end
      private_constant :Engine

      def initialize(origin:, ca_file: nil, certificate_chain: nil, private_key: nil, connection_limit: 1,
                     max_requests: 128, connect_timeout: 10, connection_policy: ConnectionPolicy.new,
                     connection_observer: nil)
        @origin = parse_origin(origin)
        @max_requests = Internal.positive_integer(max_requests, "max_requests")
        options = {
          connection_limit: Internal.positive_integer(connection_limit, "connection_limit"),
          max_requests: @max_requests, connect_timeout: Internal.seconds(connect_timeout, "connect_timeout")
        }
        unless connection_policy.is_a?(ConnectionPolicy)
          raise ArgumentError, "connection_policy must be a ConnectionPolicy"
        end
        unless connection_observer.nil? || connection_observer.respond_to?(:call)
          raise ArgumentError, "connection_observer must respond to call"
        end
        if @origin.scheme == "http" && (ca_file || certificate_chain || private_key)
          raise ArgumentError, "TLS options require an HTTPS origin"
        end

        if @origin.scheme == "https"
          @snapshot = Internal::TLS.client(ca_file: ca_file, certificate_chain: certificate_chain,
                                           private_key: private_key)
        end
        @max_bytes = connection_policy.max_pending_bytes
        @options = options.merge(connection_policy: connection_policy, connection_observer: connection_observer).freeze
        @lock = Mutex.new
        @closed = ConditionVariable.new
        @state = :open
        @close_outcome = nil
        @close_expires = nil
        @close_pid = nil
        @closing_engine = nil
        @admission = nil
        @engine = nil
      end

      def inspect = "#<#{self.class} state=#{front_state} connection_state=#{connection_state} origin=#{@origin}>"

      # A published observation, not a promise that the next call succeeds.
      def connection_state
        state = front_state
        return state unless state == :open

        engine = @engine
        return :idle unless engine && engine.pid == Process.pid && engine.state != :stopped

        engine.inner&.connection_state || :idle
      end

      def call(request)
        check_owner!
        CallCancellation.with_request(request) { |outgoing| perform(outgoing) }
      end

      def streaming_capabilities = %i[server_stream].freeze

      def open_stream(request, &)
        raise ArgumentError, "A streaming block is required" unless block_given?

        check_owner!
        CallCancellation.with_request(request) { |outgoing| perform_stream(outgoing, grpc: false, &) }
      end

      def grpc_capabilities = %i[unary server_stream].freeze

      def open_grpc_stream(request, &)
        raise ArgumentError, "A gRPC block is required" unless block_given?

        check_owner!
        CallCancellation.with_request(request) { |outgoing| perform_stream(outgoing, grpc: true, &) }
      end

      def close(grace: 5)
        Internal.seconds(grace, "grace", zero: true)
        check_owner!
        expires = Internal.now + grace
        Thread.handle_interrupt(Object => :never) do
          begin_close(expires)
          wait_closed
        end
      end

      private

      def perform(request)
        raise ArgumentError, "Request URL must match the transport origin" unless same_origin?(request.url)

        check_request!(request)
        call = prepare(request)
        Thread.handle_interrupt(Object => :never) do
          admit(call)
          taken = false
          registration = nil
          begin
            registration = request.cancellation&.on_cancel { cancel_requested(call) }
            status = await(call, request)
            if status == :result
              tag, value = take(call)
              taken = true
              check_request!(request)
              raise value if tag == :error

              value
            else
              check_request!(request)
              raise Error.new(:canceled, "Call canceled"), cause: nil if status == :canceled

              raise Error.new(:deadline_exceeded, "Deadline exceeded"), cause: nil
            end
          ensure
            registration&.unregister
            abandon(call) unless taken
          end
        end
      end

      # Only opaque frozen bytes and header maps cross to the owner thread.
      def prepare(request)
        headers = request.headers.to_h do |key, values|
          [key.frozen? ? key : key.dup.freeze, values.map { |value| value.frozen? ? value : value.dup.freeze }.freeze]
        end
        body = request.body
        body = body.dup.freeze unless body.nil? || body.frozen?
        # URI#dup shares its component strings with the caller; parse a copy.
        url = URI.parse(request.url.to_s).freeze
        source = CancellationSource.new
        proxy = LifecycleProxy.new(request.lifecycle) if request.lifecycle
        outgoing = TransportRequest.new(url: url, http_method: request.http_method, headers: headers.freeze,
                                        body: body, response_limit: request.response_limit,
                                        deadline: request.deadline, lifecycle: proxy, cancellation: source.token)
        # GET carries its encoded message in the request target.
        bytes = request.http_method == "GET" ? url.request_uri.bytesize : body.bytesize
        Call.new(outgoing, bytes, source, proxy)
      end

      def admit(call)
        START_ATTEMPTS.times do
          return call if live_engine.admit(call, @max_requests, @max_bytes)
        end
        raise Error.new(:unavailable, STOPPED), cause: nil
      end

      # A single ivar read and pid comparison find the live engine; otherwise
      # the front lock replaces a stopped engine or another process's state.
      def live_engine
        current = @engine
        return current if @state == :open && current&.pid == Process.pid && current.state == :running

        @lock.synchronize do
          inherit_fork_state
          raise Error.new(:unavailable, UNAVAILABLE), cause: nil unless @state == :open

          pid = Process.pid
          unless @admission&.pid == pid
            # Inherited engine resources belong to the parent; never touch them.
            @admission = Admission.new(pid)
            @engine = nil
          end
          @engine = build_engine(@admission) unless @engine && @engine.state != :stopped
          @engine
        end
      end

      def build_engine(admission)
        Engine.new(self, admission, origin: @origin, snapshot: @snapshot, options: @options).start
      end

      def await(call, request)
        expires = Internal.now + request.deadline.remaining + Internal::CLEANUP_SECONDS if request.deadline
        lock = call.engine.admission.lock
        lock.synchronize do
          loop do
            return :result if call.result

            limit = [expires, call.cancel_expires].compact.min
            remaining = limit && (limit - Internal.now)
            if remaining && remaining <= 0
              return call.cancel_expires && call.cancel_expires <= Internal.now ? :canceled : :deadline
            end

            Thread.handle_interrupt(Object => :immediate) do
              call.signal.wait(lock, remaining && [remaining, Internal::MAX_WAIT].min)
            end
          end
        end
      end

      def take(call)
        engine = call.engine
        engine.admission.lock.synchronize do
          result = call.result
          call.result = nil
          call.caller_done = true
          engine.release(call) if call.reactor_done
          result
        end
      ensure
        call.proxy&.detach
      end

      # Detach before firing the interrupt, so the reactor's late failure
      # handling never reaches the caller's lifecycle handle.
      def abandon(call)
        call.proxy&.detach
        engine = call.engine
        engine.admission.lock.synchronize do
          unless call.caller_done
            call.caller_done = true
            call.result = nil
            engine.release(call) if call.reactor_done
          end
        end
        call.source.cancel
      end

      # Runs on the cancelling thread, or inline when already cancelled.
      def cancel_requested(call)
        call.source.cancel
        call.engine.admission.lock.synchronize do
          call.cancel_expires ||= Internal.now + Internal::CLEANUP_SECONDS
          call.signal.signal
        end
      end

      def begin_close(expires)
        @lock.synchronize do
          inherit_fork_state
          next unless @state == :open

          @state = :closing
          @close_expires = expires
          @close_pid = Process.pid
          engine = @engine if @engine&.pid == Process.pid
          if engine&.request_close(expires)
            @closing_engine = engine
          else
            finish_close(true)
          end
        end
      end

      # Every closer waits for the owner's outcome until grace plus the cleanup
      # allowance. The first to expire records false and wakes pending callers.
      def wait_closed
        stalled = nil
        @lock.synchronize do
          until @state == :closed
            remaining = @close_expires + Internal::CLEANUP_SECONDS - Internal.now
            if remaining <= 0
              stalled = @closing_engine
              finish_close(false)
              break
            end
            Thread.handle_interrupt(Object => :immediate) { @closed.wait(@lock, remaining) }
          end
        end
        if stalled
          stalled.force_unavailable
        elsif @closing_engine&.state == :stopped
          # The owner published its outcome last; join its exit within the allowance.
          thread = @closing_engine.thread
          remaining = @close_expires + Internal::CLEANUP_SECONDS - Internal.now
          Thread.handle_interrupt(Object => :immediate) { thread.join([remaining, Internal::CLEANUP_SECONDS].max) }
        end
        @close_outcome
      end

      # Runs on the owner thread after engine bookkeeping, without its lock.
      def owner_finished(engine, outcome)
        @lock.synchronize do
          finish_close(outcome.nil? ? false : outcome) if @state == :closing && @closing_engine.equal?(engine)
        end
      end

      def finish_close(outcome)
        @close_outcome = outcome
        @state = :closed
        @closed.broadcast
      end

      # A child that inherits another process's pending close has no engine to drain.
      def inherit_fork_state
        finish_close(true) if @state == :closing && @close_pid != Process.pid
      end

      def front_state
        state = @state
        state == :closing && @close_pid != Process.pid ? :closed : state
      end

      def check_owner!
        return unless Thread.current.thread_variable_get(OWNER).equal?(self)

        raise LifecycleError, "ThreadedTransport calls and close are not allowed from its owner thread"
      end

      def check_request!(request)
        request.cancellation ? request.cancellation.check! : request.deadline&.check!
      end

      def parse_origin(value)
        uri = URI(value)
        unless %w[http https].include?(uri.scheme) && uri.host && !uri.userinfo && !uri.query && !uri.fragment &&
               uri.path.empty?
          raise ArgumentError, "Origin must contain only an HTTP(S) scheme, host and port"
        end

        uri.freeze
      end

      def same_origin?(uri)
        [uri.scheme, uri.hostname&.downcase, uri.port] == [@origin.scheme, @origin.hostname.downcase, @origin.port]
      end
    end
  end
end
