# frozen_string_literal: true

require_relative "completion_join"

module ConnectRPC
  module Async
    module Internal
      CLEANUP_SECONDS = 0.1
      MAX_WAIT = 86_400

      # Retained by the host through response output, independent of handler tasks.
      class Exchange
        attr_accessor :decision, :http_status

        def initialize(lifecycle)
          @lifecycle = lifecycle
          @decision = :processed
        end

        def finish(completed, error = nil)
          @lifecycle.finish(delivery: completed ? :completed : :aborted, error: error,
                            decision: @lifecycle.host_rejected? ? :host_rejected : @decision,
                            http_status: @http_status)
        end
      end

      class WaitTimeout < ::Async::TimeoutError; end
      private_constant :WaitTimeout

      # Every internal expiry. It is an Async::TimeoutError, so existing rescues
      # still match, while client waits can tell a caller's own enclosing
      # `with_timeout`, which they re-raise unchanged, from their own expiry.
      class Expired < ::Async::TimeoutError; end

      # A caller's own Async timeout, raised into the waiting fiber by the
      # caller's timer. `owned`, the failure a joined task or worker delivered,
      # is a transport failure whatever its class.
      def self.foreign_timeout?(error, owned = nil)
        error.is_a?(::Async::TimeoutError) && !error.is_a?(Expired) && !error.equal?(owned)
      end

      # Waits for a promise until `expires`, like `wait` does for a task.
      def self.wait_event(event, expires)
        loop do
          return event.wait unless expires

          remaining = expires - now
          raise Expired if remaining <= 0

          begin
            return ::Async::Task.current.with_timeout([remaining, MAX_WAIT].min, WaitTimeout) { event.wait }
          rescue WaitTimeout
            raise Expired if now >= expires
          end
        end
      end

      # Negotiated a protocol other than the one the client requires.
      class ALPNError < OpenSSL::SSL::SSLError; end

      # Fixed, non-secret phrases for client transport failures. They name
      # what failed, never the origin, address, path or certificate details.
      module ClientFailure
        CONNECTION_CLOSED = [EOFError, Errno::ECONNRESET, Errno::EPIPE, Errno::ECONNABORTED].freeze
        TLS_VERIFICATION = ["certificate verify failed", "does not match the server certificate"].freeze

        def self.reason(error)
          case error
          when Errno::ECONNREFUSED then "connection refused"
          when SocketError then "name resolution failed"
          when Errno::EHOSTUNREACH, Errno::ENETUNREACH then "host unreachable"
          when ::Async::TimeoutError, Errno::ETIMEDOUT, ::IO::TimeoutError then "timed out"
          when ALPNError then "ALPN negotiation failed"
          when OpenSSL::SSL::SSLError
            TLS_VERIFICATION.any? { |text| error.message.include?(text) } ? "TLS verification failed" : "TLS error"
          when *CONNECTION_CLOSED then "connection closed"
          when ::Protocol::HTTP2::Error then "HTTP/2 protocol error"
          when HTTP1ResponseError then "malformed response"
          end
        end

        def self.message(prefix, error)
          reason = reason(error)
          reason ? "#{prefix} transport failed: #{reason}" : "#{prefix} transport failed"
        end

        def self.connection_closed?(error) = CONNECTION_CLOSED.any? { |closed| error.is_a?(closed) }
      end

      def self.now = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      def self.positive_integer(value, name)
        raise ArgumentError, "#{name} must be a positive integer" unless value.is_a?(Integer) && value.positive?

        value
      end

      def self.seconds(value, name, zero: false)
        unless value.is_a?(Numeric) && value.real? && value.finite? && (zero ? value >= 0 : value.positive?)
          raise ArgumentError, "#{name} must be finite #{zero ? 'nonnegative' : 'positive'} seconds"
        end

        value
      end

      def self.finite(value, name, requirement)
        unless value.is_a?(Numeric) && value.real? && value.finite? && yield(value)
          raise ArgumentError, "#{name} must be a finite number #{requirement}"
        end

        value
      end

      # Monotonic time and scheduler-aware sleep. Specs replace a manager's
      # clock to control backoff timing.
      module MonotonicClock
        def self.now = Internal.now

        def self.sleep(seconds)
          expires = now + seconds
          while (remaining = expires - now).positive?
            ::Kernel.sleep([remaining, MAX_WAIT].min)
          end
        end
      end

      # Timeouts are injected only into a supervisor waiting on a private task.
      def self.wait(task, expires = nil)
        return task.wait unless expires

        loop do
          remaining = expires - now
          raise Expired if remaining <= 0

          begin
            return ::Async::Task.current.with_timeout([remaining, MAX_WAIT].min, WaitTimeout) { task.wait }
          rescue WaitTimeout
            # Never mistake a child's timeout for this supervisor's timer, or
            # expose our sentinel to a surrounding wait on this task.
            raise Expired if now >= expires
          end
        end
      end

      def self.join(task)
        task&.wait unless task&.current?
      rescue StandardError
        # The owner/supervisor reports the original failure.
        nil
      end

      # Client cleanup waits in a caller's fiber. The caller's own Async
      # timeout landing there does not abandon the wait; the caller re-raises
      # it once its disposition is complete.
      module Retaining
        # Returns an interruption of the joiner. The joined task's own failure
        # is reported by its owner.
        def self.join(task)
          interruption = nil
          begin
            task&.wait unless task&.current?
          rescue StandardError => e
            unless task.failed? && task.result.equal?(e)
              interruption ||= e
              retry
            end
          end
          interruption
        end

        # Like Internal.wait, adding a caller's own timeout to `retained`. The
        # task's own failure is raised whatever its class.
        def self.wait(task, expires, retained)
          Internal.wait(task, expires)
        rescue ::Async::TimeoutError => e
          raise unless Internal.foreign_timeout?(e, (task.result if task.failed?))

          retained << e
          retry
        end
      end

      def self.join_complete(...) = CompletionJoin.call(...)

      def self.cancel(task)
        return unless task && !task.current? && !task.finished?

        task.cancel
        task.cancel if task.cancel_deferred?
      end

      def self.raw_close(raw)
        raw.close if raw && !raw.closed?
      rescue IOError, SystemCallError
        nil
      end

      module Affinity
        def initialize_affinity
          @owner = ::Async::Task.current?
          raise LifecycleError, "An application-owned Async task is required" unless @owner

          @thread = Thread.current
          @reactor = Fiber.scheduler
          @progress = ::Async::Condition.new
          @resources = []
          @state = :new
        end

        def check_affinity!
          return if Thread.current.equal?(@thread) && Fiber.scheduler.equal?(@reactor) && ::Async::Task.current?

          raise LifecycleError, "Use the owning Async reactor and thread"
        end

        # The guard lives as long as the owner's block and every other task
        # under the owner, then aborts an unclosed transport so the owner can
        # finish. Owner cancellation still cancels it directly. A registered
        # watcher beside it waits for the owner and signals the guard, which
        # itself waits only on a condition: when the guard waited on the owner
        # task instead, an owner cancellation on Ruby 4 changed how its abort
        # interleaved with native socket close.
        def install_owner_guard
          exited = ::Async::Condition.new
          @lifetime = @owner.async do |guard|
            OwnerGuard.register(guard)
            exited.wait
          ensure
            begin
              abort_owned unless @state == :closed
            ensure
              Internal.cancel(@owner_watcher)
            end
          end
          @owner_watcher = @owner.async do |watcher|
            OwnerGuard.register(watcher)
            OwnerGuard.await(@owner)
            exited.signal
          end
        end

        def close(grace: 5)
          check_affinity!
          Internal.seconds(grace, "grace", zero: true)
          return @close_outcome if @state == :closed

          if @state == :closing
            @progress.wait while @state == :closing
            return @close_outcome
          end

          @state = :closing
          @closer = @owner.async(finished: false) do
            begin
              close_owned(Internal.now + grace)
            ensure
              abort_owned
            end
            @close_outcome
          end
          @closer.wait
        end

        def finish_closed(outcome)
          @close_outcome = outcome
          @state = :closed
          @progress.signal
        end

        private :initialize_affinity, :check_affinity!, :install_owner_guard, :finish_closed
      end

      # Client transport lifetime guards. Every guard skips every guard
      # subtree, its own and other transports' on the same owner, so guards
      # never wait on each other.
      module OwnerGuard
        GUARDS = ObjectSpace::WeakKeyMap.new

        def self.register(task)
          GUARDS[task] = true
        end

        def self.await(owner)
          join(owner)
          while (task = pending(owner))
            join(task)
          end
        end

        # The first non-transient task under `node`, outside a guard subtree,
        # whose block is still running, descending through tasks whose block
        # has ended but whose children remain. A cancelled task resolves
        # before it stops its children, so its fiber can stay alive while
        # their cleanup yields; waiting on it would return at once and spin.
        def self.pending(node)
          node.children&.each do |child|
            next if child.transient? || GUARDS.key?(child)
            return child if child.alive? && !(child.completed? || child.failed? || child.cancelled?)

            found = pending(child)
            return found if found
          end
          nil
        end

        def self.join(task)
          task.wait
        rescue StandardError
          # The task's owner reports its failure.
          nil
        end
      end

      # Core reads at most n bytes. Keep only the remainder of the current
      # backend chunk, including when a request crosses an exact chunk boundary.
      class Input
        def initialize(body, resource: nil)
          @body = body
          @resource = resource
          @remainder = nil
        end

        def read(length, timeout: nil)
          return read_fragment(length) unless timeout

          task = ::Async::Task.current.async(finished: false) { read_fragment(length) }
          Internal.wait(task, Internal.now + timeout)
        ensure
          if task && !task.finished?
            cleanup = ::Async::Task.current.async(finished: false) do
              task.cancel
              Internal.join(task)
            end
            begin
              Internal.wait(cleanup, Internal.now + CLEANUP_SECONDS)
            rescue StandardError
              @resource&.abort
              Internal.cancel(cleanup)
              Internal.join(cleanup)
            end
          end
        end

        def release_remainder
          size = @remainder&.bytesize || 0
          @remainder = nil
          size
        end

        private

        def read_fragment(length)
          @remainder = @body&.read if @remainder.nil? || @remainder.empty?
          return unless @remainder

          result = @remainder.byteslice(0, length)
          @remainder = @remainder.byteslice(result.bytesize..)
          result
        end
      end

      # HTTP/1 has no stream object retaining admission through body output.
      # Release only after wire writing and response-finished callbacks close
      # the body, including empty responses and cancellation during callbacks.
      class AdmissionBody < ::Protocol::HTTP::Body::Wrapper
        def initialize(body, &completion)
          super(body || ::Protocol::HTTP::Body::Buffered.wrap([]))
          @completion = completion
        end

        def close(error = nil)
          completion = @completion
          return unless completion

          @completion = nil
          begin
            super
          ensure
            completion.call
          end
        end
      end

      def self.headers(message)
        # H1 chunked parsing appends trailers to the mutable native headers.
        # Every later processing/observation phase must use the leading carrier.
        if !message.respond_to?(:stream) && (leading = message.instance_variable_get(:@connectrpc_leading_headers))
          return leading
        end

        result = {}
        message.headers.header.each { |key, value| (result[key.downcase] ||= []) << value.dup.freeze }
        length = message.body&.length
        length ||= message.stream.declared_length if message.respond_to?(:stream)
        length = 0 if length.nil? && message.body.nil?
        result["content-length"] = [length.to_s] if length
        unless message.respond_to?(:stream)
          result.each_value(&:freeze)
          message.instance_variable_set(:@connectrpc_leading_headers, result.freeze)
        end
        result
      end

      def self.fields(headers)
        headers.flat_map { |key, values| values.map { |value| [key, value] } }
      end

      def self.close_consumed_http1(request)
        return if request.respond_to?(:stream) || !request.body.respond_to?(:body)

        framed = request.body.body
        complete = framed.nil? ||
                   (framed.is_a?(::Protocol::HTTP1::Body::Fixed) && framed.remaining.zero?) ||
                   (framed.is_a?(::Protocol::HTTP1::Body::Chunked) && !framed.length.nil?)
        request.body&.close if complete
      end
    end
    private_constant :Internal
  end
end
