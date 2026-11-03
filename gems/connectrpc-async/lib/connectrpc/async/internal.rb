# frozen_string_literal: true

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

      # Timeouts are injected only into a supervisor waiting on a private task.
      def self.wait(task, expires = nil)
        return task.wait unless expires

        loop do
          remaining = expires - now
          raise ::Async::TimeoutError if remaining <= 0

          begin
            return ::Async::Task.current.with_timeout([remaining, MAX_WAIT].min, WaitTimeout) { task.wait }
          rescue WaitTimeout
            # Never mistake a child's timeout for this supervisor's timer, or
            # expose our sentinel to a surrounding wait on this task.
            raise ::Async::TimeoutError if now >= expires
          end
        end
      end

      def self.join(task)
        task&.wait unless task&.current?
      rescue StandardError
        # The owner/supervisor reports the original failure.
        nil
      end

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

        def install_owner_guard
          @lifetime = @owner.async do
            ::Async::Condition.new.wait
          ensure
            abort_owned unless @state == :closed
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
        result = {}
        message.headers.header.each { |key, value| (result[key.downcase] ||= []) << value }
        length = message.body&.length
        length ||= message.stream.declared_length if message.respond_to?(:stream)
        length = 0 if length.nil? && message.body.nil?
        result["content-length"] = [length.to_s] if length
        result
      end

      def self.fields(headers)
        headers.flat_map { |key, values| values.map { |value| [key, value] } }
      end
    end
    private_constant :Internal
  end
end
