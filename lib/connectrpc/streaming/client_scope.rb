# frozen_string_literal: true

module ConnectRPC
  module Streaming
    class ClientScope # rubocop:disable Metrics/ClassLength
      attr_accessor :completed, :abandoned, :independent_error, :failure, :result, :delivered_error

      def initialize(lifecycle)
        @lifecycle = lifecycle
        @completed = false
        @abandoned = false
        @intercepted = false
        @dispatched = false
        @innermost_ok = false
        @interceptor_error = false
      end

      def run(timeout, cancellation, &)
        # @type var error: Exception?
        error = nil
        execute(timeout, cancellation, &)
      rescue Exception => e # rubocop:disable Lint/RescueException -- Finalization needs the actual escape, including fatal exceptions.
        error = e
        raise
      ensure
        finish_preserving_user(error)
      end

      def execute(timeout, cancellation)
        active, deadline = start(timeout, cancellation)
        yield active, deadline
        active.commit! if @completed
        @result
      rescue StandardError => e
        @lifecycle&.record_failure(e)
        @state&.commit! unless @independent_error || @abandoned
        raise
      end
      private :execute

      def cleanup(session)
        return if @completed || @delivered_error

        @abandoned = true if implicit_abandonment?
        session.abandon unless session.abandoned?
      rescue StandardError => e
        @lifecycle&.record_failure(e)
      end

      def record_user_error(error)
        @independent_error = error
        @lifecycle&.record_failure(error)
      end

      # Runs a unary interceptor chain around the single-request scope. The
      # outermost value is the scope result; a short-circuit completes the
      # scope without an exchange. An exception escaping the chain that is not
      # the exact failure of the dispatch adapter is independent.
      def intercept(chain, call, &)
        @intercepted = true
        result = @result = chain.run(call, @lifecycle) { dispatch_unary(&) }
        @completed = true unless @dispatched
        result
      rescue Exception => e # rubocop:disable Lint/RescueException -- provenance compares the exact escape
        unless e.equal?(@innermost_failure)
          @interceptor_error = true
          @independent_error = e
        end
        raise
      end

      private

      def finish_preserving_user(error)
        finish(@independent_error || error)
      ensure
        original = @independent_error
        raise original, cause: original.cause if original
      end

      def dispatch_unary
        @dispatched = true
        result = yield
        @innermost_ok = true
        result
      rescue Exception => e # rubocop:disable Lint/RescueException -- provenance compares the exact escape
        @innermost_failure = e
        raise
      end

      def finish(error)
        @state&.close
        lifecycle = @lifecycle
        return unless lifecycle

        lifecycle.record_failure(error) if error
        report_processing(lifecycle, error)
        lifecycle.finish(delivery: delivery(error))
      end

      # Interceptor delivery comes from exchange facts, not from who raised.
      def delivery(error)
        return :unobserved if @intercepted && !@dispatched

        completed = @interceptor_error ? exchange_completed? : delivery_completed?(error)
        completed ? :completed : :aborted
      end

      def exchange_completed?
        delivered = @delivered_error
        @innermost_ok || (!delivered.nil? && delivered.equal?(@innermost_failure))
      end

      def start(timeout, cancellation)
        deadline = Deadline.new(timeout) unless timeout.nil?
        active = @state = CallCancellation.new(deadline: deadline, cancellation: cancellation)
        active.check!
        [active, deadline]
      end

      def implicit_abandonment?
        !@independent_error && (@failure.nil? || @failure.is_a?(StreamNotConsumedError))
      end

      def success?(error) = @completed && error.nil?

      def delivery_completed?(error)
        return true if success?(error)

        delivered = @delivered_error
        return false unless delivered

        delivered.equal?(error)
      end

      def report_processing(lifecycle, error)
        normalized = normalized_error(error)
        lifecycle.processing_finished(code: normalized&.code || ("OK" if success?(error)),
                                      error: error, normalized_error: normalized)
      end

      def normalized_error(error)
        return Error.new(:canceled, "Stream abandoned") if @abandoned

        error if error.is_a?(Error)
      end
    end
  end
end
