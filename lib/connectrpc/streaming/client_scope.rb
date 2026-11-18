# frozen_string_literal: true

module ConnectRPC
  module Streaming
    class ClientScope # rubocop:disable Metrics/ClassLength
      attr_accessor :completed, :abandoned, :independent_error, :failure, :result, :delivered_error
      # The opened session; its cached response metadata feeds the outcome.
      attr_writer :session

      def initialize(lifecycle)
        @lifecycle = lifecycle
        @completed = false
        @abandoned = false
        @intercepted = false
        @dispatched = false
        @innermost_ok = false
        @interceptor_error = false
      end

      def run(limit, cancellation, &)
        # @type var error: Exception?
        error = nil
        # @type var local_exit: bool
        local_exit = false
        execute(limit, cancellation, &).tap { local_exit = true }
      rescue Exception => e # rubocop:disable Lint/RescueException -- Finalization needs the actual escape, including fatal exceptions.
        error = e
        local_exit = true
        raise
      ensure
        finish_preserving_user(error, local_exit)
      end

      def execute(limit, cancellation)
        active, deadline = start(limit, cancellation)
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

      # Applies a stream call's wrappers. The factory runs outside the
      # application block, so its exceptions, and those of the wrap-time check,
      # are interceptor exceptions rather than application or core-owned ones.
      def wrap(call, endpoint)
        call.__send__(:wrap, endpoint)
      rescue Exception => e # rubocop:disable Lint/RescueException -- classify every factory escape
        record_user_error(e)
        @interceptor_error = true
        raise
      end

      # Runs an interceptor chain around the scope. The outermost value is the
      # scope result; a unary short-circuit completes the scope without an
      # exchange. An exception escaping the chain that is not the exact failure
      # of the dispatch adapter is independent.
      def intercept(chain, call, &) # rubocop:disable Metrics/MethodLength -- one place tracks every chain exit
        @intercepted = true
        # @type var local_exit: bool
        local_exit = false
        result = @result = chain.run(call, @lifecycle) { dispatch(&) }
        @completed = true unless @dispatched
        local_exit = true
        result
      rescue Exception => e # rubocop:disable Lint/RescueException -- provenance compares the exact escape
        local_exit = true
        unless e.equal?(@innermost_failure)
          @interceptor_error = true
          @independent_error = e
        end
        raise
      ensure
        # A throw or kill out of the chain delivers no result to the caller.
        @non_local_exit = true unless local_exit
      end

      private

      # A throw or kill out of execution or finalization keeps unwinding; only
      # a return or raise from both is replaced by the preserved independent
      # error.
      def finish_preserving_user(error, executed)
        # @type var local_exit: bool
        local_exit = false
        finish(@independent_error || error)
        local_exit = true
      rescue Exception # rubocop:disable Lint/RescueException -- the independent error replaces any finalization failure
        local_exit = true
        raise
      ensure
        original = @independent_error
        raise original, cause: original.cause if original && local_exit && executed
      end

      def dispatch
        @dispatched = true
        result = yield
        @innermost_ok = true
        result
      rescue Exception => e # rubocop:disable Lint/RescueException -- provenance compares the exact escape
        # Interceptors see an independent exception, not transport or cleanup
        # noise that replaced it on the way out of the stream.
        independent = @independent_error
        failure = @innermost_failure = independent || e
        raise failure, cause: failure.cause unless failure.equal?(e)

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

        completed = @interceptor_error || @non_local_exit ? exchange_completed? : delivery_completed?(error)
        completed ? :completed : :aborted
      end

      # An abandoned scope returns normally from the innermost continuation
      # without completing its exchange.
      def exchange_completed?
        delivered = @delivered_error
        (@innermost_ok && @completed) || (!delivered.nil? && delivered.equal?(@innermost_failure))
      end

      def start(limit, cancellation)
        timeout, supplied = limit
        deadline = Protocol.call_deadline(timeout, supplied)
        active = @state = CallCancellation.new(deadline: deadline, cancellation: cancellation)
        active.check!
        [active, deadline]
      end

      def implicit_abandonment?
        !@independent_error && (@failure.nil? || @failure.is_a?(StreamNotConsumedError))
      end

      def success?(error) = @completed && error.nil? && !@non_local_exit

      def delivery_completed?(error)
        return true if success?(error)

        delivered = @delivered_error
        return false unless delivered

        delivered.equal?(error)
      end

      # Only metadata the session already accepted: finishing never reads.
      def report_processing(lifecycle, error)
        normalized = normalized_error(error)
        session = @session
        lifecycle.processing_finished(code: normalized&.code || ("OK" if success?(error)),
                                      error: error, normalized_error: normalized,
                                      response_metadata: session&.cached_headers || Metadata.new,
                                      response_trailers: session&.trailers || Metadata.new)
      end

      # An independent exception escaping the outermost interceptor sets the
      # outcome over abandonment.
      def normalized_error(error)
        return Error.new(:canceled, "Stream abandoned") if @abandoned && !@interceptor_error

        error if error.is_a?(Error)
      end
    end
  end
end
