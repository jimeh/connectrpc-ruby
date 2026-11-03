# frozen_string_literal: true

module ConnectRPC
  module Streaming
    class ClientScope
      attr_accessor :completed, :abandoned, :independent_error, :failure, :result, :delivered_error

      def initialize(lifecycle)
        @lifecycle = lifecycle
        @completed = false
        @abandoned = false
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

      private

      def finish_preserving_user(error)
        finish(@independent_error || error)
      ensure
        original = @independent_error
        raise original, cause: original.cause if original
      end

      def finish(error)
        @state&.close
        lifecycle = @lifecycle
        return unless lifecycle

        lifecycle.record_failure(error) if error
        report_processing(lifecycle, error)
        lifecycle.finish(delivery: delivery_completed?(error) ? :completed : :aborted)
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
