# frozen_string_literal: true

module ConnectRPC
  module Health
    # One Watch stream's state, guarded by its registry's mutex. The pending
    # slot holds only the latest unsent status, so slow streams coalesce.
    class Watcher
      attr_reader :service

      def initialize(mutex, service, pending)
        @mutex = mutex
        @service = service
        @pending = pending
        @cancelled = false
        @closing = false
        @condition = ConditionVariable.new
      end

      # Callers hold the registry mutex.
      def update(status)
        @pending = status
        @condition.signal
      end

      # Callers hold the registry mutex. A nil status ends without another message.
      def close(status)
        @pending = status if status
        @closing = true
        @condition.signal
      end

      # Runs in a cancellation listener, outside the registry mutex.
      def cancel
        @mutex.synchronize do
          @cancelled = true
          @condition.signal
        end
      end

      # The next status to send, nil to end with OK, or :cancelled / :expired.
      # Repeated statuses are skipped. Every flag is re-checked under the mutex
      # before waiting, so a signal sent while the stream was busy is not lost.
      def next_status(last, deadline)
        @mutex.synchronize do
          loop do
            break :cancelled if @cancelled

            pending = take_pending
            break pending if pending && pending != last
            break if @closing
            break :expired if deadline&.expired?

            @condition.wait(@mutex, deadline&.native_timeout)
          end
        end
      end

      private

      def take_pending
        pending = @pending
        @pending = nil
        pending
      end
    end
    private_constant :Watcher
  end
end
