# frozen_string_literal: true

module ConnectRPC
  module Health
    # Per-service serving status for the standard grpc.health.v1 service.
    # Status updates are safe from any thread or fiber. Watch handlers send only
    # on their own stream's reactor/thread; updates signal them through one
    # Mutex and a ConditionVariable per watcher, never by sending directly.
    class Registry
      STATUSES = %i[unknown serving not_serving].freeze
      # grpc-go's List limit; larger registries fail resource_exhausted.
      LIST_LIMIT = 100
      WIRE = { unknown: :UNKNOWN, serving: :SERVING, not_serving: :NOT_SERVING,
               service_unknown: :SERVICE_UNKNOWN }.freeze
      private_constant :STATUSES, :LIST_LIMIT, :WIRE

      attr_reader :handlers

      def initialize(max_watchers: 100)
        unless max_watchers.is_a?(Integer) && max_watchers.positive?
          raise ArgumentError, "max_watchers must be a positive Integer"
        end

        @max_watchers = max_watchers
        @mutex = Mutex.new
        @statuses = { "" => :serving }
        @watchers = []
        @shutdown = false
        @handlers = ::Grpc::Health::V1::Health.handlers(check: method(:check), list: method(:list),
                                                        watch: method(:watch)).freeze
      end

      # Returns false, without a change, after shutdown.
      def set(service, status) # rubocop:disable Naming/PredicateMethod
        name = service_name(service)
        raise ArgumentError, "status must be :unknown, :serving or :not_serving" unless STATUSES.include?(status)

        @mutex.synchronize do
          return false if @shutdown

          @statuses[name] = status
          @watchers.each { |watcher| watcher.update(status) if watcher.service == name }
        end
        true
      end

      def status(service)
        name = service_name(service)
        @mutex.synchronize { @statuses[name] }
      end

      # Marks every entry not serving and ends existing Watch streams after
      # their final status. Later Watch calls fail unavailable. Idempotent.
      def shutdown
        @mutex.synchronize do
          next if @shutdown

          @shutdown = true
          @statuses.transform_values! { :not_serving }
          @watchers.each { |watcher| watcher.close((:not_serving if @statuses.key?(watcher.service))) }
        end
        nil
      end

      def shutdown? = @mutex.synchronize { @shutdown }
      def watcher_count = @mutex.synchronize { @watchers.size }

      private

      def service_name(service)
        raise ArgumentError, "service must be a String" unless service.is_a?(String)

        # encode always returns a copy, so the stored name is never shared.
        name = service.encode(Encoding::UTF_8)
        raise ArgumentError, "service must be valid UTF-8" unless name.valid_encoding?

        name.freeze
      rescue EncodingError
        raise ArgumentError, "service must be valid UTF-8"
      end

      def response(status)
        ::Grpc::Health::V1::HealthCheckResponse.new(status: WIRE.fetch(status))
      end

      def check(request, _context)
        status = @mutex.synchronize { @statuses[request.service] }
        raise Error.new(:not_found, "unknown service"), cause: nil unless status

        response(status)
      end

      def list(_request, _context)
        snapshot = @mutex.synchronize { @statuses.dup }
        raise Error.new(:resource_exhausted, "health list exceeds #{LIST_LIMIT} services"), cause: nil if
          snapshot.size > LIST_LIMIT

        ::Grpc::Health::V1::HealthListResponse.new(statuses: snapshot.transform_values { |status| response(status) })
      end

      def watch(request, writer, context)
        watcher = admit(request.service)
        begin
          # The listener may run synchronously here, so register it outside the
          # non-reentrant mutex; the watcher re-checks its flag before waiting.
          registration = context.on_cancel { watcher.cancel }
          stream(watcher, writer, context)
        ensure
          registration&.unregister
          @mutex.synchronize { @watchers.delete(watcher) }
        end
      end

      def admit(service)
        @mutex.synchronize do
          raise Error.new(:unavailable, "health service is shutting down"), cause: nil if @shutdown
          raise Error.new(:resource_exhausted, "too many health watchers"), cause: nil if
            @watchers.size >= @max_watchers

          watcher = Watcher.new(@mutex, service, @statuses.fetch(service, :service_unknown))
          @watchers << watcher
          watcher
        end
      end

      # Context checks and sends happen outside the registry mutex.
      def stream(watcher, writer, context)
        last = nil
        while (status = watcher.next_status(last, context.deadline))
          case status
          when :cancelled then fail_with(context, :canceled, "Call canceled")
          when :expired then fail_with(context, :deadline_exceeded, "Deadline exceeded")
          end
          writer.send_message(response(status))
          last = status
        end
      end

      # Prefers the context's own arbitrated reason when it has one.
      def fail_with(context, code, message) = context.check! || raise(Error.new(code, message), cause: nil)
    end
  end
end
