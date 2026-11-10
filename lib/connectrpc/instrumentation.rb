# frozen_string_literal: true

module ConnectRPC
  # Synchronous, fixed per-RPC callbacks. Subscribers never own the application yield.
  class Instrumentation
    # The fixed call contract names its declared RPC method explicitly.
    Call = Data.define(:role, :protocol, :method, :server_address, :server_port, :carrier) # rubocop:disable Lint/DataDefineOverride
    Outcome = Data.define(:code, :http_status, :error, :normalized_error, :duration,
                          :request_bytes, :response_bytes, :request_metadata, :response_metadata)
    Completion = Data.define(:outcome, :processing_state, :decision, :delivery, :error, :http_status, :duration)

    Callbacks = Data.define(:attach, :detach, :processing_finished, :finished)
    private_constant :Callbacks

    class Callbacks
      def self.discover(observer, bus)
        new(attach: callback(observer, :attach, bus), detach: callback(observer, :detach, bus),
            processing_finished: callback(observer, :processing_finished, bus),
            finished: callback(observer, :finished, bus))
      end

      # Reflection ends here; only typed callback records reach lifecycle work.
      def self.callback(observer, name, bus)
        bus.safely { observer.public_method(name).to_proc if observer.respond_to?(name) }
      end
      private_class_method :callback
    end

    def initialize(subscribers:, on_error: nil)
      @subscribers = subscribers.dup.freeze
      @on_error = on_error
    end

    # Internal constructor shared by client processing and the server hosting SPI.
    def start(owner:, role:, method:, carrier:, server_address: nil, server_port: nil, path: nil, protocol: :connect)
      call = Call.new(role: role, protocol: protocol, method: method,
                      server_address: server_address, server_port: server_port, carrier: carrier)
      observers = @subscribers.filter_map { |subscriber| safely { subscriber.start(call) } }
      Handle.new(self, owner, path, observers)
    end

    def safely
      yield
    rescue StandardError => e
      begin
        @on_error&.call(e)
      rescue StandardError
        nil
      end
      nil
    end

    # Defensive read-only snapshot. Extraction must not validate/reorder protocol work.
    # rubocop:disable-next Metrics/AbcSize, Metrics/MethodLength, Metrics/PerceivedComplexity
    def self.carrier(headers) # rubocop:disable Metrics/CyclomaticComplexity
      # @type var result: headers
      # @type var empty: headers
      empty = {}
      values = headers.each_with_object(empty) do |(key, entries), result|
        next unless key.is_a?(String)

        # Preserve Array coercion; the split lets Steep resolve Kernel.Array on each arm.
        entries = entries.is_a?(Array) ? entries : Array(entries) # rubocop:disable Style/RedundantSelfAssignmentBranch
        next unless entries.all?(String)

        (result[key.downcase] ||= []).concat(entries.map { |entry| entry.dup.freeze })
      end
      values.transform_values!(&:freeze).freeze
    rescue StandardError
      empty = {} #: headers
      empty.freeze
    end

    class Handle # rubocop:disable Metrics/ClassLength
      attr_reader :outcome
      attr_accessor :request_bytes, :request_metadata, :code, :normalized_error

      def initialize(bus, owner, path, observers)
        @bus = bus
        @owner = owner
        @path = path
        # Resolve the optional callback surface once per observer.
        @observers = observers.map { |observer| Callbacks.discover(observer, @bus) }
        @started_at = now
        @processing_state = :not_started
        request_metadata = {} #: headers
        @request_metadata = request_metadata.freeze
      end

      # Keep the application yield and reverse unwind in one exception boundary.
      def activate # rubocop:disable Metrics/MethodLength
        return yield if @terminal

        # @type var tokens: Array[[(^(Object?) -> void)?, Object?]]
        tokens = []
        begin
          @observers.each do |callbacks|
            next unless callbacks.attach

            @bus.safely do
              token = callbacks.attach.call
              tokens << [callbacks.detach, token]
            end
          end
          yield
        ensure
          detach_tokens(tokens)
        end
      end

      # Hosts may finish before processing unwinds. The first delivery fact wins.
      def finish(delivery:, error: nil, decision: :processed, http_status: nil)
        return if @terminal || @pending

        @pending = { delivery: delivery, error: error, decision: decision, http_status: http_status }
        complete unless @processing_held || @processing_state == :running || @notifying_processing
        nil
      end

      def host_rejected!
        @host_rejected = true
      end

      def host_rejected? = !!@host_rejected

      def record_failure(error)
        @error ||= error
        nil
      end

      # Internal, bound single-use processing entry. Hosts only activate and finish.
      def begin_processing(owner, path = nil)
        unless @owner.equal?(owner) && @path == path && @processing_state == :not_started && !@pending && !@terminal
          raise ArgumentError, "Lifecycle belongs to another call or has already been used"
        end

        @processing_state = :running
        @processing_at = now
      end

      # Prepare immutable facts; the original processor owns their final notification.
      def processing_finished(code:, http_status: nil, normalized_error: nil, error: nil,
                              response_bytes: nil, response_metadata: {})
        record_failure(error) if error
        processing_at = @processing_at #: Float
        @outcome = Outcome.new(code: code, http_status: http_status, error: @error,
                               normalized_error: normalized_error, duration: now - processing_at,
                               request_bytes: @request_bytes, response_bytes: response_bytes,
                               request_metadata: Instrumentation.carrier(@request_metadata),
                               response_metadata: Instrumentation.carrier(response_metadata))
        complete_processing unless @processing_held
      end

      private

      def detach_tokens(tokens)
        while (entry = tokens.pop)
          detach, token = entry
          @bus.safely { detach&.call(token) }
        end
        nil
      ensure
        # Only an escaping detach leaves tokens for this continuation.
        detach_tokens(tokens) unless tokens.empty?
      end

      # Native hosts hold preparation through the original application's cleanup.
      # Delivery can arrive meanwhile, but neither observer callback can run yet.
      def with_processing_completion
        @processing_held = true
        yield
      rescue Exception => e # rubocop:disable Lint/RescueException -- Retain fatal cleanup failures without swallowing them.
        record_failure(e)
        raise
      ensure
        @processing_held = false
        complete_processing if @outcome
        complete if @pending && !@outcome && !@terminal
      end

      def complete_processing
        return if @processing_state == :finished

        prepared = @outcome #: Outcome
        processing_at = @processing_at #: Float
        outcome = @outcome = prepared.with(duration: now - processing_at, error: @error)
        @processing_state = :finished
        notify_processing(outcome)
        nil
      end

      def now = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      def notify_processing(outcome)
        @notifying_processing = true
        @observers.each { |callbacks| @bus.safely { callbacks.processing_finished&.call(outcome) } }
      ensure
        @notifying_processing = false
        complete if @pending
      end

      def notify_finished(completion)
        @observers.each { |callbacks| @bus.safely { callbacks.finished&.call(completion) } }
      end

      def complete
        @terminal = true
        pending = @pending #: pending
        completion = Completion.new(outcome: @outcome, processing_state: @processing_state,
                                    duration: now - @started_at, delivery: pending[:delivery], error: pending[:error],
                                    decision: pending[:decision], http_status: pending[:http_status])
        notify_finished(completion)
      end
    end
  end
end
