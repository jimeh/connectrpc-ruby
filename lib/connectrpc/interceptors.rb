# frozen_string_literal: true

module ConnectRPC
  # Interceptor misuse: a continuation called twice, late, from another fiber or
  # thread, a write to a dispatched call, or an invalid layer result.
  class InterceptorStateError < StandardError; end

  # One client unary RPC as its interceptors see it. Core builds it; the request
  # is replaceable until the innermost continuation starts dispatch.
  class ClientUnaryCall
    attr_reader :method, :protocol, :request, :request_metadata, :deadline, :cancellation

    def initialize(method:, protocol:, request:, request_metadata:, deadline:, cancellation:)
      @method = method
      @protocol = protocol
      @request = request
      @request_metadata = request_metadata
      @deadline = deadline
      @cancellation = cancellation
      @open = true
    end

    def request=(message)
      raise InterceptorStateError, "The call has already been dispatched" unless @open

      @request = message
    end

    private

    def dispatch!
      @open = false
      @request_metadata.freeze
      nil
    end
  end

  # One server unary RPC as its interceptors see it. Request metadata and
  # response metadata live on the handler's Context.
  class ServerUnaryCall
    attr_reader :method, :protocol, :request, :context

    def initialize(method:, protocol:, request:, context:)
      @method = method
      @protocol = protocol
      @request = request
      @context = context
      @open = true
    end

    def request=(message)
      raise InterceptorStateError, "The call has already been dispatched" unless @open

      @request = message
    end

    private

    def dispatch!
      @open = false
      nil
    end
  end

  # Construction-time discovery and the per-RPC guarded continuation chain.
  module Interceptors
    def self.client(interceptors)
      build(interceptors, :client_unary, :client_stream, client: true)
    end

    def self.server(interceptors)
      build(interceptors, :server_unary, :server_stream, client: false)
    end

    def self.build(interceptors, unary, stream, client:)
      raise ArgumentError, "interceptors must be an Array" unless interceptors.is_a?(Array)

      layers = interceptors.map do |interceptor|
        probe = interceptor #: (client_interceptor | server_interceptor) & _InterceptorProbe
        # Fail closed: an accepted stream method that never ran could skip policy.
        raise ArgumentError, "Stream interceptors (#{stream}) are not supported yet" if probe.respond_to?(stream)
        raise ArgumentError, "Interceptor does not implement #{unary}" unless probe.respond_to?(unary)

        probe.public_method(unary)
      end
      Chain.new(layers.freeze, client) unless layers.empty?
    end
    private_class_method :build

    # Client unary results reach every outer layer and the caller frozen.
    def self.response!(value)
      unless value.is_a?(Response) && value.metadata.is_a?(Metadata) && value.trailers.is_a?(Metadata)
        raise InterceptorStateError, "Client unary interceptors must return a ConnectRPC::Response"
      end

      value.metadata.freeze
      value.trailers.freeze
      value.freeze
    end

    class Chain
      def initialize(layers, client)
        @layers = layers
        @client = client
      end

      # The recorder receives each failure escaping a continuation before the
      # outer layer sees it; recording keeps the first.
      def run(call, recorder, &dispatch)
        step(0, call, recorder, dispatch)
      end

      private

      def step(index, call, recorder, dispatch)
        layer = @layers[index]
        return dispatch.call unless layer

        continuation = Continuation.new(recorder)
        begin
          result = layer.call(call) { continuation.call { step(index + 1, call, recorder, dispatch) } }
        rescue Exception # rubocop:disable Lint/RescueException -- close first so a captured continuation cannot run late
          continuation.close
          raise
        end
        continuation.finish(result, @client)
      end
    end

    # One layer's continuation for one RPC: once, on the owning fiber, while the
    # layer runs. Foreign callers only latch their misuse; the owner raises it.
    class Continuation
      def initialize(recorder)
        @recorder = recorder
        @fiber = Fiber.current
        @mutex = Mutex.new
        @state = :active
        @failure = nil
        @misuse = nil
      end

      def call
        misuse = claim
        raise misuse if misuse

        begin
          yield
        rescue Exception => e # rubocop:disable Lint/RescueException -- rule 6 re-raises any escaped failure
          @failure = e
          @recorder&.record_failure(e) if e.is_a?(StandardError)
          raise
        end
      end

      def close
        @mutex.synchronize do
          @state = :finished
          @misuse
        end
      end

      # Unwind order: a continuation failure, a latched misuse, then the result.
      def finish(result, client)
        yielded = @state == :claimed
        misuse = close
        failure = @failure
        raise failure, cause: failure.cause if failure
        raise misuse if misuse
        return Interceptors.response!(result) if client
        raise InterceptorStateError, "Server interceptors must yield" unless yielded

        result
      end

      private

      # Keep each state decision and its latch in one critical section. The
      # first misuse while the layer runs is the one its unwind re-raises.
      def claim # rubocop:disable Metrics/MethodLength
        @mutex.synchronize do
          next InterceptorStateError.new("Continuation called after its interceptor returned") if @state == :finished

          if !Fiber.current.equal?(@fiber)
            error = InterceptorStateError.new("Continuation called from another fiber or thread")
          elsif @state == :claimed
            error = InterceptorStateError.new("Continuation called more than once")
          else
            @state = :claimed
            next
          end
          @misuse ||= error
          error
        end
      end
    end
  end
end
