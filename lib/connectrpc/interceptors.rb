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

      def step(index, call, recorder, dispatch) # rubocop:disable Metrics/MethodLength -- one place tracks every exit
        layer = @layers[index]
        return dispatch.call unless layer

        continuation = Continuation.new(recorder)
        # @type var exit_kind: :returned | :raised | :nonlocal
        exit_kind = :nonlocal
        # @type var yielded: bool
        # @type var result: untyped
        begin
          result = layer.call(call) { continuation.call { step(index + 1, call, recorder, dispatch) } }
          exit_kind = :returned
        rescue Exception # rubocop:disable Lint/RescueException -- an exceptional exit keeps its own exception
          exit_kind = :raised
          raise
        ensure
          # Every exit closes, so a captured continuation cannot run late.
          yielded = continuation.close(exit_kind)
        end
        continuation.finish(result, yielded, @client)
      end
    end

    # One layer's continuation for one RPC: once, on the owning fiber, while the
    # layer runs. Foreign callers only latch their misuse; the owner records and
    # raises it, and a latched misuse never dispatches.
    #
    # States: :active (layer running, not yet yielded), :claimed (the owner
    # yielded) and :finished (the layer exited). Only the owner's first claim
    # while :active and nothing is latched moves to :claimed; every exit moves to
    # :finished. A late claim raises without latching or recording.
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

      # Runs on the owner fiber for every exit and returns whether the layer
      # yielded. A latched misuse is recorded here, before any outer layer can
      # translate the exit. A killed thread keeps unwinding because raising
      # would resurrect it.
      def close(exit_kind)
        yielded = @mutex.synchronize do
          claimed = @state == :claimed
          @state = :finished
          claimed
        end
        # A finished continuation latches nothing more, so this read is stable.
        misuse = @misuse
        @recorder&.record_failure(misuse) if misuse
        escape! if exit_kind == :nonlocal && Thread.current.status != "aborting"
        yielded
      end

      # Unwind order after a normal return: a continuation failure, a latched
      # misuse, then the result.
      def finish(result, yielded, client)
        raise_latched
        return Interceptors.response!(result) if client
        raise InterceptorStateError, "Server interceptors must yield" unless yielded

        result
      end

      private

      # Keep each state decision and its latch in one critical section. Only
      # the owner records its misuse, and a late call leaves the finished RPC
      # untouched.
      def claim
        owner = Fiber.current.equal?(@fiber)
        late = false
        # @type var misuse: InterceptorStateError?
        misuse = @mutex.synchronize do
          late = @state == :finished
          next InterceptorStateError.new("Continuation called after its interceptor returned") if late

          owner ? claim_owned : latch_foreign
        end
        @recorder&.record_failure(misuse) if misuse && owner && !late
        misuse
      end

      # The first misuse is latched; every later owner claim re-raises it.
      def claim_owned
        return @misuse if @misuse
        return @misuse = InterceptorStateError.new("Continuation called more than once") if @state == :claimed

        @state = :claimed
        nil
      end

      def latch_foreign
        error = InterceptorStateError.new("Continuation called from another fiber or thread")
        @misuse ||= error
        error
      end

      # Rule 6 first, then a latched misuse.
      def raise_latched
        failure = @failure
        raise failure, cause: failure.cause if failure

        misuse = @misuse
        raise misuse if misuse
      end

      # A throw or other non-local exit cannot leave the layer as success.
      def escape!
        raise_latched
        error = InterceptorStateError.new("Interceptor exited without returning or raising")
        @recorder&.record_failure(error)
        raise error
      end
    end
  end
end
