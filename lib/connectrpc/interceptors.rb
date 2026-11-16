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

  # One client stream RPC as its interceptors see it. Wrappers registered with
  # wrap_stream decorate the endpoint the application block receives.
  class ClientStreamCall
    attr_reader :method, :protocol, :request_metadata, :deadline, :cancellation

    def initialize(method:, protocol:, request_metadata:, deadline:, cancellation:)
      @method = method
      @protocol = protocol
      @request_metadata = request_metadata
      @deadline = deadline
      @cancellation = cancellation
      @wrappers = []
      @open = true
    end

    def wrap_stream(&factory)
      raise ArgumentError, "wrap_stream requires a block" unless factory
      raise InterceptorStateError, "The call has already been dispatched" unless @open

      @wrappers << factory
      nil
    end

    private

    def dispatch!
      @open = false
      @request_metadata.freeze
      nil
    end

    def wrap(endpoint) = Interceptors.wrap(@wrappers, endpoint)
  end

  class ClientStreamCall
    # Server streaming: the single request is replaceable until dispatch.
    class ServerStreaming < ClientStreamCall
      attr_reader :request

      def initialize(method:, protocol:, request:, request_metadata:, deadline:, cancellation:)
        super(method: method, protocol: protocol, request_metadata: request_metadata, deadline: deadline,
              cancellation: cancellation)
        @request = request
      end

      def request=(message)
        raise InterceptorStateError, "The call has already been dispatched" unless @open

        @request = message
      end
    end
  end

  # One server stream RPC as its interceptors see it. The context is the
  # handler's StreamingContext.
  class ServerStreamCall
    attr_reader :method, :protocol, :context

    def initialize(method:, protocol:, context:)
      @method = method
      @protocol = protocol
      @context = context
      @wrappers = []
      @open = true
    end

    def wrap_stream(&factory)
      raise ArgumentError, "wrap_stream requires a block" unless factory
      raise InterceptorStateError, "The call has already been dispatched" unless @open

      @wrappers << factory
      nil
    end

    private

    def dispatch!
      @open = false
      nil
    end

    def wrap(endpoint) = Interceptors.wrap(@wrappers, endpoint)
  end

  class ServerStreamCall
    # Server streaming: core reads the single request before the chain; it is
    # replaceable until dispatch.
    class ServerStreaming < ServerStreamCall
      attr_reader :request

      def initialize(method:, protocol:, request:, context:)
        super(method: method, protocol: protocol, context: context)
        @request = request
      end

      def request=(message)
        raise InterceptorStateError, "The call has already been dispatched" unless @open

        @request = message
      end
    end
  end

  # Construction-time discovery and the per-RPC guarded continuation chain.
  module Interceptors
    # Returns the unary and stream chains for the role; a kind without layers
    # is nil, so its RPCs take the direct path.
    def self.client(interceptors) = build(interceptors, :client_unary, :client_stream)
    def self.server(interceptors) = build(interceptors, :server_unary, :server_stream)

    def self.build(interceptors, unary, stream)
      raise ArgumentError, "interceptors must be an Array" unless interceptors.is_a?(Array)

      interceptors.each do |interceptor|
        probe = interceptor #: (client_interceptor | server_interceptor) & _InterceptorProbe
        next if probe.respond_to?(unary) || probe.respond_to?(stream)

        raise ArgumentError, "Interceptor implements neither #{unary} nor #{stream}"
      end
      [chain(interceptors, unary), chain(interceptors, stream)]
    end
    private_class_method :build

    def self.chain(interceptors, kind)
      layers = interceptors.filter_map do |interceptor|
        probe = interceptor #: (client_interceptor | server_interceptor) & _InterceptorProbe
        probe.public_method(kind) if probe.respond_to?(kind)
      end
      Chain.new(layers.freeze, kind) unless layers.empty?
    end
    private_class_method :chain

    # Client unary and client-streaming results reach every outer layer and
    # the caller frozen.
    def self.response!(value)
      unless value.is_a?(Response) && value.metadata.is_a?(Metadata) && value.trailers.is_a?(Metadata)
        raise InterceptorStateError, "Client unary and client-streaming interceptors must return a ConnectRPC::Response"
      end

      value.metadata.freeze
      value.trailers.freeze
      value.freeze
    end

    # Applies stream wrappers innermost first, so the application receives
    # outer(inner(raw)). Each wrapper must answer the raw endpoint's API.
    def self.wrap(wrappers, raw)
      return raw if wrappers.empty?

      api = (raw.class.public_instance_methods - Object.public_instance_methods).sort
      wrappers.reverse_each.reduce(raw) do |inner, factory|
        wrapped = factory.call(inner)
        missing = api.reject { |name| wrapped.respond_to?(name) }
        raise InterceptorStateError, "Stream wrapper does not respond to #{missing.join(', ')}" unless missing.empty?

        wrapped
      end
    end

    class Chain
      def initialize(layers, kind)
        @layers = layers
        @kind = kind
      end

      # The recorder receives each failure escaping a continuation before the
      # outer layer sees it; recording keeps the first.
      def run(call, recorder, &dispatch)
        step(0, call, recorder, dispatch, nil, mode(call))
      end

      private

      # :response may skip its continuation; every other mode must yield.
      # Server streaming and bidi results are ignored, so every yield is nil.
      def mode(call)
        case @kind
        when :client_unary then :response
        when :server_unary then :value
        else
          return :ignored unless call.method.is_a?(ClientStreamMethod)

          @kind == :client_stream ? :stream_response : :value
        end
      end

      def step(index, call, recorder, dispatch, parent, mode) # rubocop:disable Metrics/ParameterLists, Metrics/MethodLength
        layer = @layers[index]
        return innermost(dispatch, mode) unless layer

        continuation = Continuation.new(recorder)
        # @type var yielded: bool
        # @type var result: untyped
        begin
          result = layer.call(call) do
            continuation.call { step(index + 1, call, recorder, dispatch, continuation, mode) }
          end
        ensure
          # Every exit closes, so a captured continuation cannot run late.
          yielded = continuation.close(parent)
        end
        continuation.finish(result, yielded, mode)
      end

      def innermost(dispatch, mode)
        value = dispatch.call
        value unless mode == :ignored
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
    #
    # The continuation fails when its block raises or exits without returning or
    # raising (a throw or a killed fiber). Once failed, no misuse is recorded
    # over the failure, and a layer that returns re-raises it. Nothing here
    # raises from an ensure, so a non-local exit keeps unwinding untouched.
    class Continuation
      def initialize(recorder)
        @recorder = recorder
        @fiber = Fiber.current
        @mutex = Mutex.new
        @state = :active
        @failure = nil
        @misuse = nil
        @handed_up = nil
      end

      def call # rubocop:disable Metrics/MethodLength -- one place tracks every block exit
        misuse = claim
        raise misuse if misuse

        settled = false
        begin
          result = yield
          settled = true
          result
        rescue Exception => e # rubocop:disable Lint/RescueException -- rule 6 re-raises any escaped failure
          settled = true
          @failure = e
          @recorder&.record_failure(e) if e.is_a?(StandardError)
          raise
        ensure
          # Neither returned nor raised. A new failure is recorded only when a
          # layer re-raises it, so a killed fiber records nothing.
          unless settled
            @failure = @handed_up || InterceptorStateError.new("Continuation exited without returning or raising")
          end
        end
      end

      # Runs on the owner fiber for every exit and returns whether the layer
      # yielded. A latched misuse is recorded here, before any outer layer can
      # translate the exit. The enclosing continuation adopts what this layer
      # carries only if the layer exited non-locally.
      def close(parent)
        yielded = @mutex.synchronize do
          claimed = @state == :claimed
          @state = :finished
          claimed
        end
        # A finished continuation latches nothing more, so this read is stable.
        misuse = @misuse
        record_misuse(misuse) if misuse
        parent&.hand_up(@failure || misuse)
        yielded
      end

      # Unwind order after a normal return: a continuation failure, a latched
      # misuse, then the result.
      def finish(result, yielded, mode)
        raise_latched
        return Interceptors.response!(result) if mode == :response
        raise InterceptorStateError, "Server and stream interceptors must yield" unless yielded

        case mode
        when :stream_response then Interceptors.response!(result)
        when :value then result
        end
      end

      # What the inner layer's exit carried: its continuation failure or
      # latched misuse.
      def hand_up(failure)
        @handed_up = failure
        nil
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
        record_misuse(misuse) if misuse && owner && !late
        misuse
      end

      # Runs on the owner fiber. Nothing is recorded over a continuation
      # failure, including a non-StandardError that is recorded only after it
      # leaves the chain.
      def record_misuse(misuse)
        @recorder&.record_failure(misuse) unless @failure
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
    end
  end
end
