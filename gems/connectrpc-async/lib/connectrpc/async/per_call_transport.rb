# frozen_string_literal: true

require "uri"

module ConnectRPC
  module Async
    # Connect unary and server streaming, and native gRPC unary and server
    # streaming, over one HTTP/2 connection per call. Each call dials the
    # scheme, host and port of its request URL, runs one exchange and closes
    # the connection with GOAWAY before it returns. Inside an Async task the
    # call runs inline on the caller's reactor; elsewhere it runs on its own
    # short-lived owner thread, which has finished when the call returns.
    class PerCallTransport
      # One setup attempt, no byte budget and no keepalive: a failed attempt
      # fails the call.
      POLICY = ConnectionPolicy.new(backoff: nil, max_pending_bytes: nil)
      INVALID_URL = "Request URL must be HTTP(S) with a host and no userinfo"
      private_constant :POLICY, :INVALID_URL

      # The H2 Transport for one inline call. The wrapper's scope task, rather
      # than the caller's task, owns its guard, watcher, closer and every
      # connection task, so the wrapper can join all of them.
      class Inline < Transport
        def initialize(scope:, snapshot:, **)
          @scope = scope
          @compiled_snapshot = snapshot
          super(**)
        end

        private

        def initialize_affinity
          super
          @owner = @scope
        end

        def client_snapshot(**) = @compiled_snapshot
      end
      private_constant :Inline

      # The ThreadedTransport for one call from an ordinary thread.
      class Threaded < ThreadedTransport
        def initialize(snapshot:, **)
          @compiled_snapshot = snapshot
          super(**)
        end

        # Closes within `grace` and joins the owner thread. An interrupt of the
        # close wait still joins the owner within the close's own bound before
        # it propagates; a stalled owner is left to exit once it resumes.
        def close_joined(grace)
          closed = false
          close(grace: grace)
          closed = true
        ensure
          join_owner unless closed
        end

        private

        def join_owner
          thread = @engine&.thread
          return unless thread && !thread.equal?(Thread.current)

          expires = (@close_expires || Internal.now) + Internal::CLEANUP_SECONDS
          thread.join([expires - Internal.now, 0].max)
        end

        def client_snapshot(**) = @compiled_snapshot
      end
      private_constant :Threaded

      def initialize(ca_file: nil, certificate_chain: nil, private_key: nil, connect_timeout: 10)
        @options = {
          connection_limit: 1, max_requests: 1, connection_policy: POLICY,
          connect_timeout: Internal.seconds(connect_timeout, "connect_timeout")
        }.freeze
        @tls_options = !(ca_file.nil? && certificate_chain.nil? && private_key.nil?)
        # Compiled once, with system roots unless a CA is given, and shared by
        # every HTTPS call.
        @snapshot = Internal::TLS.client(ca_file: ca_file, certificate_chain: certificate_chain,
                                         private_key: private_key)
        freeze
      end

      def inspect = "#<#{self.class} connect_timeout=#{@options.fetch(:connect_timeout)}>"

      def call(request) = per_call(request) { |inner| inner.call(request) }

      def streaming_capabilities = %i[server_stream].freeze

      def open_stream(request, &)
        raise ArgumentError, "A streaming block is required" unless block_given?

        per_call(request) { |inner| inner.open_stream(request, &) }
      end

      def grpc_capabilities = %i[unary server_stream].freeze

      def open_grpc_stream(request, &)
        raise ArgumentError, "A gRPC block is required" unless block_given?

        per_call(request) { |inner| inner.open_grpc_stream(request, &) }
      end

      private

      def per_call(request, &)
        origin = origin_of(request.url)
        ::Async::Task.current? ? inline(origin, request, &) : threaded(origin, request, &)
      end

      # Validates before any I/O. The request URL itself, with its path prefix
      # and query, reaches the inner transport unchanged.
      def origin_of(url)
        unless url.is_a?(URI::HTTP) && %w[http https].include?(url.scheme) && url.host && !url.host.empty? &&
               !url.userinfo
          raise ArgumentError, INVALID_URL
        end
        raise ArgumentError, "TLS options require an HTTPS origin" if url.scheme == "http" && @tls_options

        url.origin
      end

      # Core checks the deadline again once the transport returns, so the
      # graceful close never outlives it; an expired call closes raw.
      def close_grace(request)
        remaining = request.deadline&.remaining
        remaining ? [remaining, Internal::CLEANUP_SECONDS].min : Internal::CLEANUP_SECONDS
      end

      # The scope task waits until teardown releases it. Construction, the
      # call and close run in the caller's fiber; the join covers every task
      # the call created, including after an interrupted construction or close.
      def inline(origin, request)
        release = ::Async::Promise.new
        scope = ::Async::Task.new(::Async::Task.current, finished: false) { release.wait }
        inner = nil
        begin
          scope.run
          inner = Inline.new(scope: scope, origin: origin, snapshot: @snapshot, **@options)
          yield inner
        ensure
          interruption = finish_inline(inner, scope, release, close_grace(request))
          raise interruption if interruption
        end
      end

      # A close failure is ignored. A caller interruption during close or the
      # join is retained until every task has finished, then returned.
      def finish_inline(inner, scope, release, grace)
        interruption = nil
        begin
          inner&.close(grace: grace)
        rescue Exception => e # rubocop:disable Lint/RescueException -- retain a caller interruption until teardown ends
          interruption = e unless e.is_a?(StandardError) && !Internal.foreign_timeout?(e)
        ensure
          release.resolve(nil)
          # A construction cut short can leave a guard without its watcher.
          Internal.cancel(scope) unless inner
          begin
            Internal.join_complete(scope)
          rescue Exception => e # rubocop:disable Lint/RescueException -- the join already finished every task
            interruption ||= e
          end
        end
        interruption
      end

      # Close runs with interrupts deferred; its own waits accept them and
      # the owner join then finishes before they propagate.
      def threaded(origin, request)
        inner = Threaded.new(origin: origin, snapshot: @snapshot, **@options)
        Thread.handle_interrupt(Object => :never) do
          Thread.handle_interrupt(Object => :immediate) { yield inner }
        ensure
          inner.close_joined(close_grace(request))
        end
      end
    end
  end
end
