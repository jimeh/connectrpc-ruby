# frozen_string_literal: true

require "uri"

module ConnectRPC
  module Async
    # Connect unary and server streaming, and native gRPC unary and server
    # streaming, over one HTTP/2 connection per call. Each call dials the
    # scheme, host and port of its request URL, or tunnels to them through
    # its proxy, runs one exchange and closes the connection with GOAWAY
    # before it returns. Inside an Async task the call runs inline on the
    # caller's reactor; elsewhere it runs on its own short-lived owner
    # thread, which has finished when the call returns.
    class PerCallTransport
      # One setup attempt, no byte budget and no keepalive: a failed attempt
      # fails the call.
      POLICY = ConnectionPolicy.new(backoff: nil, max_pending_bytes: nil)
      INVALID_URL = "Request URL must be HTTP(S) with a host and no userinfo"
      # ConnectRPC::Deadline's default clock.
      DEADLINE_CLOCK = Deadline.const_get(:MONOTONIC)
      private_constant :POLICY, :INVALID_URL, :DEADLINE_CLOCK

      # Dials the call's proxy, when it has one, and tunnels to the origin
      # through it once the socket is published. A proxy that cannot be used
      # fails the setup attempt before any socket opens.
      module Tunnelling
        private

        def dial
          @tunnel_proxy = Internal::ProxyTunnel.select(@proxy, @origin)
          @tunnel_proxy ? super(@tunnel_proxy.host, @tunnel_proxy.port) : super
        end

        def tunnel(raw)
          return unless @tunnel_proxy

          Internal::ProxyTunnel.establish(raw, @origin.hostname, @origin.port, @tunnel_proxy,
                                          tls: @origin.scheme == "https")
        end
      end
      private_constant :Tunnelling

      # The H2 Transport for one inline call. The wrapper's scope task, rather
      # than the caller's task, owns its guard, watcher, closer and every
      # connection task, so the wrapper can join all of them.
      class Inline < Transport
        include Tunnelling

        def initialize(scope:, snapshot:, proxy:, **)
          @scope = scope
          @compiled_snapshot = snapshot
          @proxy = proxy
          super(**)
        end

        # Closes within `grace` and returns an exception raised into the
        # caller's fiber meanwhile. A failure of the closer task itself is a
        # close failure, which the call ignores whatever its class.
        def close_retaining(grace)
          close(grace: grace)
          nil
        rescue Exception => e # rubocop:disable Lint/RescueException -- the caller re-raises its own interruption after teardown
          e unless @closer&.failed? && @closer.result.equal?(e)
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
        # The owner thread's inner Transport.
        class Tunnelled < Inner
          include Tunnelling

          def initialize(proxy:, **)
            @proxy = proxy
            super(**)
          end
        end
        private_constant :Tunnelled

        def initialize(snapshot:, proxy:, **)
          @compiled_snapshot = snapshot
          @proxy = proxy
          @engines = []
          @cutoff = nil
          super(**)
        end

        # Closes within `grace`, then joins every owner thread this call
        # started, including one that stopped before the close began. The
        # close's expiry never passes `cutoff`, or the start of the close if
        # that is later, so the owner always has one cleanup allowance from
        # that point: the close stops waiting for a stalled owner then, and
        # that owner exits once it resumes. An interrupt of the close wait
        # still joins the owners, within the same bound, before it propagates.
        def close_joined(grace, cutoff)
          @cutoff = cutoff && [cutoff, Internal.now].max
          close(grace: grace)
        ensure
          join_owners
        end

        private

        def build_inner(**) = Tunnelled.new(proxy: @proxy, **)

        # A start that fails can replace a stopped engine; each one is joined.
        def build_engine(admission) = super.tap { |engine| @engines << engine }

        def begin_close(expires) = super(@cutoff ? [expires, @cutoff].min : expires)

        def join_owners
          expires = (@close_expires || Internal.now) + Internal::CLEANUP_SECONDS
          @engines.each do |engine|
            thread = engine.thread
            thread.join([expires - Internal.now, 0].max) unless thread.equal?(Thread.current)
          end
        end

        def client_snapshot(**) = @compiled_snapshot
      end
      private_constant :Threaded

      # proxy: :environment snapshots the proxy environment now; each call
      # takes the proxy for its URL's scheme, as NetHTTP does: HTTP_PROXY for
      # h2c and HTTPS_PROXY for TLS, minus NO_PROXY. A String or URI is an
      # http:// proxy for every call; nil connects directly.
      def initialize(ca_file: nil, certificate_chain: nil, private_key: nil, connect_timeout: 10,
                     proxy: :environment)
        @options = {
          connection_limit: 1, max_requests: 1, connection_policy: POLICY,
          connect_timeout: Internal.seconds(connect_timeout, "connect_timeout")
        }.freeze
        @tls_options = !(ca_file.nil? && certificate_chain.nil? && private_key.nil?)
        # Compiled once, with system roots and any given CA, and shared by
        # every HTTPS call.
        @snapshot = Internal::TLS.client(ca_file: ca_file, certificate_chain: certificate_chain,
                                         private_key: private_key)
        @proxy_mode = { environment: "environment", nil => "direct" }.fetch(proxy, "explicit")
        @proxy = Proxy.resolver(proxy)
        fixed = @proxy.is_a?(Proxy::Static) ? @proxy.endpoint : nil
        raise ArgumentError, "Unsupported proxy scheme", cause: nil if fixed && fixed.scheme != "http"

        freeze
      end

      # Never shows proxy URLs, credentials or TLS material.
      def inspect = "#<#{self.class} connect_timeout=#{@options.fetch(:connect_timeout)} proxy=#{@proxy_mode}>"

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
        if ::Async::Task.current?
          inline(origin, request, &)
        else
          threaded(origin, request, &)
        end
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

      # Returns the graceful close budget and the deadline as a monotonic
      # time. Core checks the deadline again once the transport returns, so
      # the wait for the peer's FIN ends one cleanup allowance before the
      # deadline, which leaves that allowance for the raw close and the joins;
      # with less time left the close is raw. On the default clock, which
      # Internal.now also reads, the cutoff is the deadline's expiry, live or
      # past. A custom clock has no monotonic instant, so a live deadline
      # places the cutoff by its remaining time, read after the clock so a
      # slow clock shortens the budget rather than leaving it already spent,
      # and past its expiry the cutoff is now.
      def close_plan(deadline)
        return [Internal::CLEANUP_SECONDS, nil] unless deadline

        if deadline.__send__(:clock).equal?(DEADLINE_CLOCK)
          now = Internal.now
          cutoff = deadline.__send__(:expires_at)
        else
          remaining = deadline.remaining
          now = Internal.now
          cutoff = remaining.positive? ? now + remaining : now
        end
        [(cutoff - now - Internal::CLEANUP_SECONDS).clamp(0.0, Internal::CLEANUP_SECONDS), cutoff]
      end

      # The deadline's clock is the caller's code. Whatever it raises, the
      # close falls back to a raw one and the call keeps its own outcome.
      def close_plan_or_raw(deadline)
        close_plan(deadline)
      rescue Exception # rubocop:disable Lint/RescueException -- a failing clock must not skip teardown
        [0.0, Internal.now]
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
          inner = Inline.new(scope: scope, origin: origin, snapshot: @snapshot, proxy: @proxy, **@options)
          yield inner
        ensure
          interruption = finish_inline(inner, scope, release, request.deadline)
          raise interruption if interruption
        end
      end

      # A close failure is ignored. A caller interruption while the close
      # budget is computed, during close or during the join is retained until
      # every task has finished, then returned. The budget is computed in the
      # caller's own fiber, so a clock that reads fiber-local state sees the
      # caller's; whatever it raises, the close is raw and still runs.
      def finish_inline(inner, scope, release, deadline)
        interruption = nil
        begin
          if inner
            begin
              grace = close_plan(deadline).first
            rescue Exception => e # rubocop:disable Lint/RescueException -- the caller re-raises its own interruption after teardown
              grace = 0.0
              interruption = e unless clock_failure?(deadline, e)
            end
            closing = inner.close_retaining(grace)
            interruption ||= closing
          end
        ensure
          release.resolve(nil)
          joining = false
          begin
            # A construction cut short can leave a guard without its watcher,
            # which only a cancel ends.
            Internal.cancel(scope) unless inner
            Internal.join_complete(scope) { joining = true }
          rescue Exception => e # rubocop:disable Lint/RescueException -- the caller re-raises its own interruption after teardown
            interruption ||= e
            # Raised before the join took over, as during the cancel, so the
            # scope may still run: cancel again and join.
            retry unless joining || scope.finished?
          end
        end
        interruption
      end

      # Whether an exception raised while the inline close budget was computed
      # is the deadline clock's own failure rather than the caller's
      # interruption. The default clock cannot fail, so with it, or with no
      # deadline, the exception is the caller's. A custom clock's
      # StandardError is its failure unless it is the caller's own Async
      # timeout. A StandardError raised into the caller while a custom clock
      # runs looks the same and is taken for a clock failure.
      def clock_failure?(deadline, error)
        return false unless deadline && !deadline.__send__(:clock).equal?(DEADLINE_CLOCK)

        error.is_a?(StandardError) && !Internal.foreign_timeout?(error)
      end

      # Close runs with interrupts deferred; its own waits accept them and
      # the owner joins then finish before they propagate. Deferred, no
      # interrupt can land while the close budget is computed, so anything
      # raised there is the deadline clock's own failure. A clock that leaves
      # without raising, by throw or by killing its thread, still gets the
      # raw close.
      def threaded(origin, request)
        inner = Threaded.new(origin: origin, snapshot: @snapshot, proxy: @proxy, **@options)
        Thread.handle_interrupt(Object => :never) do
          Thread.handle_interrupt(Object => :immediate) { yield inner }
        ensure
          plan = nil
          begin
            plan = close_plan_or_raw(request.deadline)
          ensure
            # Sampled after the clock, however long it ran.
            inner.close_joined(*(plan || [0.0, Internal.now]))
          end
        end
      end
    end
  end
end
