# frozen_string_literal: true

module ConnectRPC
  module Hyper
    # Connect and native gRPC calls of every shape from any Ruby thread,
    # without an Async reactor. One shared HTTP/2 connection per origin serves
    # every thread, answers PINGs and GOAWAY while idle and is replaced after a
    # server restart. Construction validates options and starts no thread or
    # socket; a forked child builds its own native driver on first use.
    class Transport # rubocop:disable Metrics/ClassLength
      STREAMING = %i[server_stream client_stream bidi_stream].freeze
      HTTP1_STREAMING = %i[server_stream client_stream].freeze
      GRPC = %i[unary server_stream client_stream bidi_stream].freeze
      NO_GRPC = [].freeze
      CONNECTIONS = %i[pooled per_call].freeze
      # Go net/http bounds a proxy CONNECT exchange by one minute and its
      # response head by MaxResponseHeaderBytes' default, as core NetHTTP does.
      TUNNEL_TIMEOUT = 60
      MAX_TUNNEL_HEAD = 10 * 1024 * 1024
      UNUSABLE_PROXY = { cgi: :proxy_cgi, invalid_address: :proxy_address }.freeze
      private_constant :STREAMING, :HTTP1_STREAMING, :GRPC, :NO_GRPC, :CONNECTIONS, :TUNNEL_TIMEOUT,
                       :MAX_TUNNEL_HEAD, :UNUSABLE_PROXY

      # proxy: :environment snapshots HTTP_PROXY, HTTPS_PROXY and NO_PROXY now
      # (Go net/http rules, through core's ConnectRPC::Proxy). A String or URI
      # proxies every request; nil connects directly. Only http:// proxies.
      # rubocop:disable-next Metrics/MethodLength
      def initialize(connections: :pooled, idle_timeout: 1800, keepalive: nil, http2: true, connect_timeout: 10,
                     ca_file: nil, certificate_chain: nil, private_key: nil, proxy: :environment)
        raise ArgumentError, "connections must be :pooled or :per_call" unless CONNECTIONS.include?(connections)
        raise ArgumentError, "keepalive must be a Keepalive or nil" unless keepalive.nil? || keepalive.is_a?(Keepalive)
        raise ArgumentError, "http2 must be true or false" unless [true, false].include?(http2)

        @connections = connections
        @idle_timeout = optional_seconds(idle_timeout, "idle_timeout")
        @connect_timeout = seconds(connect_timeout, "connect_timeout")
        @keepalive = keepalive
        @http2 = http2
        @tls = TLS.inputs(ca_file: ca_file, certificate_chain: certificate_chain, private_key: private_key)
        @proxy = proxy_resolver(proxy)
        @lock = Mutex.new
        @state = :open
        # Building the driver imports the TLS identity into the provider, so
        # an identity aws-lc-rs cannot use fails here. It starts nothing.
        build_driver
      end

      # Never shows proxy URLs, credentials or TLS material.
      def inspect
        "#<#{self.class} connections=#{@connections} http2=#{@http2} tls=#{!@tls.nil?} proxy=#{proxy_mode} " \
          "state=#{state}>"
      end

      def streaming_capabilities = @http2 ? STREAMING : HTTP1_STREAMING
      def grpc_capabilities = @http2 ? GRPC : NO_GRPC

      def call(request)
        check_origin!(request.url)
        CallCancellation.with_request(request) { |outgoing| perform_call(outgoing) }
      end

      def open_stream(request, &)
        raise ArgumentError, "A streaming block is required" unless block_given?

        check_origin!(request.url)
        CallCancellation.with_request(request) { |outgoing| perform_stream(outgoing, grpc: false, &) }
      end

      def open_grpc_stream(request, &)
        raise ArgumentError, "A gRPC block is required" unless block_given?

        check_origin!(request.url)
        CallCancellation.with_request(request) { |outgoing| perform_stream(outgoing, grpc: true, &) }
      end

      # Stops admission (later calls fail `unavailable` with the closed
      # phrase), lets in-flight calls finish within `grace`, then aborts the
      # rest and waits for every owned connection task. True when everything
      # drained; false after forced teardown. Repeated and concurrent calls
      # share the first outcome.
      def close(grace: 5)
        unless grace.is_a?(Numeric) && grace.real? && grace.finite? && grace >= 0
          raise ArgumentError, "grace must be finite nonnegative seconds"
        end

        Thread.handle_interrupt(Object => :never) { close_masked(grace) }
      end

      private

      # Core NetHTTP's validation: an explicit proxy must be http://.
      def proxy_resolver(option)
        resolver = Proxy.resolver(option)
        fixed = resolver.is_a?(Proxy::Static) ? resolver.endpoint : nil
        raise ArgumentError, "Unsupported proxy scheme", cause: nil if fixed && fixed.scheme != "http"

        resolver
      end

      def proxy_mode
        return "environment" unless @proxy.is_a?(Proxy::Static)

        @proxy.endpoint ? "explicit" : "direct"
      end

      def optional_seconds(value, name)
        return if value.nil?

        seconds(value, name, nil_allowed: true)
      end

      # connectrpc-async's rule (ConnectRPC::Async::Internal.seconds).
      def seconds(value, name, nil_allowed: false)
        return value if value.is_a?(Numeric) && value.real? && value.finite? && value.positive?

        raise ArgumentError, "#{name} must be finite positive seconds#{' or nil' if nil_allowed}"
      end

      # Every closer in this process, concurrent ones included, waits in
      # native close: it starts teardown once, gives every closer its
      # published outcome and holds an interrupt or a fiber scheduler's
      # cancellation until then. The lock covers only the state and the
      # driver handoff.
      def close_masked(grace) # rubocop:disable Metrics/MethodLength
        driver = nil
        @lock.synchronize do
          inherit_fork_state
          return @close_outcome if @state == :closed
          # A forked child that never built its own driver has nothing to drain.
          return finish_close(true) unless @driver_pid == Process.pid

          @state = :closing
          @close_pid = Process.pid
          driver = @driver
        end
        outcome = nil
        begin
          outcome = driver.close(grace.to_f)
        ensure
          # Native close raises only after teardown published its outcome.
          outcome = driver.close_outcome == true if outcome.nil?
          @lock.synchronize { finish_close(outcome) }
        end
        outcome
      end

      def finish_close(outcome)
        @state = :closed
        @close_outcome = outcome
      end

      # A child that inherits another process's pending close has no driver
      # to drain; a transport closed before fork stays closed.
      def inherit_fork_state
        finish_close(true) if @state == :closing && @close_pid != Process.pid
      end

      def state
        @state == :closing && @close_pid != Process.pid ? :closed : @state
      end

      # The driver for this process: a forked child never touches its
      # parent's native state.
      def driver
        @lock.synchronize do
          raise Error.new(:unavailable, Errors::CLOSED), cause: nil unless state == :open

          build_driver unless @driver_pid == Process.pid
          @driver
        end
      end

      def build_driver
        roots, chain, key = @tls
        @driver = Native::Driver.new(@connections == :pooled, @http2, @idle_timeout&.to_f, @connect_timeout.to_f,
                                     native_keepalive, roots || [], chain, key, tunnel_limits)
        @driver_pid = Process.pid
      rescue Native::Failure => e
        raise unless e.kind == :tls_config

        raise ArgumentError, "Invalid TLS client configuration: #{e.message}", cause: nil
      end

      def native_keepalive
        @keepalive && [@keepalive.time.to_f, @keepalive.timeout.to_f, @keepalive.permit_without_calls]
      end

      # [CONNECT exchange seconds, response head bytes].
      def tunnel_limits = [TUNNEL_TIMEOUT.to_f, MAX_TUNNEL_HEAD]

      def check_request!(request)
        request.cancellation ? request.cancellation.check! : request.deadline&.check!
      end

      def check_origin!(url)
        return if url.scheme == "https" || @tls.nil?

        raise ArgumentError, "TLS options require an HTTPS origin"
      end

      # [host, port, Proxy-Authorization or nil] for the request's proxy, or
      # nil for a direct connection. An unusable environment proxy fails the
      # call before any socket opens; it never falls back to a direct one.
      def proxy_route(url)
        endpoint = begin
          @proxy.for(url)
        rescue Proxy::Unusable => e
          raise Errors.proxy(UNUSABLE_PROXY.fetch(e.reason)), cause: nil
        end
        return unless endpoint
        raise Errors.proxy(:proxy_scheme), cause: nil unless endpoint.scheme == "http"

        credentials = "Basic #{["#{endpoint.user}:#{endpoint.password}"].pack('m0')}" if endpoint.user
        [endpoint.host, endpoint.port, credentials]
      end

      def fields(headers)
        headers.flat_map { |name, values| values.map { |value| [name, value.to_s] } }
      end

      # The registration exists before the first native wait, so a token
      # cancels resolution, connect, TLS and waiting behind another dial.
      def with_cancel(request)
        cancel = Native::Cancel.new
        registration = request.cancellation&.on_cancel { cancel.fire }
        begin
          yield cancel
        ensure
          registration&.unregister
        end
      end

      def perform_call(request) # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
        check_request!(request)
        native = driver
        with_cancel(request) do |cancel|
          status, headers, body = Errors.native(request, grpc: false) do
            native.unary(request.http_method, request.url.to_s, fields(request.headers),
                         request.http_method == "GET" ? nil : request.body, request.response_limit,
                         request.deadline ? :connect : nil, request.deadline&.remaining, cancel,
                         proxy_route(request.url))
          end
          TransportResponse.new(status: status, headers: headers, body: body)
        end
      end

      def perform_stream(request, grpc:) # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
        check_request!(request)
        native = driver
        with_cancel(request) do |cancel|
          call = nil
          begin
            call = Errors.native(request, grpc: grpc) do
              native.open(request.url.to_s, fields(request.headers), (if request.deadline
                                                                        grpc ? :grpc : :connect
                                                                      end),
                          request.deadline&.remaining, grpc, @http2, cancel, proxy_route(request.url))
            end
            yield Exchange.new(call, request, grpc: grpc)
          ensure
            # ensure, not rescue: Thread#kill runs ensure blocks only. abort
            # never waits and does nothing after a finished exchange.
            Thread.handle_interrupt(Object => :never) { call&.abort }
          end
        end
      end
    end
  end
end
