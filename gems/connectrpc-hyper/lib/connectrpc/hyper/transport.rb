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
      private_constant :STREAMING, :HTTP1_STREAMING, :GRPC, :NO_GRPC, :CONNECTIONS

      # rubocop:disable-next Metrics/MethodLength
      def initialize(connections: :pooled, idle_timeout: 1800, keepalive: nil, http2: true, connect_timeout: 20,
                     ca_file: nil, certificate_chain: nil, private_key: nil)
        raise ArgumentError, "connections must be :pooled or :per_call" unless CONNECTIONS.include?(connections)
        raise ArgumentError, "keepalive must be a Keepalive or nil" unless keepalive.nil? || keepalive.is_a?(Keepalive)
        raise ArgumentError, "http2 must be true or false" unless [true, false].include?(http2)

        @connections = connections
        @idle_timeout = optional_seconds(idle_timeout, "idle_timeout")
        @connect_timeout = optional_seconds(connect_timeout, "connect_timeout")
        @keepalive = keepalive
        @http2 = http2
        @tls = TLS.inputs(ca_file: ca_file, certificate_chain: certificate_chain, private_key: private_key)
        @lock = Mutex.new
        @closed = ConditionVariable.new
        @state = :open
        # Building the driver imports the TLS identity into the provider, so
        # an identity aws-lc-rs cannot use fails here. It starts nothing.
        build_driver
      end

      def inspect
        "#<#{self.class} connections=#{@connections} http2=#{@http2} tls=#{!@tls.nil?} state=#{state}>"
      end

      def streaming_capabilities = @http2 ? STREAMING : HTTP1_STREAMING
      def grpc_capabilities = @http2 ? GRPC : NO_GRPC

      def call(request)
        CallCancellation.with_request(request) { |outgoing| perform_call(outgoing) }
      end

      def open_stream(request, &)
        raise ArgumentError, "A streaming block is required" unless block_given?

        CallCancellation.with_request(request) { |outgoing| perform_stream(outgoing, grpc: false, &) }
      end

      def open_grpc_stream(request, &)
        raise ArgumentError, "A gRPC block is required" unless block_given?

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

      def optional_seconds(value, name)
        return if value.nil?
        return value if value.is_a?(Numeric) && value.real? && value.finite? && value.positive?

        raise ArgumentError, "#{name} must be finite positive seconds or nil"
      end

      def close_masked(grace) # rubocop:disable Metrics/MethodLength
        driver = nil
        @lock.synchronize do
          inherit_fork_state
          @closed.wait(@lock) while @state == :closing
          return @close_outcome if @state == :closed

          @state = :closing
          @close_pid = Process.pid
          driver = @driver if @driver_pid == Process.pid
        end
        outcome = false
        begin
          outcome = driver ? driver.close(grace.to_f) : true
        ensure
          @lock.synchronize { finish_close(outcome) }
        end
        outcome
      end

      def finish_close(outcome)
        @close_outcome = outcome
        @state = :closed
        @closed.broadcast
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
        keepalive = @keepalive && [@keepalive.time.to_f, @keepalive.timeout.to_f, @keepalive.permit_without_calls]
        roots, chain, key = @tls
        @driver = Native::Driver.new(@connections == :pooled, @http2, @idle_timeout&.to_f, @connect_timeout&.to_f,
                                     keepalive, roots || [], chain, key)
        @driver_pid = Process.pid
      rescue Native::Failure => e
        raise unless e.kind == :tls_config

        raise ArgumentError, "Invalid TLS client configuration: #{e.message}", cause: nil
      end

      def check_request!(request)
        request.cancellation ? request.cancellation.check! : request.deadline&.check!
      end

      def check_origin!(url)
        return if url.scheme == "https" || @tls.nil?

        raise ArgumentError, "TLS options require an HTTPS origin"
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
        check_origin!(request.url)
        check_request!(request)
        native = driver
        with_cancel(request) do |cancel|
          status, headers, body = Errors.native(request, grpc: false) do
            native.unary(request.http_method, request.url.to_s, fields(request.headers),
                         request.http_method == "GET" ? nil : request.body, request.response_limit,
                         request.deadline ? :connect : nil, request.deadline&.remaining, cancel)
          end
          TransportResponse.new(status: status, headers: headers, body: body)
        end
      end

      def perform_stream(request, grpc:) # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
        check_origin!(request.url)
        check_request!(request)
        native = driver
        with_cancel(request) do |cancel|
          call = nil
          begin
            call = Errors.native(request, grpc: grpc) do
              native.open(request.url.to_s, fields(request.headers), (if request.deadline
                                                                        grpc ? :grpc : :connect
                                                                      end),
                          request.deadline&.remaining, grpc, @http2, cancel)
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
