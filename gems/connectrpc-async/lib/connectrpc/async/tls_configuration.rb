# frozen_string_literal: true

module ConnectRPC
  module Async
    # Worker-local source supervision. Published snapshots never escape through
    # status, diagnostics or caller-visible mutable OpenSSL contexts.
    class TLSConfiguration
      class Error < ArgumentError
        attr_reader :category

        def initialize(category)
          @category = category
          super("TLS configuration failed (#{category})")
        end
      end

      REQUIRED_KEYS = %i[version certificate_chain private_key client_ca].freeze
      private_constant :REQUIRED_KEYS

      def initialize(tls: nil, tls_provider: nil, refresh_interval: 60, refresh_jitter: 0.1,
                     refresh_timeout: 10, on_error: nil, alpn_protocols: ["h2"])
        @owner = ::Async::Task.current?
        raise LifecycleError, "An application-owned Async task is required" unless @owner
        raise ArgumentError, "tls must be a Hash" unless tls.nil? || tls.is_a?(Hash)
        raise ArgumentError, "Choose tls or tls_provider" unless tls.nil? || tls_provider.nil?
        raise ArgumentError, "tls_provider must be callable" unless tls_provider.nil? || tls_provider.respond_to?(:call)
        raise ArgumentError, "on_error must be callable" unless on_error.nil? || on_error.respond_to?(:call)

        @interval = Internal.seconds(refresh_interval, "refresh_interval")
        @timeout = Internal.seconds(refresh_timeout, "refresh_timeout")
        unless refresh_jitter.is_a?(Numeric) && refresh_jitter.real? && refresh_jitter.finite? &&
               refresh_jitter >= 0 && refresh_jitter < 1
          raise ArgumentError, "refresh_jitter must be finite and in [0,1)"
        end
        unless alpn_protocols.is_a?(Array) && !alpn_protocols.empty? &&
               alpn_protocols.uniq == alpn_protocols && (alpn_protocols - %w[h2 http/1.1]).empty?
          raise ArgumentError, "Unsupported ALPN configuration"
        end

        @thread = Thread.current
        @reactor = Fiber.scheduler
        @static = tls
        @provider = tls_provider
        @reporter = on_error
        @jitter = refresh_jitter
        @alpn = alpn_protocols.map { |value| value.dup.freeze }.freeze
        @state = :new
        @timer_progress = ::Async::Condition.new
        @lifetime = @owner.async do
          ::Async::Condition.new.wait
        ensure
          shutdown unless @state == :closed
        end
      end

      def inspect = "#<#{self.class} state=#{@state} generation=#{@generation.inspect}>"
      attr_reader :generation

      def status
        check_affinity!
        now = Internal.now
        { state: @state, generation: @generation, category: @category,
          source_check_age: @checked_at && (now - @checked_at),
          publication_age: @published_at && (now - @published_at) }.freeze
      end

      def start
        check_affinity!
        check_recursion!
        raise LifecycleError, "TLS configuration is closed" if %i[closing closed].include?(@state)
        return self if @state == :running

        @state = :starting
        if @provider
          join_attempt
        elsif @static
          publish(compile(@static), nil)
        end
        raise LifecycleError, "TLS configuration closed during startup" unless @state == :starting

        @state = :running
        start_timer if @provider && !@timer
        self
      rescue LifecycleError
        raise
      rescue Error
        @state = :new unless %i[closing closed].include?(@state)
        raise
      rescue StandardError
        @state = :new unless %i[closing closed].include?(@state)
        fail_safely(:invalid)
      end

      def refresh
        check_affinity!
        check_recursion!
        raise LifecycleError, "A running provider is required" unless @provider && @state == :running

        join_attempt
        @generation
      end

      def reload(certificate_chain:, private_key:, client_ca:)
        check_affinity!
        raise LifecycleError, "Provider owns TLS configuration" if @provider
        raise LifecycleError, "A running TLS configuration is required" unless @state == :running && @snapshot

        publish(compile(certificate_chain: certificate_chain, private_key: private_key, client_ca: client_ca), nil)
        @generation
      rescue Error
        raise
      rescue ArgumentError
        fail_safely(:invalid)
      end

      def close
        check_affinity!
        check_recursion!
        shutdown
        self
      end

      # Hosting SPI: captures an opaque socket owner synchronously at raw accept.
      # It exposes no parsed credentials or mutable SSLContext.
      def capture(raw)
        check_affinity!
        raise LifecycleError, "TLS admission is closed" unless @state == :running

        @snapshot&.check_validity!
        Capture.new(raw, @snapshot, @generation)
      end
      private :capture

      class Capture
        attr_reader :generation

        def initialize(raw, snapshot, generation)
          @raw = raw
          @snapshot = snapshot
          @generation = generation
        end

        def handshake
          @snapshot&.check_validity!
          @socket ||= @snapshot ? @snapshot.socket(@raw) : @raw
          yield @socket if block_given?
          @socket.accept if @snapshot
          @socket
        end

        def authenticated_leaf
          @socket.peer_cert.to_der.freeze if @snapshot&.authenticated?
        end

        def tls? = !@snapshot.nil?
        def inspect = "#<#{self.class} generation=#{@generation.inspect}>"
      end
      private_constant :Capture

      private

      def check_affinity!
        return if Thread.current.equal?(@thread) && Fiber.scheduler.equal?(@reactor) && ::Async::Task.current?

        raise LifecycleError, "Use the owning Async reactor and thread"
      end

      def check_recursion!
        return unless @callback&.current? || @attempt&.current?

        raise LifecycleError, "Recursive TLS provider lifecycle call"
      end

      def compile(options)
        Internal::TLS.server(**options, alpn_protocols: @alpn)
      rescue StandardError
        raise Error.new(:invalid), cause: nil
      end

      def publish(snapshot, version)
        @snapshot = snapshot
        @version = version
        @generation = (@generation || 0) + 1
        @published_at = @checked_at = Internal.now
        @category = nil
      end

      def join_attempt
        attempt = @attempt
        unless attempt
          attempt = ::Async::Task.new(@lifetime, finished: false) { perform_attempt(attempt) }
          @attempt = attempt
          attempt.run
        end
        # Cancellation of a waiter never owns or cancels this shared task.
        category = attempt.wait
        category ||= :closed unless %i[starting running].include?(@state)
        raise Error.new(category), cause: nil if category
      end

      def perform_attempt(attempt)
        @callback = ::Async::Task.new(attempt, finished: false) do
          result = @provider.call
          unless result.is_a?(Hash) && result.size == REQUIRED_KEYS.size &&
                 REQUIRED_KEYS.all? { |key| result.key?(key) } &&
                 result[:version].is_a?(String) && !result[:version].empty?
            next [:invalid]
          end

          version = result.fetch(:version).dup.freeze
          snapshot = compile(result.except(:version))
          [:ok, version, snapshot]
        rescue Error
          [:invalid]
        rescue StandardError, ScriptError
          [:source]
        end
        @callback.run
        result, version, snapshot = Internal.wait(@callback, Internal.now + @timeout)
        return :closed unless @attempt.equal?(attempt) && %i[starting running].include?(@state)

        if result != :ok
          category = %i[invalid source].include?(result) ? result : :source
          report(category)
          return category
        end
        if version == @version
          if snapshot.fingerprint != @snapshot.fingerprint
            report(:version_conflict)
            return :version_conflict
          end
          @checked_at = Internal.now
          @category = nil
        else
          publish(snapshot, version)
        end
        nil
      rescue ::Async::TimeoutError
        report(:timeout)
        :timeout
      ensure
        Internal.cancel(@callback)
        Internal.join(@callback)
        @callback = nil
        @attempt = nil if @attempt.equal?(attempt)
        @next_refresh = Internal.now + (@interval * (1 + (((rand * 2) - 1) * @jitter)))
        @timer_progress.signal
      end

      def start_timer
        @timer = @lifetime.async(finished: false) do |task|
          while @state == :running
            # Completion-relative monotonic scheduling; explicit attempts also
            # coalesce with the timer rather than starting another callback.
            remaining = @next_refresh - Internal.now
            if remaining.positive?
              begin
                task.with_timeout([remaining, Internal::MAX_WAIT].min) { @timer_progress.wait }
              rescue ::Async::TimeoutError
                nil
              end
              next
            end
            break unless @state == :running

            begin
              refresh
            rescue Error
              nil # perform_attempt has already emitted a safe event.
            end
          end
        end
      end

      def report(category)
        @category = category
        event = { category: category, status: status }.freeze
        if @reporter
          begin
            @reporter.call(event)
          rescue StandardError, ScriptError
            warn "ConnectRPC TLS error reporter failed"
          end
        else
          warn "ConnectRPC TLS refresh failed (#{category})"
        end
      end

      def fail_safely(category)
        report(category)
        raise Error.new(category), cause: nil
      end

      def shutdown
        return if @state == :closed

        @state = :closing
        Internal.cancel(@timer)
        Internal.cancel(@attempt)
        Internal.cancel(@callback)
        Internal.join(@timer)
        Internal.join(@callback)
        Internal.join(@attempt)
        @state = :closed
        Internal.cancel(@lifetime)
      end
    end
  end
end
