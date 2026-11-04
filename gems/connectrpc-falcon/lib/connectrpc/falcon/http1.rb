# frozen_string_literal: true

require "English"

require "async/http/protocol/http1/server"

module ConnectRPC
  module Falcon
    class HTTP1Connection < ::Async::HTTP::Protocol::HTTP1::Server
      attr_accessor :resource
      attr_writer :rejected_input

      def close_read
        # The framing parser can close input itself before raising EOFError.
        @rejected_input&.abort_read
        super
      end

      def next_request
        return if @resource.draining

        request = super
        if request
          fields = request.headers.header
          if fields.any? do |key, value|
            key.casecmp?("content-type") && value.downcase.start_with?("application/grpc-web")
          end &&
             fields.sum { |key, value| key.bytesize + value.bytesize + 32 } > GRPCProtocol::FIELD_LIMIT
            raise Error.new(:resource_exhausted, "Web field block exceeds byte limit"), cause: nil
          end
        end
        # Retirement can happen while the upstream parser is blocked.
        return if @resource.draining

        @resource.active = true if request
        @resource.changed
        request
      end

      def each(...)
        super
      ensure
        # Header failures and cancellation can bypass write_body entirely.
        @resource.exchange&.finish(false, $ERROR_INFO)
        @resource.exchange = nil
      end

      def write_body(...)
        super
        written = true
      rescue StandardError
        @resource.failed!
        raise
      ensure
        # Upstream returns after final fixed/chunked bytes AND body callbacks.
        begin
          @resource.exchange&.finish(!!written, $ERROR_INFO)
        ensure
          @resource.exchange = nil
          @resource.active = false
          @resource.changed
        end
      end
    end
    private_constant :HTTP1Connection

    # One request's original framing and deadline, owned only by native Web H1.
    class HTTP1RejectedInput
      def initialize(body, raw:, connection:, headers:)
        @body = body
        @raw = raw
        @connection = connection
        # Capture before processing. Invalid fields remain core's validation
        # responsibility and must not change its validation order.
        @deadline = GRPCProtocol.request_deadline(headers)
      rescue Error
        @deadline = nil
      end

      def close(error = $ERROR_INFO)
        return if @closed

        @closed = true
        begin
          @connection.rejected_input = self
          raise error if error

          if @body && !@body.empty?
            @expires = now + 0.1
            @expires = [@expires, now + @deadline.remaining].min if @deadline
            until @body.empty?
              remaining = @expires - now
              raise ::Async::TimeoutError, "Rejected Web input did not finish" if remaining <= 0

              # Pinned Fixed/Chunked reads are <=64 KiB and preserve framing,
              # including a core input adapter's discarded readahead remainder.
              ::Async::Task.current.with_timeout(remaining) { @body.read }
            end
          end
          finished = true
        ensure
          # Cancellation and malformed/expired input must fail raw TCP before
          # Finishable/body or TLS cleanup can yield. Never signal RPC cancellation.
          begin
            abort_read unless finished
          ensure
            begin
              @body&.close
            ensure
              @connection.rejected_input = nil
            end
          end
        end
      end

      def abort_read
        @raw.close unless @raw.closed?
      end

      private

      def now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
    private_constant :HTTP1RejectedInput

    # Serial HTTP/1 ownership. It deliberately has no HTTP/2 stream/GOAWAY API.
    class HTTP1Resource
      attr_accessor :raw, :socket, :task, :snapshot, :ready, :draining, :active, :exchange
      attr_reader :connection, :generation, :progress, :activity_at

      def initialize(stream, progress:, generation:)
        @connection = HTTP1Connection.new(stream, "HTTP/1.1")
        @connection.resource = self
        @shared_progress = progress
        @progress = ::Async::Condition.new
        @generation = generation
        @tasks = []
        @activity_at = now
        @active = @ready = @draining = @aborted = @finishing = false
      end

      def changed
        @activity_at = now
        @progress.signal
        @shared_progress.signal
      end

      def track(task)
        @tasks.reject!(&:finished?)
        @tasks << task
        task
      end

      def empty? = !@active && @tasks.all?(&:finished?)
      def finished? = @aborted
      def finishing? = @finishing || @draining

      def failed!
        @failed = true
      end

      def web_input(body, headers:)
        HTTP1RejectedInput.new(body, raw: @raw, connection: @connection, headers: headers)
      end

      def abort
        return if @aborted

        @aborted = true
        @raw.close unless @raw.closed?
        owned = [*@tasks, @task].compact.uniq
        owned.each { |task| cancel(task) }
        # Cancel every task before joining dependencies of other owned tasks.
        # rubocop:disable-next Style/CombinableLoops
        owned.each { |task| join(task) }
        begin
          @connection.close
        rescue StandardError
          nil
        ensure
          changed
        end
      end

      def drain(parent, expires)
        return wait(@drain_task, expires) if @drain_task
        return true if @aborted

        @draining = true
        unless @ready
          abort
          return true
        end
        @drain_task = parent.async(finished: false) do
          @progress.wait until empty? || @aborted
          next false if @aborted || @failed

          @finishing = true
          # Cancel an idle parser only after the admitted response and callback
          # complete. Raw half-close preserves the HTTP wire-completion boundary.
          @raw.shutdown(Socket::SHUT_WR)
          cancel(@task)
          join(@task)
          loop { @raw.readpartial(16_384) }
        rescue EOFError
          true
        rescue StandardError
          false
        ensure
          abort
        end
        wait(@drain_task, expires)
      rescue ::Async::TimeoutError
        abort
        false
      end

      private

      def now = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      def wait(task, expires)
        remaining = expires - now
        raise ::Async::TimeoutError if remaining <= 0

        ::Async::Task.current.with_timeout(remaining) { task.wait }
      end

      def cancel(task)
        return unless task && !task.current? && !task.finished?

        task.cancel
        task.cancel if task.cancel_deferred?
      end

      def join(task)
        task&.wait unless task&.current?
      rescue StandardError
        nil
      end
    end
    private_constant :HTTP1Resource
  end
end
