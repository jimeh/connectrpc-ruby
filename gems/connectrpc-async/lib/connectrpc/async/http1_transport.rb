# frozen_string_literal: true

require "uri"
require_relative "http1_connection"
require_relative "http1_exchange"

module ConnectRPC
  module Async
    # Explicit Connect HTTP/1.1 execution on the application's Async reactor.
    class HTTP1Transport
      include Internal::Affinity

      def initialize(origin:, ca_file: nil, certificate_chain: nil, private_key: nil, connection_limit: 4,
                     max_requests: 128, connect_timeout: 10)
        initialize_affinity
        @origin = parse_origin(origin)
        @connection_limit = Internal.positive_integer(connection_limit, "connection_limit")
        @max_requests = Internal.positive_integer(max_requests, "max_requests")
        @connect_timeout = Internal.seconds(connect_timeout, "connect_timeout")
        if @origin.scheme == "http" && (ca_file || certificate_chain || private_key)
          raise ArgumentError, "TLS options require an HTTPS origin"
        end

        if @origin.scheme == "https"
          @snapshot = Internal::TLS.client(ca_file: ca_file, certificate_chain: certificate_chain,
                                           private_key: private_key, alpn_protocols: ["http/1.1"])
        end
        @calls = {}.compare_by_identity
        @leases = []
        @idle = []
        @state = :running
        install_owner_guard
      end

      def inspect = "#<#{self.class} state=#{@state} origin=#{@origin}>"
      def streaming_capabilities = %i[server_stream client_stream].freeze

      def call(request)
        check_affinity!
        CallCancellation.with_request(request) do |outgoing|
          with_operation(outgoing) do |operation|
            operate(operation) { dispatch(operation) }
            head = response_head(operation)
            bytes = operate(operation) { read_body(operation) }
            if head.status == 200
              finish(operation)
            else
              # A complete HTTP error is useful to core even if upload is
              # blocked. Abort before any yield can emit a normal chunk EOF.
              cleanup(operation)
            end
            TransportResponse.new(status: head.status, headers: head.headers, body: bytes)
          end
        end
      end

      def open_stream(request)
        raise ArgumentError, "A streaming block is required" unless block_given?

        check_affinity!
        CallCancellation.with_request(request) do |outgoing|
          with_operation(outgoing) do |operation|
            operate(operation) { dispatch(operation, streaming: true) }
            yield Internal::HTTP1Exchange.new(self, operation)
          end
        end
      end

      private

      def with_operation(request)
        raise ArgumentError, "Request URL must match the transport origin" unless same_origin?(request.url)

        check_request!(request)
        unless @state == :running && @calls.size < @max_requests
          raise Error.new(:unavailable, "HTTP/1 transport admission is closed or full"), cause: nil
        end

        # Registry values record cancellation delivery, not task completion.
        operation = { request: request, tasks: {}, events: {}, writer_state: :pending, writer_reported: false }
        @calls[operation] = true
        begin
          result = yield operation
          cleanup(operation)
          check_cleanup!(operation)
          result
        rescue Exception => e # rubocop:disable Lint/RescueException -- preserve the caller's original interruption through finalization
          interruption = e unless e.is_a?(StandardError)
          raise
        ensure
          begin
            cleanup(operation, interruption: interruption)
          ensure
            @calls.delete(operation)
            @progress.signal
          end
        end
      end

      # Each wait owns a private activated task. Foreign-thread callbacks only
      # resolve the event, never touch a socket or a later lease.
      def operate(operation, timeout: nil, &)
        check_affinity!
        request = operation.fetch(:request)
        check_operation!(operation)

        check_request!(request)
        event = ::Async::Promise.new
        operation.fetch(:events)[event] = true
        registration = request.cancellation&.on_cancel { event.resolve([:cancellation, nil]) }
        child = @lifetime.async(finished: false) do |owned|
          child = owned
          operation.fetch(:tasks)[owned] = false
          value = activate(request) do
            check_operation!(operation)
            yield
          end
          event.resolve([:result, value])
        rescue StandardError => e
          request.lifecycle&.record_failure(e)
          event.resolve([:error, e])
        ensure
          event.resolve([:stopped, nil])
        end
        expires = Internal.now + [timeout, Deadline::MAX_NATIVE_WAIT].min if timeout
        expires = [expires || Float::INFINITY, Internal.now + request.deadline.remaining].min if request.deadline
        expires = [expires || Float::INFINITY, operation[:completion_expires]].min if operation[:completion_expires]
        tag, result = wait_event(event, expires)
        check_request!(request)
        raise result if tag == :error
        raise IOError, "HTTP/1 operation stopped" unless tag == :result

        result
      rescue StandardError => e
        request.lifecycle&.record_failure(e)
        check_request!(request)
        raise if e.is_a?(Error)

        raise Error.new(:unavailable, "HTTP/1 transport failed"), cause: nil
      rescue Exception => e # rubocop:disable Lint/RescueException -- an ensure must not replace this interruption with a later one
        interruption = e
        raise
      ensure
        if child && !child.finished?
          cleanup(operation, interruption: interruption)
        else
          operation[:tasks]&.delete(child)
        end
        registration&.unregister
        operation[:events]&.delete(event)
      end

      def wait_event(event, expires)
        loop do
          return event.wait unless expires

          remaining = expires - Internal.now
          raise ::Async::TimeoutError if remaining <= 0

          begin
            return event.wait(timeout: [remaining, Internal::MAX_WAIT].min)
          rescue ::Async::TimeoutError
            raise if Internal.now >= expires
          end
        end
      end

      def acquire(operation)
        loop do
          check_operation!(operation)
          check_request!(operation.fetch(:request))
          raise IOError, "HTTP/1 transport closed" if @forced

          if (resource = @idle.pop)
            if resource.connection.viable?
              operation[:resource] = resource
              return resource
            end
            retire_idle(resource)
            next
          end
          if @leases.size < @connection_limit
            # Reserve before TCP/TLS can yield. A stalled setup consumes only
            # its own slot and never serializes other reserved dials.
            resource = Internal::HTTP1Resource.new
            @leases << resource
            @resources.reject!(&:finished?)
            @resources << resource
            operation[:resource] = resource
            setup(operation, resource)
            return resource
          end
          @progress.wait
        end
      end

      def setup(operation, resource)
        check_operation!(operation)
        request = operation.fetch(:request)
        expires = Internal.now + @connect_timeout
        expires = [expires, Internal.now + request.deadline.remaining].min if request.deadline
        resource.setup = @lifetime.async(finished: false) do |owned|
          resource.setup = owned
          operation.fetch(:tasks)[owned] = false
          activate(request) do
            check_operation!(operation)
            check_request!(request)
            resource.raw = ::IO::Endpoint.tcp(@origin.hostname, @origin.port).connect
            socket = resource.raw
            if @snapshot
              socket = @snapshot.socket(resource.raw)
              resource.socket = socket
              socket.hostname = @origin.hostname
              socket.connect
              socket.post_connection_check(@origin.hostname)
              unless [nil, "http/1.1"].include?(socket.alpn_protocol)
                raise OpenSSL::SSL::SSLError, "HTTP/1.1 ALPN is required"
              end
            end
            resource.socket = socket
            check_request!(request)
            resource.connection = Internal::HTTP1Connection.new(Internal::HTTP1Stream.new(socket))
          end
        rescue StandardError => e
          request.lifecycle&.record_failure(e)
          raise
        end
        Internal.wait(resource.setup, expires)
      end

      def dispatch(operation, streaming: false)
        request = operation.fetch(:request)
        resource = acquire(operation)
        connection = resource.connection
        operation[:connection] = connection
        check_request!(request)
        headers = request.headers.dup
        headers["connect-timeout-ms"] = [request.deadline.timeout_ms] if request.deadline
        dispatched = ::Async::Promise.new
        body = if streaming
                 Internal::HTTP1Output.new(dispatched)
               elsif request.http_method != "GET"
                 ::Protocol::HTTP::Body::Buffered.wrap([request.body])
               end
        operation[:output] = body
        check_operation!(operation)
        operation[:writer] = @lifetime.async(finished: false) do |owned|
          operation[:writer] = owned
          operation.fetch(:tasks)[owned] = false
          activate(request) do
            check_operation!(operation)
            connection.write_request(@origin.authority, streaming ? "POST" : request.http_method,
                                     request.url.request_uri, "HTTP/1.1", Internal.fields(headers))
            if !streaming && request.http_method == "GET"
              connection.stream.write("\r\n")
              connection.stream.flush
              connection.send_end_stream!
            else
              connection.write_body("HTTP/1.1", body)
            end
            operation[:writer_state] = :ok
          end
        rescue StandardError => e
          operation.update(writer_error: e, writer_state: :failed)
          request.lifecycle&.record_failure(e)
        ensure
          # Native ensure may call send_end_stream! even after cancellation.
          # Only our explicit :ok means the whole request was normally written.
          unless operation[:writer_state] == :ok
            operation[:writer_state] = :failed
            operation[:writer_error] ||= IOError.new("HTTP/1 request writer stopped")
            body&.close(operation[:writer_error])
          end
          dispatched.resolve(nil)
        end
        dispatched.wait if streaming
      end

      def response_head(operation, timeout: nil)
        check_affinity!
        return operation[:head] if operation[:head]

        operate(operation, timeout: timeout) do
          head, body = operation.fetch(:connection).read_head(operation.fetch(:request).http_method || "POST")
          operation[:head] = head
          operation[:input_body] = body
          operation[:input] = Internal::Input.new(body)
          head
        end
      end

      def read(operation, length, timeout: nil)
        response_head(operation, timeout: timeout)
        operate(operation, timeout: timeout) do
          bytes = operation.fetch(:input).read(length)
          operation[:eof] = true if bytes.nil?
          bytes
        end
      end

      def read_body(operation)
        request = operation.fetch(:request)
        length = operation.fetch(:head).headers["content-length"]&.first&.to_i
        limit = request.response_limit
        raise Error.new(:resource_exhausted, "Response exceeds byte limit") if length && length > limit

        bytes = +"".b
        while (chunk = operation.fetch(:input).read([16_384, limit + 1 - bytes.bytesize].min))
          check_request!(request)
          bytes << chunk
          raise Error.new(:resource_exhausted, "Response exceeds byte limit") if bytes.bytesize > limit
        end
        operation[:eof] = true
        raise Error.new(:internal, "Content-Length does not match response") if length && bytes.bytesize != length

        bytes
      end

      def write(operation, bytes)
        operate(operation) do
          raise operation[:writer_error] if operation[:writer_state] == :failed

          operation.fetch(:output).write(bytes)
        end
        nil
      rescue Error
        operation[:writer_reported] = true if operation[:writer_state] == :failed
        raise
      end

      def close_send(operation)
        check_affinity!
        return if operation[:cleaned]

        operation.fetch(:output).close_write
        nil
      end

      def finish(operation, timeout: nil)
        check_affinity!
        return if operation[:finished]

        operation[:completion_expires] = Internal.now + timeout if timeout
        operate(operation, timeout: timeout) do
          raise IOError, "HTTP response EOF is required" unless operation[:eof]

          operation.fetch(:writer).wait
          unless operation[:writer_state] == :ok || operation[:writer_reported]
            raise(operation[:writer_error] || IOError.new("HTTP/1 request writer incomplete"))
          end
        end
        cleanup(operation, reusable: operation[:writer_state] == :ok)
        check_cleanup!(operation)
        check_request!(operation.fetch(:request))
        operation[:finished] = true
        nil
      end

      def abort(operation)
        check_affinity!
        cleanup(operation) unless operation[:finished]
        nil
      end

      def cleanup(operation, reusable: false, interruption: nil)
        return if operation[:cleanup_complete]

        current = ::Async::Task.current
        if operation.fetch(:tasks).key?(current)
          # A joined task may request retirement, but cannot wait for its owner.
          pending = retire_cleanup(operation)
          raise pending if pending

          return
        end
        if operation[:cleaned]
          pending = retire_cleanup(operation)
          interruption ||= pending
          return if operation.fetch(:cleanup_owner).current?

          loop do
            operation.fetch(:cleanup_done).wait
            break
          rescue Exception => e # rubocop:disable Lint/RescueException -- external waiters retain their own interruption until disposition
            interruption ||= e
          end
          raise interruption if interruption

          return
        end

        operation[:cleaned] = true
        operation[:cleanup_owner] = current
        operation[:cleanup_done] = ::Async::Promise.new
        resource = operation[:resource]
        # Terminal entry closes all launch gates. Each child registered before
        # activation could yield, so this set cannot gain another worker.
        tasks = operation.fetch(:tasks).keys
        accepted = false
        begin
          unless reusable && !operation[:retiring]
            pending = retire_cleanup(operation)
            interruption ||= pending
          end
          expires = Internal.now + Internal::CLEANUP_SECONDS
          expires = [expires, operation[:completion_expires]].min if operation[:completion_expires]
          if (deadline = operation.fetch(:request).deadline)
            expires = [expires, Internal.now + deadline.remaining].min
          end
          events = operation.fetch(:events).keys
          events.each { |event| event.resolve([:stopped, nil]) }
          operation[:cleanup_task] = current.async(finished: false) do |owned|
            operation[:cleanup_task] = owned
            operation.fetch(:tasks)[owned] = false
            activate(operation.fetch(:request)) do
              # Normal cleanup waits for natural completion. Destructive paths
              # already raw-aborted and sent each worker its one cancellation.
              tasks.reverse_each(&:wait)
              operation[:input_body]&.close
              unless reusable && !operation[:retiring] && resource&.connection&.viable?
                operation[:retiring] = true
                resource&.abort
                resource&.close
              end
            end
          end
          Internal.wait(operation[:cleanup_task], expires)
          # Task completion includes lifecycle detachment, not just body close.
          raise IOError, "HTTP/1 cleanup stopped" unless operation[:cleanup_task].completed?

          accepted = true
        rescue Exception => e # rubocop:disable Lint/RescueException -- no exit may bypass final joins and disposition
          if e.is_a?(StandardError)
            operation[:cleanup_error] ||= e
            operation.fetch(:request).lifecycle&.record_failure(e)
          else
            interruption ||= e
          end
        ensure
          unless accepted && !operation[:retiring]
            pending = retire_cleanup(operation)
            interruption ||= pending
            [operation[:cleanup_task], *tasks.reverse].compact.each do |task|
              pending = join_cleanup_task(operation, task)
              interruption ||= pending
            end
          end
          if resource
            if accepted && !operation[:retiring]
              @idle << resource
            else
              resource.finish!
              @leases.delete(resource)
            end
          end
          operation[:cleanup_complete] = true
          operation.fetch(:cleanup_done).resolve(nil)
          @progress.signal
        end
        raise interruption if interruption
      end

      def retire_cleanup(operation)
        operation[:retiring] = true
        operation[:resource]&.abort
        begin
          operation[:output]&.close(IOError.new("HTTP/1 exchange retired"))
        rescue StandardError => e
          operation[:cleanup_error] ||= e
          operation.fetch(:request).lifecycle&.record_failure(e)
        end
        events = operation.fetch(:events).keys
        events.each { |event| event.resolve([:stopped, nil]) }
        interruption = nil
        tasks = operation.fetch(:tasks).keys
        tasks.each do |task|
          next if task.current? || task.finished? || operation[:tasks][task]

          # Mark before delivery: cancel can immediately run a reentrant ensure.
          operation[:tasks][task] = true
          begin
            task.cancel
          rescue Exception => e # rubocop:disable Lint/RescueException -- finish retirement even if delivery interrupts the owner
            interruption ||= e
          end
        end
        interruption
      end

      def join_cleanup_task(operation, task)
        interruption = nil
        loop do
          task.wait
          break
        rescue Exception => e # rubocop:disable Lint/RescueException -- a live owner interruption must not release a live task's reservation
          if task.failed? && task.result.equal?(e)
            operation[:cleanup_error] ||= e
            break
          end

          interruption ||= e
        end
        interruption
      end

      def check_operation!(operation)
        return unless operation[:cleaned] || operation[:retiring]

        raise Error.new(:unavailable, "HTTP/1 exchange is retired"), cause: nil
      end

      def check_cleanup!(operation)
        return unless operation[:cleanup_error]

        raise Error.new(:unavailable, "HTTP/1 transport cleanup failed"), cause: nil
      end

      def retire_idle(resource)
        resource.abort
        resource.close
      ensure
        resource.finish!
        @leases.delete(resource)
        @progress.signal
      end

      def close_owned(expires)
        waiter = @lifetime.async(finished: false) { @progress.wait until @calls.empty? }
        begin
          Internal.wait(waiter, expires)
          @idle.dup.each { |resource| retire_idle(resource) }
          @idle.clear
          finish_closed(true)
        rescue ::Async::TimeoutError
          force_operations
          finish_closed(false)
        ensure
          Internal.cancel(waiter)
          Internal.join(waiter)
        end
      end

      def force_operations
        @forced = true
        @leases.dup.each(&:abort)
        operations = @calls.keys
        interruption = nil
        operations.each do |operation|
          cleanup(operation)
        rescue Exception => e # rubocop:disable Lint/RescueException -- force every admitted operation before propagating the closer's interruption
          interruption ||= e
        end
        @idle.dup.each { |resource| retire_idle(resource) }
        @idle.clear
        @progress.signal
        raise interruption if interruption
      end

      def abort_owned
        return if @aborting

        @aborting = true
        @state = :closing unless @state == :closed
        begin
          force_operations
        ensure
          finish_closed(false) unless @state == :closed
          Internal.cancel(@lifetime)
        end
      end

      def check_request!(request)
        request.cancellation ? request.cancellation.check! : request.deadline&.check!
      end

      def activate(request, &work)
        request.lifecycle ? request.lifecycle.activate(&work) : work.call
      end

      def parse_origin(value)
        uri = URI(value)
        unless %w[http https].include?(uri.scheme) && uri.host && !uri.userinfo && !uri.query && !uri.fragment &&
               uri.path.empty?
          raise ArgumentError, "Origin must contain only an HTTP(S) scheme, host and port"
        end

        uri.freeze
      end

      def same_origin?(uri)
        [uri.scheme, uri.hostname&.downcase, uri.port] == [@origin.scheme, @origin.hostname.downcase, @origin.port]
      end
    end
  end
end
