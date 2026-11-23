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
                     max_requests: 128, connect_timeout: 10, proxy: :environment)
        initialize_affinity
        @origin = parse_origin(origin)
        @connection_limit = Internal.positive_integer(connection_limit, "connection_limit")
        @max_requests = Internal.positive_integer(max_requests, "max_requests")
        @connect_timeout = Internal.seconds(connect_timeout, "connect_timeout")
        if @origin.scheme == "http" && (ca_file || certificate_chain || private_key)
          raise ArgumentError, "TLS options require an HTTPS origin"
        end

        @proxy = Internal::ProxyTunnel.resolver(proxy)
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

      # Never shows proxy URLs or credentials.
      def inspect = "#<#{self.class} state=#{@state} origin=#{@origin} proxy=#{Internal::ProxyTunnel.mode(@proxy)}>"
      def streaming_capabilities = %i[server_stream client_stream].freeze

      def call(request)
        check_affinity!
        CallCancellation.with_request(request) do |outgoing|
          sent = first_attempt(outgoing)
          deferred = sent.lifecycle unless sent.equal?(outgoing)
          attempt = nil
          resent = false
          begin
            with_operation(sent) do |operation|
              attempt = operation
              exchange(operation)
            end
          rescue Error => e
            # The first operation's cleanup, lease disposition and admission
            # entry are complete. Its cancellation and deadline still apply.
            raise if resent || !resend?(attempt, e)

            # Observers never see the failures of an attempt that is resent.
            resent = true
            deferred = nil
            sent = outgoing
            retry
          ensure
            deferred&.flush
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

      def exchange(operation)
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

      # Holds a first attempt's failures until the call knows it is not resent.
      class AttemptLifecycle
        def initialize(lifecycle)
          @lifecycle = lifecycle
          @failures = []
        end

        def activate(&) = @lifecycle.activate(&)

        def record_failure(error)
          @failures ? @failures << error : @lifecycle.record_failure(error)
          nil
        end

        def flush
          failures = @failures
          @failures = nil
          failures.each { |error| @lifecycle.record_failure(error) }
          nil
        end
      end
      private_constant :AttemptLifecycle

      # Only a GET with a lifecycle needs its first attempt's failures held.
      def first_attempt(request)
        return request unless request.lifecycle && request.http_method == "GET"

        attempt = request.dup
        attempt.lifecycle = AttemptLifecycle.new(request.lifecycle)
        attempt
      end

      # A server may close an idle keep-alive connection while the next
      # request crosses it. Only a safe GET that found its reused connection
      # closed before any response head is sent once more, and only while the
      # transport still admits calls.
      def resend?(operation, error)
        @state == :running && operation && operation[:reused] && !operation[:head] &&
          operation.fetch(:request).http_method == "GET" &&
          Internal::ClientFailure.connection_closed?(error.cause)
      end

      def with_operation(request)
        raise ArgumentError, "Request URL must match the transport origin" unless same_origin?(request.url)

        check_request!(request)
        raise Error.new(:unavailable, "HTTP/1 transport admission is closed"), cause: nil unless @state == :running
        raise Error.new(:unavailable, "HTTP/1 transport admission is full"), cause: nil if @calls.size >= @max_requests

        # Registry values record cancellation delivery, not task completion.
        operation = { request: request, tasks: {}, events: {}, writer_state: :pending, writer_reported: false }
        @calls[operation] = true
        begin
          result = yield operation
          cleanup(operation)
          check_cleanup!(operation)
          result
        rescue Exception => e # rubocop:disable Lint/RescueException -- preserve the caller's original interruption through finalization
          interruption = e unless e.is_a?(StandardError) && !Internal.foreign_timeout?(e)
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
        check_operation!(operation)
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
        check_operation!(operation)
        tag, result = Internal.wait_event(event, expires)
        check_request!(request)
        check_operation!(operation)
        raise result if tag == :error
        raise IOError, "HTTP/1 operation stopped" unless tag == :result

        result
      rescue StandardError => e
        request.lifecycle&.record_failure(e)
        check_request!(request)
        raise if e.is_a?(Error)

        if Internal.foreign_timeout?(e, (result if tag == :error))
          # The caller's own enclosing Async timeout is not a transport failure;
          # the worker's delivered failure is.
          interruption = e
          raise
        end

        raise Error.new(:unavailable, Internal::ClientFailure.message("HTTP/1", e)), cause: e
      rescue Exception => e # rubocop:disable Lint/RescueException -- an ensure must not replace this interruption with a later one
        interruption = e
        raise
      ensure
        begin
          if child && !child.finished?
            cleanup(operation, interruption: interruption)
          else
            operation[:tasks]&.delete(child)
          end
        ensure
          registration&.unregister
          operation[:events]&.delete(event)
        end
      end

      def acquire(operation)
        loop do
          check_operation!(operation)
          check_request!(operation.fetch(:request))
          check_operation!(operation)
          raise IOError, "HTTP/1 transport closed" if @forced

          if (resource = @idle.pop)
            if resource.connection.viable?
              operation[:resource] = resource
              operation[:reused] = true
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
        check_operation!(operation)
        resource.setup = @lifetime.async(finished: false) do |owned|
          resource.setup = owned
          operation.fetch(:tasks)[owned] = false
          activate(request) do
            check_request!(request)
            check_operation!(operation)
            dial(operation, resource)
            socket = resource.raw
            if @snapshot
              socket = resource.socket = @snapshot.socket(resource.raw)
              socket.hostname = Internal::TLS.server_name(@origin.hostname)
              socket.connect
              socket.post_connection_check(@origin.hostname)
              unless [nil, "http/1.1"].include?(socket.alpn_protocol)
                raise Internal::ALPNError, "HTTP/1.1 ALPN is required"
              end
            end
            resource.socket = socket
            check_request!(request)
            check_operation!(operation)
            resource.connection = Internal::HTTP1Connection.new(Internal::HTTP1Stream.new(socket, resource))
          end
        rescue StandardError => e
          request.lifecycle&.record_failure(e)
          raise
        end
        Internal.wait(resource.setup, expires)
      end

      # Publishes the raw socket to the origin or its proxy. An https://
      # origin tunnels through the proxy with CONNECT before its TLS; an
      # http:// origin's requests are forwarded to it. A proxy that cannot be
      # used fails before any socket opens.
      def dial(operation, resource)
        proxy = Internal::ProxyTunnel.select(@proxy, @origin)
        host, port = proxy ? [proxy.host, proxy.port] : [@origin.hostname, @origin.port]
        resource.raw = ::IO::Endpoint.tcp(host, port).connect
        check_operation!(operation)
        return unless proxy
        return resource.forward = proxy unless @snapshot

        # The raw socket is published, so an abort wakes the exchange.
        Internal::ProxyTunnel.establish(resource.raw, @origin.hostname, @origin.port, proxy, tls: true)
        check_operation!(operation)
      end

      def dispatch(operation, streaming: false)
        request = operation.fetch(:request)
        resource = acquire(operation)
        connection = resource.connection
        operation[:connection] = connection
        check_request!(request)
        check_operation!(operation)
        headers = request.headers.dup
        headers["connect-timeout-ms"] = [request.deadline.timeout_ms] if request.deadline
        target = request.url.request_uri
        if (forward = resource.forward)
          # Net::HTTP and Go net/http send an http:// request to its proxy in
          # absolute form, with Proxy-Authorization on every request.
          target = "http://#{@origin.authority}#{target}"
          if forward.user
            headers["proxy-authorization"] = ["Basic #{["#{forward.user}:#{forward.password}"].pack('m0')}"]
          end
        end
        check_operation!(operation)
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
                                     target, "HTTP/1.1", Internal.fields(headers))
            if !streaming && request.http_method == "GET"
              # The native write_body(nil) adds content-length: 0, which a
              # bodyless GET should not send (RFC 9110 section 8.6).
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
          head, body = forwarded(operation) do
            operation.fetch(:connection).read_head(operation.fetch(:request).http_method || "POST",
                                                   forward: !operation.fetch(:resource).forward.nil?)
          end
          operation[:head] = head
          operation[:input_body] = body
          operation[:input] = Internal::Input.new(body)
          head
        end
      end

      def read(operation, length, timeout: nil)
        response_head(operation, timeout: timeout)
        operate(operation, timeout: timeout) do
          bytes = forwarded(operation) { operation.fetch(:input).read(length) }
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
        input = operation.fetch(:input)
        while (chunk = forwarded(operation) { input.read([16_384, limit + 1 - bytes.bytesize].min) })
          check_request!(request)
          check_operation!(operation)
          bytes << chunk
          raise Error.new(:resource_exhausted, "Response exceeds byte limit") if bytes.bytesize > limit
        end
        operation[:eof] = true
        raise Error.new(:internal, "Content-Length does not match response") if length && bytes.bytesize != length

        bytes
      end

      # A forward proxy writes an http:// call's response. protocol-http1 and
      # protocol-http parser and header errors, and the ArgumentError of a
      # field value conversion such as Integer("08"), can quote it, so they
      # may carry the proxy's URL or credentials; all of them become one fixed
      # failure. EOF and reset stay connection failures, so a stale GET still
      # resends.
      def forwarded(operation)
        yield
      rescue ::Protocol::HTTP1::Error, ::Protocol::HTTP::InvalidHeaderError, ArgumentError
        raise unless operation[:resource]&.forward

        raise Internal::ProxyFailure, "malformed response", cause: nil
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
                # Final descriptor release belongs to the owner after this
                # child's lifecycle detachment and all worker joins.
              end
            end
          end
          Internal.wait(operation[:cleanup_task], expires)
          # Task completion includes lifecycle detachment, not just body close.
          raise IOError, "HTTP/1 cleanup stopped" unless operation[:cleanup_task].completed?

          accepted = true
        rescue Exception => e # rubocop:disable Lint/RescueException -- no exit may bypass final joins and disposition
          # A caller's own Async timeout is an interruption, not a cleanup
          # failure; the cleanup task's own failure is.
          owned = operation[:cleanup_task]
          if e.is_a?(StandardError) && !Internal.foreign_timeout?(e, (owned.result if owned&.failed?))
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
              pending = abort_resource(resource)
              interruption ||= pending
              begin
                resource.close
              rescue Exception => e # rubocop:disable Lint/RescueException -- raw disposition and joins are complete; preserve the owner's first interruption
                interruption ||= e
              end
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
        interruption = abort_resource(operation[:resource])
        begin
          operation[:output]&.close(IOError.new("HTTP/1 exchange retired"))
        rescue StandardError => e
          operation[:cleanup_error] ||= e
          operation.fetch(:request).lifecycle&.record_failure(e)
        end
        events = operation.fetch(:events).keys
        events.each { |event| event.resolve([:stopped, nil]) }
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
        return unless @forced || operation[:cleaned] || operation[:retiring]

        raise Error.new(:unavailable, "HTTP/1 exchange is retired"), cause: nil
      end

      def check_cleanup!(operation)
        return unless operation[:cleanup_error]

        raise Error.new(:unavailable, "HTTP/1 transport cleanup failed"), cause: operation[:cleanup_error]
      end

      # IO#close can suspend, and its caller may be interrupted repeatedly.
      # Retry only this ownership boundary, retaining the first escape for the
      # caller to propagate after the remaining disposition work.
      def abort_resource(resource)
        interruption = nil
        begin
          resource&.abort
        rescue Exception => e # rubocop:disable Lint/RescueException -- native raw disposition must finish before cancellation or slot release
          interruption ||= e
          retry
        end
        interruption
      end

      def retire_idle(resource)
        interruption = abort_resource(resource)
        begin
          resource.close
        rescue Exception => e # rubocop:disable Lint/RescueException -- finish idle accounting after raw disposal, retaining the first interruption
          interruption ||= e
        end
        resource.finish!
        @idle.delete(resource)
        @leases.delete(resource)
        @progress.signal
        raise interruption if interruption
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
        interruption = nil
        @leases.dup.each do |resource|
          pending = abort_resource(resource)
          interruption ||= pending
        end
        operations = @calls.keys
        operations.each do |operation|
          cleanup(operation)
        rescue Exception => e # rubocop:disable Lint/RescueException -- force every admitted operation before propagating the closer's interruption
          interruption ||= e
        end
        @idle.dup.each do |resource|
          retire_idle(resource)
        rescue Exception => e # rubocop:disable Lint/RescueException -- an interrupted idle close cannot bypass remaining resources
          interruption ||= e
        end
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
          # force_operations retains interruptions until all disposition ends.
          if @state != :closed && @leases.empty? && @calls.keys.all? { |operation| operation[:cleanup_complete] }
            finish_closed(false)
          end
          Internal.cancel(@lifetime) if @state == :closed
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
               ["", "/"].include?(uri.path)
          raise ArgumentError, "Origin must contain only an HTTP(S) scheme, host and port"
        end

        uri.path = ""
        uri.freeze
      end

      def same_origin?(uri)
        [uri.scheme, uri.hostname&.downcase, uri.port] == [@origin.scheme, @origin.hostname.downcase, @origin.port]
      end
    end
  end
end
