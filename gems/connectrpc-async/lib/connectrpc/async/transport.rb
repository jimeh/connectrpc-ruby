# frozen_string_literal: true

require "uri"

module ConnectRPC
  module Async
    class Transport
      include Internal::Affinity

      # The default ceiling of dynamic receive windows, grpc-go's.
      MAX_RECEIVE_WINDOW = 16_777_216
      private_constant :MAX_RECEIVE_WINDOW

      def initialize(origin:, ca_file: nil, certificate_chain: nil, private_key: nil, connection_limit: 1,
                     max_requests: 128, connect_timeout: 10, connection_policy: ConnectionPolicy.new,
                     connection_observer: nil, stream_receive_window: 4_194_304,
                     connection_receive_window: 16_777_216, dynamic_receive_windows: true,
                     max_receive_window: MAX_RECEIVE_WINDOW)
        initialize_affinity
        @origin = parse_origin(origin)
        @connection_limit = Internal.positive_integer(connection_limit, "connection_limit")
        @max_requests = Internal.positive_integer(max_requests, "max_requests")
        @connect_timeout = Internal.seconds(connect_timeout, "connect_timeout")
        @stream_receive_window = Internal.receive_window(stream_receive_window, "stream_receive_window")
        @connection_receive_window = Internal.receive_window(connection_receive_window, "connection_receive_window")
        @receive_window_limit = Internal.receive_window_limit(dynamic_receive_windows, max_receive_window)
        unless connection_policy.is_a?(ConnectionPolicy)
          raise ArgumentError, "connection_policy must be a ConnectionPolicy"
        end
        unless connection_observer.nil? || connection_observer.respond_to?(:call)
          raise ArgumentError, "connection_observer must respond to call"
        end
        if @origin.scheme == "http" && (ca_file || certificate_chain || private_key)
          raise ArgumentError, "TLS options require an HTTPS origin"
        end

        if @origin.scheme == "https"
          @snapshot = Internal::TLS.client(ca_file: ca_file, certificate_chain: certificate_chain,
                                           private_key: private_key)
        end
        @calls = {}.compare_by_identity
        @acquiring = {}
        @state = :running
        install_owner_guard
        @manager = Internal::ConnectionManager.new(
          policy: connection_policy, observer: connection_observer, connection_limit: @connection_limit,
          connect_timeout: @connect_timeout, lifetime: @lifetime, progress: @progress,
          build: method(:build_resource), setup: method(:setup_connection), abort_setup: method(:abort_setup)
        )
      end

      def inspect = "#<#{self.class} state=#{@state} connection_state=#{@manager&.state} origin=#{@origin}>"

      # A published observation, not a promise that the next call succeeds.
      def connection_state = @manager.state

      def call(request)
        check_affinity!
        @manager.check_reentry!
        CallCancellation.with_request(request) { |outgoing| perform_call(outgoing) }
      end

      def streaming_capabilities = %i[server_stream client_stream bidi_stream].freeze

      def open_stream(request, &)
        raise ArgumentError, "A streaming block is required" unless block_given?

        check_affinity!
        @manager.check_reentry!
        CallCancellation.with_request(request) { |outgoing| perform_stream(outgoing, &) }
      end

      def grpc_capabilities = %i[unary server_stream client_stream bidi_stream].freeze

      def open_grpc_stream(request, &)
        raise ArgumentError, "A gRPC block is required" unless block_given?

        check_affinity!
        @manager.check_reentry!
        CallCancellation.with_request(request) { |outgoing| perform_stream(outgoing, grpc: true, &) }
      end

      def close(grace: 5)
        check_affinity!
        @manager.check_reentry!
        super
      end

      private

      def perform_stream(request, grpc: false)
        raise ArgumentError, "Request URL must match the transport origin" unless same_origin?(request.url)
        raise Error.new(:unavailable, "HTTP/2 transport admission is closed") unless @state == :running

        operation = { request: request, tasks: {}, grpc: grpc }
        # Streams reserve an admission slot but no request bytes.
        @manager.admit(0, full: @calls.size >= @max_requests)
        @calls[operation] = true
        body = Internal::OutputBody.new
        completed = false
        begin
          stream_operation(operation) { dispatch_stream(operation, body) }
          exchange = grpc ? Internal::GRPCStreamExchange : Internal::StreamExchange
          result = yield exchange.new(self, operation, body)
          completed = true
          result
        ensure
          body.close
          begin
            cleanup(operation)
          ensure
            @calls.delete(operation)
            @progress.signal
          end
          if grpc && completed && operation[:cleanup_error]
            check_request!(request)
            raise Error.new(:unavailable, "HTTP/2 transport cleanup failed"), cause: operation[:cleanup_error]
          end
        end
      end

      def stream_operation(operation, timeout: nil, &work)
        check_affinity!
        timeout = [timeout || Deadline::MAX_NATIVE_WAIT, Deadline::MAX_NATIVE_WAIT].min if operation[:grpc]
        request = operation.fetch(:request)
        raise Error.new(:unavailable, "HTTP/2 exchange is retired"), cause: nil if operation[:cleaned]

        check_request!(request)
        event = ::Async::Promise.new
        registration = request.cancellation&.on_cancel { event.resolve([:cancellation, nil]) }
        child = @lifetime.async(finished: false) do
          result = activate(request, &work)
          event.resolve([:result, result])
        rescue StandardError => e
          request.lifecycle&.record_failure(e)
          event.resolve([:error, e])
        ensure
          event.resolve([:stopped, nil])
        end
        operation.fetch(:tasks)[child] = true
        expires = Internal.now + timeout if timeout
        expires = [expires || Float::INFINITY, Internal.now + request.deadline.remaining].min if request.deadline
        tag, result = Internal.wait_event(event, expires)
        check_request!(request)
        raise result if tag == :error
        raise IOError, "HTTP/2 operation stopped" unless tag == :result

        result
      rescue StandardError => e
        request.lifecycle&.record_failure(e)
        check_request!(request)
        # The caller's own enclosing Async timeout is not a transport failure;
        # the worker's delivered failure is.
        raise if Internal.foreign_timeout?(e, (result if tag == :error))
        if operation[:grpc] && (failure = operation[:response]&.stream&.grpc_error)
          raise failure, cause: nil
        end
        raise if e.is_a?(ConnectRPC::Error)

        raise Error.new(:unavailable, Internal::ClientFailure.message("HTTP/2", e)), cause: e
      ensure
        registration&.unregister
        # The call's one cleanup budget owns stopping a still-blocked task.
        if child && !child.finished? && !%i[result error].include?(tag)
          cleanup(operation)
        else
          operation[:tasks]&.delete(child)
        end
      end

      def dispatch_stream(operation, body)
        request = operation.fetch(:request)
        dispatch(operation) do |connection, task|
          headers = request.headers.dup
          if request.deadline
            key = operation[:grpc] ? "grpc-timeout" : "connect-timeout-ms"
            headers[key] = [operation[:grpc] ? GRPCProtocol.timeout(request.deadline) : request.deadline.timeout_ms]
          end
          GRPCProtocol.check_fields!(headers) if operation[:grpc]
          outgoing = ::Protocol::HTTP::Request["POST", request.url.request_uri, Internal.fields(headers), body,
                                               scheme: @origin.scheme, authority: @origin.authority]
          response = connection.create_response
          operation[:response] = response
          response.stream.grpc_mark! if operation[:grpc]
          task.defer_cancel { connection.write_request(response, outgoing) }
        end
      end

      # Acquires a lease and runs the block under its write guard. A connection
      # found unusable before any stream ID is allocated is released and the
      # call returns to acquisition; once dispatch starts it is never resent.
      def dispatch(operation)
        request = operation.fetch(:request)
        task = ::Async::Task.current
        loop do
          check_live!(operation)
          connection = acquire(operation, task)
          dispatched = false
          begin
            return connection.synchronize(dispatch: true) do
              check_request!(request)
              check_live!(operation)
              raise Internal::UnusableConnection, "Connection is not reusable" unless @manager.usable?(connection)

              dispatched = true
              yield connection, task
            end
          rescue Internal::UnusableConnection
            raise if dispatched

            reroute(operation)
          end
        end
      end

      # Once whole-operation cleanup starts, the call sends nothing new; the
      # lease belongs to cleanup.
      def check_live!(operation)
        raise Error.new(:unavailable, "HTTP/2 exchange is retired"), cause: nil if operation[:cleaned]
      end

      def acquire(operation, task)
        @acquiring[task] = operation
        begin
          connection = @manager.acquire
          operation[:connection] = connection
          operation[:resource] = connection.resource
          connection
        ensure
          @acquiring.delete(task)
        end
      end

      # Detaches the unsent lease before any yielding release, so cleanup can
      # never release it again. A failed release fails the call.
      def reroute(operation)
        connection = operation.delete(:connection)
        operation.delete(:resource)
        @manager.release(connection)
      rescue StandardError
        begin
          connection.resource&.abort
        ensure
          @manager.retire(connection)
        end
        raise
      end

      def perform_call(request)
        raise ArgumentError, "Request URL must match the transport origin" unless same_origin?(request.url)

        check_request!(request)
        raise Error.new(:unavailable, "HTTP/2 transport admission is closed") unless @state == :running

        event = ::Async::Promise.new
        operation = { request: request }
        # GET carries its encoded message in the request target.
        bytes = request.http_method == "GET" ? request.url.request_uri.bytesize : request.body.bytesize
        operation[:bytes] = @manager.admit(bytes, full: @calls.size >= @max_requests)
        @calls[operation] = true
        begin
          registration = request.cancellation&.on_cancel { event.resolve([:cancellation, nil]) }
          check_request!(request)
          operation[:task] = @lifetime.async(finished: false) do
            result = activate(request) { exchange(operation) }
            event.resolve([:response, result])
          rescue StandardError => e
            request.lifecycle&.record_failure(e)
            event.resolve([:error, e])
          ensure
            event.resolve([:stopped, nil])
          end
          expires = Internal.now + request.deadline.remaining if request.deadline
          tag, result = Internal.wait_event(event, expires)
          check_request!(request)
          raise result if tag == :error
          raise Error.new(:unavailable, "HTTP/2 connection closed") unless result

          result
        rescue StandardError => e
          request.lifecycle&.record_failure(e)
          check_request!(request)
          # The caller's own enclosing Async timeout is not a transport failure;
          # the worker's delivered failure is.
          raise if e.is_a?(ConnectRPC::Error) || Internal.foreign_timeout?(e, (result if tag == :error))

          raise Error.new(:unavailable, Internal::ClientFailure.message("HTTP/2", e)), cause: e
        ensure
          registration&.unregister
          begin
            cleanup(operation)
          ensure
            @calls.delete(operation)
            @manager.release_bytes(operation.fetch(:bytes))
            @progress.signal
          end
        end
      end

      def check_request!(request)
        request.cancellation ? request.cancellation.check! : request.deadline&.check!
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

      def build_resource
        resource = Internal::Resource.new(progress: @progress)
        resource.send(:retirement_parent=, @lifetime)
        @resources.reject! { |entry| entry.send(:retirement_completed?) }
        @resources << resource
        resource
      end

      def abort_setup(resource)
        raw = resource.raw
        # Ruby 4 close may wait for the blocked SSL handshake. A non-flushing
        # shutdown wakes that setup operation before the existing raw-first abort.
        raw.shutdown(Socket::SHUT_RDWR) if raw && !raw.closed?
      rescue IOError, SystemCallError
        nil
      ensure
        resource.abort
      end

      def activate(request, &work)
        request.lifecycle ? request.lifecycle.activate(&work) : work.call
      end

      def setup_connection(resource)
        resource.raw = ::IO::Endpoint.tcp(@origin.hostname, @origin.port).connect
        socket = resource.raw
        if @snapshot
          socket = @snapshot.socket(resource.raw)
          resource.socket = socket
          socket.hostname = Internal::TLS.server_name(@origin.hostname)
          socket.connect
          socket.post_connection_check(@origin.hostname)
          raise Internal::ALPNError, "HTTP/2 ALPN is required" unless socket.alpn_protocol == "h2"
        end
        resource.socket = socket
        connection = Internal::ClientConnection.new(::IO::Stream(socket))
        resource.connection = connection
        connection.resource = resource
        connection.receive_windows(stream: @stream_receive_window, connection: @connection_receive_window,
                                   limit: @receive_window_limit)
        connection.send_connection_preface(Internal.settings(@max_requests, stream_window: @stream_receive_window)) do
          connection.grow_connection_window
        end
        resource.ready = true
        connection.start_receive_window_writer(@lifetime)
        connection.read_in_background(parent: @lifetime)
        connection
      end

      def exchange(operation)
        request = operation.fetch(:request)
        check_request!(request)
        retried = false
        loop do
          connection = dispatch(operation) do |guarded, task|
            headers = request.headers.dup
            headers["connect-timeout-ms"] = [request.deadline.timeout_ms] if request.deadline
            body = request.http_method == "GET" ? nil : [request.body]
            outgoing = ::Protocol::HTTP::Request[request.http_method, request.url.request_uri,
                                                 Internal.fields(headers), body,
                                                 scheme: @origin.scheme, authority: @origin.authority]
            # Stream ID allocation shares the wire dispatch guard.
            response = guarded.create_response
            operation[:response] = response
            task.defer_cancel { guarded.write_request(response, outgoing) }
            guarded
          end
          response = operation.fetch(:response)
          begin
            connection.read_response(response)
            break
          rescue ::Protocol::HTTP::RefusedError
            # Only the header wait proves RFC 9113 non-processing: dispatch can
            # convert a HEADERS write failure into the same error.
            raise if retried || !@manager.retry_refused? || response.stream.response_headers?

            retried = true
            retire_refused(operation, response)
          end
        end
        response = operation.fetch(:response)
        headers = Internal.headers(response)
        bytes = read_body(response, request)
        TransportResponse.new(status: response.status, headers: headers, body: bytes)
      end

      # The refused stream is closed, but its output may still be inside a
      # deferred frame write. That output retires within the call's cleanup
      # budget before the lease returns to acquisition; a stuck frame fails
      # its connection and the call, which is not sent again.
      def retire_refused(operation, response)
        # The refused stream already stopped its output once. Cleanup still
        # retires it after the response detaches; see #retire_stopped_output.
        operation[:refused_output] = response.stream.output_task
        operation.delete(:response)
        response.close
        request = operation.fetch(:request)
        expires = Internal.now + Internal::CLEANUP_SECONDS
        expires = [expires, Internal.now + request.deadline.remaining].min if request.deadline
        unless output_retired?(operation[:refused_output], expires)
          # An expired call leaves its output to the call's own cleanup.
          check_request!(request)
          # Raw-first abort ends the stuck frame's hazard and its ownership.
          operation[:resource]&.abort
          operation.delete(:refused_output)
          raise Error.new(:unavailable, "HTTP/2 refused stream output did not retire"), cause: nil
        end
        operation.delete(:refused_output)
        # A call whose cleanup started while it joined leaves the lease there.
        check_live!(operation)
        reroute(operation)
      end

      # A closed stream already stopped its output once. Cancelling the call
      # task cascades a second cancellation into that output, which would stop
      # a deferred frame write mid-frame. A stopped output that does not retire
      # within the budget, or whose wait is interrupted, fails its connection
      # raw-first instead. Until it retires, the call still holds that
      # output's connection lease.
      def retire_stopped_output(operation, expires)
        output = stopped_output(operation)
        output_retired?(output, expires)
      ensure
        operation[:resource]&.abort if output && !output.finished?
        operation.delete(:refused_output)
      end

      # A refused output retiring for a retry, or a unary exchange's output
      # whose stream closed, for example by a reset after response HEADERS.
      # Stream operations keep their existing cleanup order.
      def stopped_output(operation)
        return operation[:refused_output] if operation[:refused_output]
        return if operation.key?(:tasks)

        stream = operation[:response]&.stream
        stream.output_task if stream&.closed?
      end

      # Waits until `expires` for the output and reports whether it finished.
      def output_retired?(output, expires)
        return true unless output && !output.finished?

        joiner = ::Async::Task.current.async(finished: false) { Internal.join(output) }
        begin
          Internal.wait(joiner, expires)
        rescue ::Async::TimeoutError
          nil
        ensure
          Internal.cancel(joiner)
        end
        output.finished?
      end

      def read_body(response, request)
        body = response.body
        limit = request.response_limit
        length = body&.length || response.stream.declared_length
        raise Error.new(:resource_exhausted, "Response exceeds byte limit") if length && length > limit

        bytes = +"".b
        input = Internal::Input.new(body)
        while (chunk = input.read([16_384, limit + 1 - bytes.bytesize].min))
          check_request!(request)
          bytes << chunk
          raise Error.new(:resource_exhausted, "Response exceeds byte limit") if bytes.bytesize > limit
        end
        raise Error.new(:internal, "Content-Length does not match response") if length && bytes.bytesize != length

        bytes
      end

      def cleanup(operation)
        return if operation[:cleaned]

        operation[:cleaned] = true
        expires = Internal.now + Internal::CLEANUP_SECONDS
        cleanup_task = ::Async::Task.current.async(finished: false) do
          # The manager owns connection setup; a call owns only its own tasks.
          tasks = [operation[:task], *operation.fetch(:tasks, {}).keys].compact.uniq
          begin
            retire_stopped_output(operation, expires)
          ensure
            tasks.each { |task| task.cancel unless task.finished? }
          end
          # Cancel every task before joining in reverse ownership order.
          tasks.reverse_each do |task|
            operation[:grpc] ? (task.wait unless task.current?) : Internal.join(task)
          end
          response = operation[:response]
          if response
            stream = response.stream
            case stream.state
            when :idle then stream.close!
            when :closed then nil
            else stream.send_reset_stream(::Protocol::HTTP2::Error::CANCEL)
            end
            response.close
          end
        end
        # A caller's own Async timeout landing during cleanup is re-raised
        # unchanged once cleanup has used its budget and released the lease.
        interruptions = []
        begin
          Internal::Retaining.wait(cleanup_task, expires, interruptions)
        rescue StandardError => e
          operation[:cleanup_error] ||= e
          begin
            operation[:resource]&.abort
          rescue StandardError => abort_error
            abort_failed(operation, abort_error, interruptions)
          ensure
            Internal.cancel(cleanup_task)
            pending = Internal::Retaining.join(cleanup_task)
            interruptions << pending if pending
          end
        ensure
          if (connection = operation.delete(:connection))
            begin
              @manager.release(connection)
            rescue StandardError => e
              Internal.foreign_timeout?(e) ? interruptions << e : operation[:cleanup_error] ||= e
              begin
                operation[:resource]&.abort
                # A failed release may precede usage accounting. Retire the
                # aborted connection without retrying release or cleanup.
                @manager.retire(connection)
              rescue StandardError => abort_error
                abort_failed(operation, abort_error, interruptions)
              end
            end
          end
          if operation[:grpc] && (error = operation[:cleanup_error])
            lifecycle = operation.fetch(:request).lifecycle
            lifecycle&.record_failure(error)
            lifecycle&.finish(delivery: :aborted, error: error)
          end
        end
        raise interruptions.first unless interruptions.empty?
      end

      # An abort joins connection retirement in the caller's fiber, where the
      # caller's own timeout is retained. Its other failures fail a unary call.
      def abort_failed(operation, error, interruptions)
        if Internal.foreign_timeout?(error)
          interruptions << error
        elsif !operation[:grpc]
          raise error
        end
      end

      def close_owned(expires)
        @manager.close
        waiter = @lifetime.async(finished: false) { @progress.wait until @calls.empty? }
        begin
          Internal.wait(waiter, expires)
        rescue ::Async::TimeoutError
          # An expired grace forces the same teardown as an abort, which
          # reports its own first failure; the closer's abort is then a no-op.
          return abort_owned
        ensure
          Internal.cancel(waiter)
        end
        drains = @resources.map { |resource| @lifetime.async(finished: false) { resource.drain(@lifetime, expires) } }
        graceful = drains.map(&:wait).all?
        @manager.finish
        finish_closed(graceful)
      end

      # Every connection fails its raw socket before anything here can yield.
      # Each later step runs whatever an earlier one raised, was interrupted
      # by or changed, and the first interruption, otherwise the first
      # failure, is raised once the transport has closed.
      def abort_owned
        return if @aborting

        @aborting = true
        @state = :closing unless @state == :closed
        resources = @resources.dup
        resources.each { |resource| resource.send(:fail_raw) }
        failures = [Internal::Teardown.capture { @manager&.close }]
        failures.concat((resources | @resources).map { |resource| Internal::Teardown.capture { resource.abort } })
        @calls.each_key.to_a.each do |operation|
          failures << Internal::Teardown.capture { Internal.cancel(operation[:task]) }
        end
        failures << Internal::Teardown.capture { @manager&.finish }
        finish_closed(false) unless @state == :closed
        failure = Internal::Teardown.first(failures)
        stopped = Internal::Teardown.capture { Internal.cancel(@lifetime) }
        failure ||= stopped
        raise failure if failure
      end
    end
  end
end
