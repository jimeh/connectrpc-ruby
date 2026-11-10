# frozen_string_literal: true

require "uri"

module ConnectRPC
  module Async
    class Transport
      include Internal::Affinity

      def initialize(origin:, ca_file: nil, certificate_chain: nil, private_key: nil, connection_limit: 1,
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
                                           private_key: private_key)
        end
        @calls = {}.compare_by_identity
        @acquiring = {}
        @state = :running
        install_owner_guard
        @pool = ::Async::Pool::Controller.wrap(limit: @connection_limit) { connect(@acquiring.fetch(::Async::Task.current)) }
      end

      def inspect = "#<#{self.class} state=#{@state} origin=#{@origin}>"

      def call(request)
        check_affinity!
        CallCancellation.with_request(request) { |outgoing| perform_call(outgoing) }
      end

      def streaming_capabilities = %i[server_stream client_stream bidi_stream].freeze

      def open_stream(request, &)
        raise ArgumentError, "A streaming block is required" unless block_given?

        check_affinity!
        CallCancellation.with_request(request) { |outgoing| perform_stream(outgoing, &) }
      end

      def grpc_capabilities = %i[unary server_stream client_stream bidi_stream].freeze

      def open_grpc_stream(request, &)
        raise ArgumentError, "A gRPC block is required" unless block_given?

        check_affinity!
        CallCancellation.with_request(request) { |outgoing| perform_stream(outgoing, grpc: true, &) }
      end

      private

      def perform_stream(request, grpc: false)
        raise ArgumentError, "Request URL must match the transport origin" unless same_origin?(request.url)
        raise Error.new(:unavailable, "HTTP/2 transport admission is closed or full") unless
          @state == :running && @calls.size < @max_requests

        operation = { request: request, tasks: {}, grpc: grpc }
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
            raise Error.new(:unavailable, "HTTP/2 transport cleanup failed"), cause: nil
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
        tag, result = wait_event(event, expires)
        check_request!(request)
        raise result if tag == :error
        raise IOError, "HTTP/2 operation stopped" unless tag == :result

        result
      rescue StandardError => e
        request.lifecycle&.record_failure(e)
        check_request!(request)
        if operation[:grpc] && (failure = operation[:response]&.stream&.grpc_error)
          raise failure, cause: nil
        end
        raise if e.is_a?(ConnectRPC::Error)

        raise Error.new(:unavailable, "HTTP/2 transport failed"), cause: nil
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
        task = ::Async::Task.current
        @acquiring[task] = operation
        begin
          connection = @pool.acquire
          operation[:connection] = connection
          operation[:resource] = connection.resource
        ensure
          @acquiring.delete(task)
        end
        connection.synchronize do
          check_request!(request)
          raise IOError, "Connection is not reusable" unless connection.reusable?

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

      def perform_call(request)
        raise ArgumentError, "Request URL must match the transport origin" unless same_origin?(request.url)

        check_request!(request)
        unless @state == :running && @calls.size < @max_requests
          raise Error.new(:unavailable, "HTTP/2 transport admission is closed or full")
        end

        event = ::Async::Promise.new
        operation = { request: request }
        @calls[operation] = true
        registration = request.cancellation&.on_cancel { event.resolve([:cancellation, nil]) }
        begin
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
          tag, result = wait_event(event, expires)
          check_request!(request)
          raise result if tag == :error
          raise Error.new(:unavailable, "HTTP/2 connection closed") unless result

          result
        rescue ::Async::TimeoutError, StandardError => e
          request.lifecycle&.record_failure(e)
          check_request!(request)
          raise e if e.is_a?(ConnectRPC::Error)

          raise Error.new(:unavailable, "HTTP/2 transport failed"), cause: nil
        ensure
          registration&.unregister
          cleanup(operation)
          @calls.delete(operation)
          @progress.signal
        end
      end

      def check_request!(request)
        request.cancellation ? request.cancellation.check! : request.deadline&.check!
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

      def connect(operation)
        resource = Internal::Resource.new(progress: @progress)
        resource.send(:retirement_parent=, @lifetime)
        @resources.reject! { |entry| entry.send(:retirement_completed?) }
        @resources << resource
        operation[:resource] = resource
        expires = Internal.now + @connect_timeout
        deadline = operation.fetch(:request).deadline
        expires = [expires, Internal.now + deadline.remaining].min if deadline
        check_request!(operation.fetch(:request))
        setup = ::Async::Task.new(@lifetime, finished: false) do
          activate(operation.fetch(:request)) do
            check_request!(operation.fetch(:request))
            setup_connection(resource)
            check_request!(operation.fetch(:request))
            resource.connection
          end
        end
        resource.task = setup
        begin
          setup.run
          Internal.wait(setup, expires)
        rescue ::Async::Cancel, StandardError
          abort_setup(resource)
          raise
        end
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
          socket.hostname = @origin.hostname
          socket.connect
          socket.post_connection_check(@origin.hostname)
          raise OpenSSL::SSL::SSLError, "HTTP/2 ALPN is required" unless socket.alpn_protocol == "h2"
        end
        resource.socket = socket
        connection = Internal::ClientConnection.new(::IO::Stream(socket))
        resource.connection = connection
        connection.resource = resource
        connection.send_connection_preface(Internal.settings(@max_requests))
        resource.ready = true
        connection.read_in_background(parent: @lifetime)
        connection
      end

      def exchange(operation)
        request = operation.fetch(:request)
        check_request!(request)
        task = ::Async::Task.current
        @acquiring[task] = operation
        begin
          connection = @pool.acquire
          operation[:connection] = connection
          operation[:resource] = connection.resource
        ensure
          @acquiring.delete(task)
        end
        connection.synchronize do
          check_request!(request)
          raise IOError, "Connection is not reusable" unless connection.reusable?

          headers = request.headers.dup
          headers["connect-timeout-ms"] = [request.deadline.timeout_ms] if request.deadline
          body = request.http_method == "GET" ? nil : [request.body]
          outgoing = ::Protocol::HTTP::Request[request.http_method, request.url.request_uri, Internal.fields(headers),
                                               body, scheme: @origin.scheme, authority: @origin.authority]
          # Stream ID allocation shares the wire dispatch guard.
          response = connection.create_response
          operation[:response] = response
          task.defer_cancel { connection.write_request(response, outgoing) }
        end
        response = operation.fetch(:response)
        connection.read_response(response)
        headers = Internal.headers(response)
        bytes = read_body(response, request)
        TransportResponse.new(status: response.status, headers: headers, body: bytes)
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
          tasks = [operation[:task], operation[:resource]&.task, *operation.fetch(:tasks, {}).keys].compact.uniq
          tasks.each { |task| task.cancel unless task.finished? }
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
        begin
          Internal.wait(cleanup_task, expires)
        rescue StandardError => e
          operation[:cleanup_error] ||= e
          begin
            operation[:resource]&.abort
          rescue StandardError
            raise unless operation[:grpc]
          ensure
            Internal.cancel(cleanup_task)
            Internal.join(cleanup_task)
          end
        ensure
          if (connection = operation.delete(:connection))
            begin
              @pool.release(connection)
            rescue StandardError => e
              operation[:cleanup_error] ||= e
              begin
                operation[:resource]&.abort
                # A failed release may precede usage accounting. Retire the
                # aborted connection without retrying release or cleanup.
                @pool.retire(connection) if operation[:grpc]
              rescue StandardError
                raise unless operation[:grpc]
              end
            end
          end
          if operation[:grpc] && (error = operation[:cleanup_error])
            lifecycle = operation.fetch(:request).lifecycle
            lifecycle&.record_failure(error)
            lifecycle&.finish(delivery: :aborted, error: error)
          end
        end
      end

      def close_owned(expires)
        waiter = @lifetime.async(finished: false) { @progress.wait until @calls.empty? }
        graceful = true
        begin
          Internal.wait(waiter, expires)
        rescue ::Async::TimeoutError
          graceful = false
          @resources.each(&:abort)
          @calls.each_key { |operation| Internal.cancel(operation[:task]) }
        ensure
          Internal.cancel(waiter)
        end
        drains = @resources.map { |resource| @lifetime.async(finished: false) { resource.drain(@lifetime, expires) } }
        graceful = drains.map(&:wait).all? && graceful
        @pool.close unless @pool.busy?
        finish_closed(graceful)
      end

      def abort_owned
        return if @aborting

        @aborting = true
        @state = :closing unless @state == :closed
        @resources.each(&:abort)
        @calls.each_key { |operation| Internal.cancel(operation[:task]) }
        @pool&.close unless @pool&.busy?
        finish_closed(false) unless @state == :closed
        Internal.cancel(@lifetime)
      end
    end
  end
end
