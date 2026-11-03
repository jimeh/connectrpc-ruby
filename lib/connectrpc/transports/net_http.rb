# frozen_string_literal: true

require "net/http"
require "openssl"
require_relative "../../connectrpc"

module ConnectRPC
  module Transports
    class NetHTTP # rubocop:disable Metrics/ClassLength
      # Net::HTTP uses the open timeout again for TLS. Recompute it from the
      # original deadline after TCP/DNS, rather than starting another budget.
      class Connection < Net::HTTP
        attr_accessor :deadline, :cancellation, :socket_lifecycle

        private

        def check_cancellation!
          cancellation ? cancellation.check! : deadline&.check!
        end

        def connect
          check_cancellation!
          # begin_transport can reconnect a closed socket even with retries
          # disabled. A second socket would escape the call's watchdog.
          raise IOError, "Owned connection is closed" if @socket

          super
        end

        def begin_transport(request)
          check_cancellation!
          super
          request["connect-timeout-ms"] = deadline.timeout_ms if deadline
        end

        def ssl_socket_connect(socket, _timeout)
          socket_lifecycle&.attach(socket.to_io)
          check_cancellation!
          super(socket, deadline ? deadline.native_timeout : Deadline::MAX_NATIVE_WAIT)
          check_cancellation!
        end
      end

      # Owns only this call's raw socket. Closing wakes Net::HTTP's blocking
      # read/write waits even when individual header/body reads keep progressing.
      class Watchdog
        def initialize(socket, deadline)
          @mutex = Mutex.new
          @condition = ConditionVariable.new
          @done = false
          @thread = Thread.new { watch(socket, deadline) }
        end

        def watch(socket, deadline)
          @mutex.synchronize do
            @condition.wait(@mutex, deadline.native_timeout) until @done || deadline.expired?
            close_socket(socket) unless @done
          end
        end

        def close_socket(socket)
          socket.shutdown(Socket::SHUT_RDWR)
          socket.close
        rescue IOError, SystemCallError
          # Net::HTTP may have closed the same owned socket first.
          nil
        end

        def stop
          @mutex.synchronize do
            @done = true
            @condition.signal
          end
          @thread.join
        end
      end

      # Retirement and callback closure share this guard. Unregister alone
      # cannot prevent an already-running callback from touching a reused FD.
      class SocketLifecycle
        def initialize(cancellation)
          @mutex = Mutex.new
          @socket = nil
          @closed = false
          @retired = false
          @registration = cancellation&.on_cancel { close }
        end

        def attach(socket)
          @mutex.synchronize do
            return if @retired

            @socket = socket
            close_socket if @closed
          end
        end

        def close
          @mutex.synchronize do
            return if @retired

            @closed = true
            close_socket
          end
        end

        def retire
          @mutex.synchronize do
            @retired = true
            @socket = nil
          end
          @registration&.unregister
          @registration = nil
        end

        private

        def close_socket
          @socket&.shutdown(Socket::SHUT_RDWR)
          @socket&.close
        rescue IOError, SystemCallError
          nil
        end
      end

      def call(request)
        CallCancellation.with_request(request) { |outgoing| perform_call(outgoing) }
      end

      private

      def perform_call(request) # rubocop:disable Metrics/MethodLength
        deadline = request.deadline
        outgoing = prepare_request(request)
        http = connection(request.url, deadline)
        http.cancellation = request.cancellation
        with_connection(http, deadline, request.cancellation) { exchange(http, request, outgoing) }
      rescue Net::OpenTimeout, Net::ReadTimeout, Net::WriteTimeout,
             SocketError, IOError, SystemCallError, Net::HTTPBadResponse, Net::HTTPHeaderSyntaxError,
             OpenSSL::SSL::SSLError => e
        request.lifecycle&.record_failure(e)
        check_request!(request)
        raise Error.new(:unavailable, "HTTP transport failed"), cause: nil
      end

      def check_request!(request)
        request.cancellation ? request.cancellation.check! : request.deadline&.check!
      end

      def prepare_request(request)
        check_request!(request)
        outgoing = outgoing_request(request)
        check_request!(request)
        outgoing
      end

      # Keep startup ownership and terminal retirement in one ensure.
      # rubocop:disable Metrics/MethodLength, Metrics/AbcSize, Metrics/CyclomaticComplexity
      def with_connection(http, deadline, cancellation = nil)
        sockets = SocketLifecycle.new(cancellation)
        http.socket_lifecycle = sockets
        cancellation&.check!
        http.start
        sockets.attach(http.instance_variable_get(:@socket).io.to_io)
        cancellation ? cancellation.check! : deadline&.check!
        # Pinned Net::HTTP exposes its BufferedIO; one call owns this raw socket.
        watchdog = Watchdog.new(http.instance_variable_get(:@socket).io.to_io, deadline) if deadline
        yield
      ensure
        watchdog&.stop
        sockets&.retire
        http.finish if http.started?
      end

      # rubocop:enable Metrics/MethodLength, Metrics/AbcSize, Metrics/CyclomaticComplexity

      def exchange(http, request, outgoing)
        result = nil
        http.request(outgoing) do |response|
          check_request!(request)
          check_response_size!(response, request.response_limit)
          body = read_body(response, request)
          result = TransportResponse.new(status: response.code.to_i, headers: response.to_hash, body: body)
        end
        check_request!(request)
        result
      end

      def check_response_size!(response, limit)
        length = Protocol.content_length!(response.get_fields("content-length"), invalid_code: :internal)
        raise Error.new(:resource_exhausted, "Response exceeds byte limit") if length && length > limit
      end

      def outgoing_request(request) # rubocop:disable Metrics/MethodLength, Metrics/AbcSize
        headers = request.headers.merge("connection" => ["close"])
        accept = request.headers.fetch("accept-encoding", ["identity"]).join(", ")
        has_body = request.http_method != "GET"
        outgoing = Net::HTTPGenericRequest.new(request.http_method, has_body, true, request.url.request_uri,
                                               "accept-encoding" => accept)
        headers.each do |key, values|
          next if key == "accept-encoding"

          values.each { |value| outgoing.add_field(key, value) }
        end
        outgoing.body = request.body unless request.http_method == "GET"
        outgoing
      end

      def read_body(response, request)
        body = +"".b
        response.read_body do |chunk|
          check_request!(request)
          if body.bytesize + chunk.bytesize > request.response_limit
            raise Error.new(:resource_exhausted, "Response exceeds byte limit")
          end

          body << chunk
        end
        body
      end

      def verify_tls!(http)
        http.verify_mode = OpenSSL::SSL::VERIFY_PEER
        http.verify_hostname = true
      end

      def connection(uri, deadline)
        http = Connection.new(uri.hostname, uri.port, nil)
        http.deadline = deadline
        http.use_ssl = uri.scheme == "https"
        configure_http(http, deadline)
        http
      end

      def configure_http(http, deadline)
        verify_tls!(http) if http.use_ssl?
        native_timeout = deadline ? deadline.native_timeout : Deadline::MAX_NATIVE_WAIT
        http.open_timeout = native_timeout
        http.read_timeout = native_timeout
        http.write_timeout = native_timeout
        http.max_retries = 0
        http.ignore_eof = false
      end
    end
  end
end
