# frozen_string_literal: true

require "net/http"
require "openssl"
require_relative "../../connectrpc"

module ConnectRPC
  module Transports
    class NetHTTP
      # Net::HTTP uses the open timeout again for TLS. Recompute it from the
      # original deadline after TCP/DNS, rather than starting another budget.
      class Connection < Net::HTTP
        attr_accessor :deadline

        private

        def connect
          deadline&.check!
          # begin_transport can reconnect a closed socket even with retries
          # disabled. A second socket would escape the call's watchdog.
          raise IOError, "Owned connection is closed" if @socket

          super
        end

        def begin_transport(request)
          deadline&.check!
          super
          request["connect-timeout-ms"] = deadline.timeout_ms if deadline
        end

        def ssl_socket_connect(socket, _timeout)
          deadline&.check!
          super(socket, deadline ? deadline.native_timeout : Deadline::MAX_NATIVE_WAIT)
          deadline&.check!
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

      def call(request)
        deadline = request.deadline
        outgoing = prepare_request(request)
        http = connection(request.url, deadline)
        with_connection(http, deadline) { exchange(http, request, outgoing) }
      rescue Net::OpenTimeout, Net::ReadTimeout, Net::WriteTimeout,
             SocketError, IOError, SystemCallError, Net::HTTPBadResponse, Net::HTTPHeaderSyntaxError,
             OpenSSL::SSL::SSLError => e
        request.lifecycle&.record_failure(e)
        deadline&.check!
        raise Error.new(:unavailable, "HTTP transport failed")
      end

      private

      def prepare_request(request)
        request.deadline&.check!
        outgoing = outgoing_request(request)
        request.deadline&.check!
        outgoing
      end

      def with_connection(http, deadline)
        http.start
        deadline&.check!
        # Net::HTTP has no public connection-close hook usable concurrently
        # with request. These pinned stdlib versions expose their BufferedIO.
        watchdog = Watchdog.new(http.instance_variable_get(:@socket).io.to_io, deadline) if deadline
        yield
      ensure
        watchdog&.stop
        http.finish if http.started?
      end

      def exchange(http, request, outgoing)
        result = nil
        http.request(outgoing) do |response|
          request.deadline&.check!
          check_response_size!(response, request.response_limit)
          body = read_body(response, request)
          result = TransportResponse.new(status: response.code.to_i, headers: response.to_hash, body: body)
        end
        request.deadline&.check!
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
          request.deadline&.check!
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
