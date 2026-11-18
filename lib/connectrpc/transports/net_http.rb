# frozen_string_literal: true

require "net/http"
require "openssl"
require "resolv"
require "securerandom"
require "socket"
require_relative "../../connectrpc"
require_relative "../proxy"

module ConnectRPC
  module Transports
    class NetHTTP # rubocop:disable Metrics/ClassLength
      # Go net/http bounds a proxy CONNECT exchange by one minute and its
      # response head by MaxResponseHeaderBytes' default.
      TUNNEL_TIMEOUT = 60
      MAX_TUNNEL_HEAD = 10 * 1024 * 1024
      UNUSABLE_PROXY = { cgi: "proxy refused in CGI environment", invalid_address: "invalid proxy address" }.freeze

      # A proxy failure with a fixed message. Proxy-supplied bytes never reach
      # it, so it is safe as an error cause and in lifecycle failures.
      class ProxyFailure < StandardError; end

      # Validates TLS client inputs exactly as ConnectRPC::Async's TLS client
      # does and builds the one context every call of a transport reuses.
      module ClientTLS
        CERTIFICATE = %r{\A-----BEGIN CERTIFICATE-----\s+[A-Za-z0-9+/=\s]+-----END CERTIFICATE-----\z}
        PRIVATE_KEY = %r{
          \A-----BEGIN\ (RSA\ PRIVATE\ KEY|EC\ PRIVATE\ KEY|PRIVATE\ KEY)-----
          \s+[A-Za-z0-9+/=\s]+-----END\ \1-----\z
        }x

        # Names the input that failed validation, never its value.
        class InvalidInput < ArgumentError
          attr_reader :input

          def initialize(input)
            @input = input
            super("Invalid #{input}")
          end
        end

        def self.certificate(pem)
          raise ArgumentError unless pem.is_a?(String) && CERTIFICATE.match?(pem.strip)

          certificates = OpenSSL::X509::Certificate.load(pem.dup)
          raise ArgumentError unless certificates.size == 1

          certificate = certificates.first
          raise ArgumentError unless certificate.not_before <= Time.now && certificate.not_after > Time.now

          certificate
        end

        def self.input(name)
          yield
        rescue InvalidInput
          raise
        rescue StandardError
          raise InvalidInput.new(name), cause: nil
        end

        def self.identity(chain, pem) # rubocop:disable Metrics/MethodLength, Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
          certificates = input(:certificate_chain) do
            raise ArgumentError unless chain.is_a?(Array) && !chain.empty?

            chain.map { |element| certificate(element) }
          end
          key = input(:private_key) do
            raise ArgumentError unless pem.is_a?(String) && PRIVATE_KEY.match?(pem.strip)

            OpenSSL::PKey.read(pem.dup) { nil }
          end
          input(:private_key) do
            digest = %w[ED25519 ED448].include?(key.oid) ? nil : "SHA256"
            challenge = SecureRandom.random_bytes(32)
            signature = key.sign(digest, challenge)
            raise ArgumentError unless certificates.first.public_key.verify(digest, signature, challenge)
          end
          input(:certificate_chain) do
            certificates.each_cons(2) do |child, issuer|
              raise ArgumentError unless child.issuer == issuer.subject && child.verify(issuer.public_key)
            end
          end
          [certificates, key]
        end

        # RFC 6066 section 3 forbids a server name for an IP literal.
        def self.server_name(host)
          host unless Resolv::IPv4::Regex.match?(host) || Resolv::IPv6::Regex.match?(host)
        end

        def self.context(ca_file:, certificate_chain:, private_key:) # rubocop:disable Metrics/MethodLength, Metrics/AbcSize
          context = OpenSSL::SSL::SSLContext.new
          context.set_params(verify_mode: OpenSSL::SSL::VERIFY_PEER, verify_hostname: true)
          context.min_version = OpenSSL::SSL::TLS1_2_VERSION
          context.options |= OpenSSL::SSL::OP_NO_TICKET
          context.session_cache_mode = OpenSSL::SSL::SSLContext::SESSION_CACHE_OFF
          context.session_id_context = SecureRandom.random_bytes(16)
          store = OpenSSL::X509::Store.new
          store.set_default_paths
          input(:ca_file) { store.add_file(ca_file) } if ca_file
          context.cert_store = store
          if certificate_chain || private_key
            certificates, key = identity(certificate_chain, private_key)
            context.add_certificate(certificates.first, key, certificates.drop(1))
          end
          context.alpn_protocols = ["http/1.1"]
          context.setup
          context
        rescue InvalidInput => e
          # Errors name only the input: never paths or key material.
          raise ArgumentError, "Invalid TLS client configuration: #{e.input}", cause: nil
        rescue StandardError
          raise ArgumentError, "Invalid TLS client configuration", cause: nil
        end
      end
      private_constant :ClientTLS

      # Owns TCP, proxy CONNECT and TLS setup so every socket belongs to the
      # call from creation. Net::HTTP keeps request writing and response parsing.
      class Connection < Net::HTTP # rubocop:disable Metrics/ClassLength
        TUNNEL_HEAD_END = /\r?\n\r?\n/n
        TUNNEL_STATUS = %r{\AHTTP/1\.[01] ([0-9]{3})(?: [^\r\n]*)?\z}n
        TUNNEL_FIELD = /\A[!#$%&'*+\-.^_`|~0-9A-Za-z]+:/n

        attr_accessor :deadline, :cancellation, :socket_lifecycle, :tls_context, :tunnel

        private

        def check_cancellation!
          cancellation ? cancellation.check! : deadline&.check!
        end

        def native_wait
          deadline ? deadline.native_timeout : Deadline::MAX_NATIVE_WAIT
        end

        def connect # rubocop:disable Metrics/MethodLength
          check_cancellation!
          # begin_transport can reconnect a closed socket even with retries
          # disabled. A second socket would escape the call's watchdog.
          raise IOError, "Owned connection is closed" if @socket

          connected = false
          raw = open_socket(*dial_target)
          establish_tunnel(raw) if tunnel
          socket = use_ssl? ? start_tls(raw) : raw
          @socket = Net::BufferedIO.new(socket, read_timeout: @read_timeout, write_timeout: @write_timeout,
                                                continue_timeout: @continue_timeout, debug_output: @debug_output)
          @last_communicated = nil
          on_connect
          connected = true
        ensure
          close_quietly(raw) if raw && !connected
        end

        def dial_target
          endpoint = tunnel
          return [endpoint.host, endpoint.port] if endpoint
          return [proxy_address, proxy_port] if proxy?

          [address, port]
        end

        # Addresses are tried one at a time. Each socket is owned before its
        # connect starts, so cancellation and expiry close whichever is live.
        def open_socket(host, port) # rubocop:disable Metrics/MethodLength
          addresses = Addrinfo.getaddrinfo(host, port, nil, :STREAM, nil, 0, timeout: native_wait)
          failure = nil
          addresses.each do |address|
            check_cancellation!
            socket = Socket.new(address.afamily, Socket::SOCK_STREAM, 0)
            socket_lifecycle&.attach(socket)
            begin
              connect_socket(socket, address)
              socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
              return socket
            rescue IOError, SystemCallError, Net::OpenTimeout => e
              close_quietly(socket)
              failure ||= e
            end
          end
          check_cancellation!
          raise failure || SocketError.new("No addresses")
        end

        def connect_socket(socket, address)
          return if socket.connect_nonblock(address, exception: false) == 0 # rubocop:disable Style/NumericPredicate
          raise Net::OpenTimeout, "TCP connect timed out" unless socket.wait_writable(native_wait)

          socket.connect_nonblock(address)
        rescue Errno::EISCONN
          nil
        end

        def establish_tunnel(socket)
          expires_at = Process.clock_gettime(Process::CLOCK_MONOTONIC) + TUNNEL_TIMEOUT
          authority = address.include?(":") ? "[#{address}]:#{port}" : "#{address}:#{port}"
          head = "CONNECT #{authority} HTTP/1.1\r\nHost: #{authority}\r\n"
          if (endpoint = tunnel) && endpoint.user
            head << "Proxy-Authorization: Basic #{["#{endpoint.user}:#{endpoint.password}"].pack('m0')}\r\n"
          end
          write_tunnel(socket, "#{head}\r\n", expires_at)
          read_tunnel(socket, expires_at)
        end

        def write_tunnel(socket, data, expires_at)
          until data.empty?
            written = socket.write_nonblock(data, exception: false)
            if written == :wait_writable
              wait_tunnel(socket, expires_at, :wait_writable)
            else
              data = data.byteslice(written..).to_s
            end
          end
        end

        # A TLS server never speaks first, so bytes after a 200 head came from
        # the proxy. Status text and fields never leave this method.
        def read_tunnel(socket, expires_at) # rubocop:disable Metrics/MethodLength, Metrics/AbcSize
          head = +"".b
          loop do
            room = MAX_TUNNEL_HEAD - head.bytesize
            raise ProxyFailure, "proxy tunnel failed" unless room.positive?

            chunk = socket.read_nonblock([16_384, room].min, exception: false)
            next wait_tunnel(socket, expires_at, :wait_readable) if chunk == :wait_readable
            raise ProxyFailure, "proxy tunnel failed" if chunk.nil?

            head << chunk
            ending = head.match(TUNNEL_HEAD_END, [head.bytesize - chunk.bytesize - 3, 0].max)
            next unless ending

            return tunnel_status(head.byteslice(0, ending.begin(0)).to_s, ending.end(0) == head.bytesize)
          end
        end

        def tunnel_status(head, complete)
          lines = head.split(/\r?\n/n)
          status = TUNNEL_STATUS.match(lines.first.to_s)
          raise ProxyFailure, "proxy tunnel failed" unless status && lines.drop(1).all? { TUNNEL_FIELD.match?(it) }

          code = status[1].to_i
          raise ProxyFailure, "proxy authentication failed" if code == 407
          raise ProxyFailure, "proxy tunnel refused" unless code == 200
          raise ProxyFailure, "proxy tunnel failed" unless complete
        end

        # The CONNECT cap applies even without a call deadline. Call expiry
        # and cancellation keep their own outcomes.
        def wait_tunnel(socket, expires_at, direction)
          loop do
            wait = [native_wait, expires_at - Process.clock_gettime(Process::CLOCK_MONOTONIC)].min
            return if wait.positive? && socket.public_send(direction, wait)

            check_cancellation!
            if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= expires_at
              raise ProxyFailure, "proxy tunnel timed out"
            end
          end
        end

        def start_tls(raw)
          socket = OpenSSL::SSL::SSLSocket.new(raw, tls_context)
          socket.sync_close = true
          name = ClientTLS.server_name(address)
          socket.hostname = name if name
          ssl_socket_connect(socket, native_wait)
          socket.post_connection_check(address)
          socket
        end

        def close_quietly(socket)
          socket.close unless socket.closed?
        rescue IOError, SystemCallError
          nil
        end

        def begin_transport(request)
          check_cancellation!
          super
          request["connect-timeout-ms"] = deadline.timeout_ms if deadline
        end

        # Net::HTTP uses the open timeout again for TLS. Recompute it from the
        # original deadline after TCP/DNS, rather than starting another budget.
        def ssl_socket_connect(socket, _timeout)
          check_cancellation!
          super(socket, native_wait)
          check_cancellation!
        end
      end

      # Closes whatever socket the call owns when its deadline expires. Closing
      # wakes blocking waits even when individual reads keep progressing.
      class Watchdog
        def initialize(owner, deadline)
          @mutex = Mutex.new
          @condition = ConditionVariable.new
          @done = false
          @thread = Thread.new { watch(owner, deadline) }
        end

        def watch(owner, deadline)
          @mutex.synchronize do
            @condition.wait(@mutex, deadline.native_timeout) until @done || deadline.expired?
            owner.close unless @done
          end
        end

        def stop
          @mutex.synchronize do
            @done = true
            @condition.signal
          end
          @thread.join
        end
      end

      # Holds the call's current socket. Once closed by cancellation or the
      # watchdog, a later attach closes its socket at once. Retirement and
      # callback closure share this guard. Unregister alone cannot prevent an
      # already-running callback from touching a reused FD.
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

        # A socket still connecting cannot shut down; it must still close.
        def close_socket
          socket = @socket
          return unless socket

          begin
            socket.shutdown(Socket::SHUT_RDWR)
          rescue IOError, SystemCallError
            nil
          end
          socket.close
        rescue IOError, SystemCallError
          nil
        end
      end

      # proxy: :environment snapshots HTTP_PROXY, HTTPS_PROXY and NO_PROXY now.
      # A String or URI proxies every request; nil connects directly.
      def initialize(ca_file: nil, certificate_chain: nil, private_key: nil, proxy: :environment)
        @tls_options = !(ca_file.nil? && certificate_chain.nil? && private_key.nil?)
        @tls_context = ClientTLS.context(ca_file: ca_file, certificate_chain: certificate_chain,
                                         private_key: private_key)
        @proxy_mode = { environment: "environment", nil => "direct" }.fetch(proxy, "explicit")
        @proxy = Proxy.resolver(proxy)
        fixed = @proxy.is_a?(Proxy::Static) ? @proxy.endpoint : nil
        raise ArgumentError, "Unsupported proxy scheme" if fixed && fixed.scheme != "http"
      end

      # Never shows proxy URLs, credentials or TLS material.
      def inspect = "#<#{self.class} proxy=#{@proxy_mode}>"

      def call(request)
        raise ArgumentError, "TLS options require an HTTPS origin" if @tls_options && request.url.scheme != "https"

        CallCancellation.with_request(request) { |outgoing| perform_call(outgoing) }
      end

      private

      def perform_call(request) # rubocop:disable Metrics/MethodLength, Metrics/AbcSize
        deadline = request.deadline
        outgoing = prepare_request(request)
        http = connection(request.url, deadline, proxy_for(request.url))
        http.cancellation = request.cancellation
        with_connection(http, deadline, request.cancellation) { exchange(http, request, outgoing) }
      rescue Net::OpenTimeout, Net::ReadTimeout, Net::WriteTimeout,
             SocketError, IOError, SystemCallError, Net::HTTPBadResponse, Net::HTTPHeaderSyntaxError,
             OpenSSL::SSL::SSLError, ProxyFailure, Proxy::Unusable => e
        request.lifecycle&.record_failure(e)
        check_request!(request)
        reason = failure_reason(e)
        raise Error.new(:unavailable, reason ? "HTTP transport failed: #{reason}" : "HTTP transport failed"), cause: e
      end

      # A fixed, non-secret phrase naming what failed. These errors are local
      # to the caller, so they keep the original exception as their cause.
      def failure_reason(error) # rubocop:disable Metrics/MethodLength, Metrics/CyclomaticComplexity
        case error
        when ProxyFailure then error.message
        when Proxy::Unusable then UNUSABLE_PROXY[error.reason]
        when Errno::ECONNREFUSED then "connection refused"
        when SocketError then "name resolution failed"
        when Errno::EHOSTUNREACH, Errno::ENETUNREACH then "host unreachable"
        when Net::OpenTimeout, Net::ReadTimeout, Net::WriteTimeout, Errno::ETIMEDOUT, IO::TimeoutError then "timed out"
        when OpenSSL::SSL::SSLError
          verification = ["certificate verify failed", "does not match the server certificate"]
          verification.any? { |text| error.message.include?(text) } ? "TLS verification failed" : "TLS error"
        when EOFError, Errno::ECONNRESET, Errno::EPIPE, Errno::ECONNABORTED then "connection closed"
        when Net::HTTPBadResponse, Net::HTTPHeaderSyntaxError then "malformed response"
        end
      end

      # NetHTTP speaks only to http:// proxies; CONNECT carries https targets.
      def proxy_for(url)
        proxy = @proxy.for(url)
        raise ProxyFailure, "unsupported proxy scheme" if proxy && proxy.scheme != "http"

        proxy
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

      # Keep startup ownership and terminal retirement in one ensure. The
      # watchdog starts before TCP so it also bounds proxy CONNECT and TLS.
      def with_connection(http, deadline, cancellation = nil) # rubocop:disable Metrics/MethodLength, Metrics/CyclomaticComplexity
        sockets = SocketLifecycle.new(cancellation)
        http.socket_lifecycle = sockets
        cancellation&.check!
        watchdog = Watchdog.new(sockets, deadline) if deadline
        http.start
        cancellation ? cancellation.check! : deadline&.check!
        yield
      ensure
        watchdog&.stop
        sockets&.retire
        http.finish if http.started?
      end

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

      # The wire carries exactly the request's user-agent values, never
      # Net::HTTP's default "Ruby".
      def outgoing_request(request) # rubocop:disable Metrics/MethodLength, Metrics/AbcSize
        headers = request.headers.merge("connection" => ["close"])
        accept = request.headers.fetch("accept-encoding", ["identity"]).join(", ")
        has_body = request.http_method != "GET"
        outgoing = Net::HTTPGenericRequest.new(request.http_method, has_body, true, request.url.request_uri,
                                               "accept-encoding" => accept)
        outgoing.delete("user-agent")
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

      # Net::HTTP sends absolute-form requests and Proxy-Authorization to an
      # http:// target's proxy. An https:// target tunnels through CONNECT.
      def connection(uri, deadline, proxy = nil) # rubocop:disable Metrics/AbcSize
        forward = proxy if uri.scheme == "http"
        http = Connection.new(uri.hostname, uri.port, forward&.host, forward&.port, forward&.user, forward&.password)
        http.tunnel = proxy if uri.scheme == "https"
        http.tls_context = @tls_context
        http.deadline = deadline
        http.use_ssl = uri.scheme == "https"
        configure_http(http, deadline)
        http
      end

      def configure_http(http, deadline)
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
