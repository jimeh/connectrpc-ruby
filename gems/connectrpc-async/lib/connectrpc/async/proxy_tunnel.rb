# frozen_string_literal: true

require "socket"
require "connectrpc/proxy"

module ConnectRPC
  module Async
    module Internal
      # A proxy failure with a fixed message. Proxy-supplied bytes never reach
      # it, so it is safe as an error cause and in lifecycle failures.
      class ProxyFailure < StandardError; end

      # One HTTP CONNECT exchange through an http:// proxy, for a client
      # transport that owns the socket. HTTP/2 cannot use a forward proxy, so
      # every proxied target, h2c or TLS, tunnels. Go net/http bounds the
      # exchange by one minute and its response head by MaxResponseHeaderBytes'
      # default. Failures raise ProxyFailure with a fixed message that never
      # contains the proxy URL, credentials or proxy-supplied bytes.
      module ProxyTunnel
        TIMEOUT = 60
        MAX_HEAD = 10 * 1024 * 1024
        HEAD_END = /\r?\n\r?\n/n
        STATUS = %r{\AHTTP/1\.[01] ([0-9]{3})(?: [^\r\n]*)?\z}n
        FIELD = /\A[!#$%&'*+\-.^_`|~0-9A-Za-z]+:/n
        UNUSABLE = { cgi: "proxy refused in CGI environment", invalid_address: "invalid proxy address" }.freeze

        # The proxy for an origin, or nil, chosen by the origin's scheme as Go
        # net/http chooses it: an environment resolver applies HTTP_PROXY to
        # http (h2c) origins, refusing it in a CGI environment, and
        # HTTPS_PROXY to https origins. A proxy this module cannot use fails
        # rather than going direct.
        def self.select(resolver, origin)
          proxy = resolver.for(origin)
          raise ProxyFailure, "unsupported proxy scheme", cause: nil if proxy && proxy.scheme != "http"

          proxy
        rescue Proxy::Unusable => e
          raise ProxyFailure, UNUSABLE.fetch(e.reason), cause: nil
        end

        # Opens a tunnel to host:port. A TLS server never speaks first, so
        # bytes already queued after a 200 head came from the proxy. An h2c
        # server may send its SETTINGS at once, so the reply is read without
        # consuming anything after its head.
        def self.establish(socket, host, port, proxy, tls:)
          expires = Internal.now + TIMEOUT
          authority = host.include?(":") ? "[#{host}]:#{port}" : "#{host}:#{port}"
          head = "CONNECT #{authority} HTTP/1.1\r\nHost: #{authority}\r\n"
          head << "Proxy-Authorization: Basic #{["#{proxy.user}:#{proxy.password}"].pack('m0')}\r\n" if proxy.user
          write(socket, "#{head}\r\n", expires)
          reply, surplus = read(socket, expires)
          check_status(reply, !(tls && surplus))
        end

        def self.write(socket, data, expires)
          until data.empty?
            written = socket.write_nonblock(data, exception: false)
            if written == :wait_writable
              wait(socket, expires, :wait_writable)
            else
              data = data.byteslice(written..).to_s
              check!(expires)
            end
          end
        end

        # Peeks at queued bytes and consumes only those up to the end of the
        # head. Returns the head and whether more bytes were already queued.
        # The head so far has no delimiter, so one that ends in the peeked
        # bytes starts at most three bytes before them; only that tail and the
        # peeked bytes are searched, keeping the work per byte bounded.
        def self.read(socket, expires)
          head = +"".b
          loop do
            room = MAX_HEAD - head.bytesize
            raise ProxyFailure, "proxy tunnel failed", cause: nil unless room.positive?

            peeked = socket.recv_nonblock([16_384, room].min, Socket::MSG_PEEK, exception: false)
            next wait(socket, expires, :wait_readable) if peeked == :wait_readable
            raise ProxyFailure, "proxy tunnel failed", cause: nil if peeked.nil? || peeked.empty?

            check!(expires)
            offset = head.bytesize
            base = [offset - 3, 0].max
            ending = (head.byteslice(base..) + peeked).match(HEAD_END)
            length = ending ? base + ending.end(0) - offset : peeked.bytesize
            head << consume(socket, length)
            return [head.byteslice(0, base + ending.begin(0)).to_s, length < peeked.bytesize] if ending
          end
        end

        # The bytes were peeked, so they are already queued.
        def self.consume(socket, length)
          bytes = +"".b
          while bytes.bytesize < length
            chunk = socket.recv_nonblock(length - bytes.bytesize, 0, exception: false)
            raise ProxyFailure, "proxy tunnel failed", cause: nil unless chunk.is_a?(String) && !chunk.empty?

            bytes << chunk
          end
          bytes
        end

        # Status text and fields never leave this method.
        def self.check_status(head, complete)
          lines = head.split(/\r?\n/n)
          status = STATUS.match(lines.first.to_s)
          fields = lines.drop(1).all? { |line| FIELD.match?(line) }
          raise ProxyFailure, "proxy tunnel failed", cause: nil unless status && fields

          code = status[1].to_i
          raise ProxyFailure, "proxy authentication failed", cause: nil if code == 407
          raise ProxyFailure, "proxy tunnel refused", cause: nil unless code == 200
          raise ProxyFailure, "proxy tunnel failed", cause: nil unless complete
        end

        def self.wait(socket, expires, direction)
          loop do
            remaining = expires - Internal.now
            return if remaining.positive? && socket.public_send(direction, remaining)

            check!(expires)
          end
        end

        # The cap applies after every read and write as well as while
        # waiting, so a reply that arrives late is never accepted.
        def self.check!(expires)
          raise ProxyFailure, "proxy tunnel timed out", cause: nil unless Internal.now < expires
        end
        private_class_method :write, :read, :consume, :check_status, :wait, :check!
      end
    end
  end
end
