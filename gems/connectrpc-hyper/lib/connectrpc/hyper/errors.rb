# frozen_string_literal: true

module ConnectRPC
  module Hyper
    module Native
      # Raised by the extension. `kind` classifies the failure; `reset_code` is
      # the HTTP/2 error code of an RST_STREAM or GOAWAY, the peer's or h2's own;
      # `errno` is the OS error, if any. Messages never carry paths or key
      # material.
      class Failure < StandardError
        attr_reader :kind, :reset_code, :errno

        def initialize(message, kind, reset_code, errno)
          super(message)
          @kind = kind
          @reset_code = reset_code
          @errno = errno
        end
      end
    end

    # Fixed, non-secret phrases naming what failed (core's transport phrases).
    # Local failures keep the native failure as their cause.
    module Errors
      CLOSED = "HTTP transport admission is closed"
      PHRASES = {
        dns: "name resolution failed", refused: "connection refused", unreachable: "host unreachable",
        connect: "connection failed", connect_timeout: "timed out", timeout: "timed out",
        tls_verify: "TLS verification failed", tls: "TLS error", closed: "connection closed",
        goaway: "connection closed", shutdown: "connection closed", protocol: "malformed response",
        alpn: "ALPN negotiation failed", reset: "stream reset",
        # Core NetHTTP's proxy phrases. Proxy reply bytes never reach them.
        proxy_auth: "proxy authentication failed", proxy_refused: "proxy tunnel refused",
        proxy_failed: "proxy tunnel failed", proxy_timeout: "proxy tunnel timed out",
        proxy_scheme: "unsupported proxy scheme", proxy_address: "invalid proxy address",
        proxy_cgi: "proxy refused in CGI environment"
      }.freeze
      # The Async transport's mapping of server RST_STREAM codes for gRPC.
      GRPC_RESET = { 3 => :resource_exhausted, 7 => :unavailable, 8 => :canceled, 11 => :resource_exhausted,
                     12 => :permission_denied }.freeze

      # A proxy the call must not use, raised before any socket opens.
      def self.proxy(kind) = Native::Failure.new(PHRASES.fetch(kind), kind, nil, nil)

      def self.transport(kind)
        reason = PHRASES[kind]
        Error.new(:unavailable, reason ? "HTTP transport failed: #{reason}" : "HTTP transport failed")
      end

      # Maps a native failure; deadline and cancellation are the caller's
      # arbiter's to raise first.
      def self.translate(failure, grpc:)
        case failure.kind
        when :admission then raise Error.new(:unavailable, CLOSED), cause: nil
        when :response_limit then raise Error.new(:resource_exhausted, "Response exceeds byte limit"), cause: nil
        when :field_limit then raise Error.new(:resource_exhausted, "gRPC field block exceeds byte limit"), cause: nil
        when :reset
          if grpc
            raise Error.new(GRPC_RESET.fetch(failure.reset_code, :internal), "HTTP/2 stream reset"), cause: failure
          end
        end
        raise transport(failure.kind), cause: failure
      end

      # Runs one native operation for a request: cancellation and deadline
      # first, then lifecycle failure recording (a per-wait timeout included,
      # as the Async transport records it), then the fixed phrase.
      def self.native(request, grpc:)
        yield
      rescue Native::Failure => e
        request.cancellation ? request.cancellation.check! : request.deadline&.check!
        request.lifecycle&.record_failure(e)
        raise Error.new(:unavailable, "HTTP transport failed: timed out"), cause: nil if e.kind == :timeout

        translate(e, grpc: grpc)
      end
    end
  end
end
