# frozen_string_literal: true

require "protocol/http1/connection"
require_relative "http1_stream"

module ConnectRPC
  module Async
    module Internal
      # A malformed HTTP/1 response head or unsupported framing.
      class HTTP1ResponseError < IOError; end

      class HTTP1Connection < ::Protocol::HTTP1::Connection
        STATUS = %r{\A(HTTP/1\.1) ([0-9]{3}) [\x09\x20-\x7e\x80-\xff]*\z}n
        # A forward proxy may answer in HTTP/1.0, as Net::HTTP and Go net/http
        # accept; its version then decides persistence.
        FORWARD_STATUS = %r{\A(HTTP/1\.[01]) ([0-9]{3}) [\x09\x20-\x7e\x80-\xff]*\z}n

        attr_reader :response_version

        def read_head(method, forward: false)
          raise IOError, "HTTP/1 connection is not open" unless open? || half_closed_local?

          interim = 0
          loop do
            line = read_line
            match = (forward ? FORWARD_STATUS : STATUS).match(line)
            raise HTTP1ResponseError, "Invalid HTTP/1.1 status line" unless match

            @response_version = match[1]
            status = match[2].to_i
            headers = read_headers
            leading = snapshot(headers)
            validate_framing!(leading)
            # RFC 9112 section 6.1: transfer coding in an HTTP/1.0 message is
            # faulty framing, so the connection never returns to the pool.
            if @response_version == "HTTP/1.0" && leading.key?("transfer-encoding")
              raise HTTP1ResponseError, "Unsupported HTTP transfer framing"
            end
            raise HTTP1ResponseError, "HTTP upgrade is unsupported" if status == 101

            # protocol-http1's parser compares the transfer token literally.
            # Our validation already permits exactly one case-insensitive token.
            headers["transfer-encoding"] = "chunked" if leading.key?("transfer-encoding")
            self.persistent &&= persistent?(@response_version, method, headers)
            if status.between?(100, 199)
              interim += 1
              raise HTTP1ResponseError, "Too many interim responses" if interim > 16

              next
            end
            body = read_response_body(method, status, headers)
            receive_end_stream! unless body
            return [StreamHead.new(status: status, headers: leading), body]
          end
        end

        def viable? = idle? && persistent && stream.viable?

        private

        def snapshot(headers)
          result = headers.header.each_with_object({}) do |(name, value), result|
            (result[name.downcase] ||= []) << value.dup.freeze
          end
          result.each_value(&:freeze)
          result.freeze
        end

        # Validate the original fields before the native parser deletes or
        # combines them. Only a single chunked transfer coding is supported.
        def validate_framing!(headers)
          lengths = headers["content-length"]
          coding = headers["transfer-encoding"]
          if lengths && (lengths.size != 1 || !/\A[0-9]+\z/.match?(lengths.first))
            raise HTTP1ResponseError, "Invalid HTTP Content-Length"
          end
          return unless coding && (lengths || coding.size != 1 || !coding.first.strip.casecmp?("chunked"))

          raise HTTP1ResponseError, "Unsupported HTTP transfer framing"
        end
      end

      class HTTP1Output < OutputBody
        def initialize(dispatched)
          super()
          @dispatched = dispatched
        end

        def ready? = false

        def read
          @dispatched.resolve(nil)
          super
        end
      end

      # One resource per reserved slot, including setup and destructive joins.
      # `forward` is the proxy an http:// origin's requests go through.
      class HTTP1Resource
        attr_accessor :raw, :socket, :connection, :setup, :forward

        def finished? = !!@finished
        def finish! = @finished = true
        def aborted? = !!@aborted

        # Fails the connection at once without closing its descriptor. On
        # Ruby 4, IO#close interrupts every fiber blocked on the socket, and
        # Async raises that IOError into the fiber wherever it next suspends,
        # even after it has left the read. A shutdown wakes those fibers, a
        # pending TLS handshake included, with EOF or EPIPE instead. Setup can
        # assign raw after an earlier abort, so every abort shuts down the raw
        # socket it finds; it never yields.
        def abort
          @aborted = true
          pending_tls = @socket.is_a?(OpenSSL::SSL::SSLSocket) && @connection.nil?
          @raw.shutdown(Socket::SHUT_RDWR) if @raw && !@raw.closed?
        rescue IOError, SystemCallError
          nil
        ensure
          # A failed pending TLS setup can leave its abandoned EOF in
          # OpenSSL's thread-local queue. Drain only this destructive setup
          # boundary before a sibling's TLS.
          OpenSSL.errors if pending_tls
        end

        # Only the final owner calls this, after all accessing tasks have
        # joined, so no fiber is still blocked on the descriptor it closes.
        # Every stage runs. Afterwards it raises the first interruption (an
        # error that is not a StandardError), reporting the first
        # StandardError, or else raises that first StandardError. Each later
        # StandardError is reported, and an interruption of that report is
        # kept like any other.
        def close
          failure = nil
          interruption = nil
          [@connection, @socket, @raw].compact.uniq.each do |owned|
            next if owned.equal?(@raw) && owned.closed?

            begin
              owned.close
            rescue Exception => e # rubocop:disable Lint/RescueException -- complete every close stage before propagating the first interruption
              if !e.is_a?(StandardError)
                interruption ||= e
              elsif failure
                interruption ||= Teardown.escape(e)
              else
                failure = e
              end
              # Native wrappers can be interrupted before completing their own
              # cleanup. Retry interruptions, but let ordinary wrapper failures
              # proceed to the remaining stages.
              retry if !e.is_a?(StandardError) && !(owned.equal?(@raw) && owned.closed?)
            end
          end
          if interruption
            # The interruption already held is the one raised.
            Teardown.escape(failure) if failure
            raise interruption
          end
          raise failure if failure
        end
      end
    end
  end
end
