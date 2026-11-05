# frozen_string_literal: true

require "protocol/http1/connection"
require_relative "http1_stream"

module ConnectRPC
  module Async
    module Internal
      class HTTP1Connection < ::Protocol::HTTP1::Connection
        STATUS = %r{\AHTTP/1\.1 ([0-9]{3}) [\x09\x20-\x7e\x80-\xff]*\z}n

        attr_reader :response_version

        def read_head(method)
          raise IOError, "HTTP/1 connection is not open" unless open? || half_closed_local?

          interim = 0
          loop do
            line = read_line
            match = STATUS.match(line)
            raise IOError, "Invalid HTTP/1.1 status line" unless match

            @response_version = "HTTP/1.1"
            status = match[1].to_i
            headers = read_headers
            leading = snapshot(headers)
            validate_framing!(leading)
            raise IOError, "HTTP upgrade is unsupported" if status == 101

            # The pinned native parser compares the transfer token literally.
            # Our validation already permits exactly one case-insensitive token.
            headers["transfer-encoding"] = "chunked" if leading.key?("transfer-encoding")
            @persistent &&= persistent?(@response_version, method, headers)
            if status.between?(100, 199)
              interim += 1
              raise IOError, "Too many interim responses" if interim > 16

              next
            end
            body = read_response_body(method, status, headers)
            receive_end_stream! unless body
            @count += 1
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
            raise IOError, "Invalid HTTP Content-Length"
          end
          return unless coding && (lengths || coding.size != 1 || !coding.first.strip.casecmp?("chunked"))

          raise IOError, "Unsupported HTTP transfer framing"
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
      class HTTP1Resource
        attr_accessor :raw, :socket, :connection, :setup

        def finished? = !!@finished
        def finish! = @finished = true

        def abort
          return if @raw.nil? || @raw.closed?

          pending_tls = @socket.is_a?(OpenSSL::SSL::SSLSocket) && @connection.nil?
          # Ruby 4 marks raw closed before waiting for blocked fibers, before
          # closing the descriptor. Transfer descriptor ownership first so an
          # interruption cannot strand it behind raw.closed?. This handle has
          # no blocked readers/writers and is never used for transport IO.
          @raw_close_io ||= ::IO.for_fd(@raw.fileno)
          @raw.autoclose = false
          # Ruby 4 raw close can wait on SSL setup. Shutdown wakes it first.
          @raw.shutdown(Socket::SHUT_RDWR)
        rescue IOError, SystemCallError
          nil
        ensure
          begin
            Internal.raw_close(@raw)
          ensure
            begin
              Internal.raw_close(@raw_close_io) if @raw&.closed?
            ensure
              # Ruby 4 raw close of pending SSL setup can leave its abandoned
              # EOF in OpenSSL's thread-local queue, including on interruption.
              # Drain only this destructive setup boundary before sibling TLS.
              OpenSSL.errors if pending_tls
            end
          end
        end

        def close
          @connection&.close
        ensure
          @socket&.close unless @socket&.closed?
        end
      end
    end
  end
end
