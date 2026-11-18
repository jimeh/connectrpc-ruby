# frozen_string_literal: true

require "json"
require "digest"

module Conformance
  module Telemetry
    def self.wire(body, headers)
      { encoding: Array(headers["content-encoding"]).first || "identity",
        content_length: Array(headers["content-length"]).first,
        bytes: body.bytesize, sha256: Digest::SHA256.hexdigest(body),
        gzip_magic: body.b.start_with?("\x1f\x8b".b) }
    end

    # Fixture-only observation, independent of the core parser. Official runner
    # requests are valid forms; retain payload hashes separately from HTTP bodies.
    def self.query(query)
      return unless query && !query.empty?

      values = URI.decode_www_form(query).to_h
      message = values.fetch("message")
      if values["base64"] == "1"
        message += "=" * ((4 - (message.bytesize % 4)) % 4)
        message = message.tr("-_", "+/").unpack1("m0")
      end
      { controls: values.except("message"),
        payload: wire(message, "content-encoding" => [values.fetch("compression", "identity")]) }
    end

    class Input
      attr_reader :bytes

      def initialize(input)
        @input = input
        @bytes = 0
        @digest = Digest::SHA256.new
        @prefix = +"".b
      end

      def read(length)
        chunk = @input&.read(length)
        if chunk
          @bytes += chunk.bytesize
          @digest.update(chunk)
          @prefix << chunk.byteslice(0, 2 - @prefix.bytesize) if @prefix.bytesize < 2
        end
        chunk
      end

      def observation
        { bytes: @bytes, sha256: @digest.hexdigest, gzip_magic: @prefix == "\x1f\x8b".b }
      end
    end

    # Adapter processes share the file through flock. The write runs as a
    # blocking fiber, so the fiber scheduler never runs another fiber while
    # this one holds the lock: on Ruby 4 IO#write can otherwise yield there,
    # and a second fiber waiting in flock would block the reactor thread that
    # has to resume the holder. Nor can another fiber run and cancel the
    # writer partway through, which would lose a record made while unwinding.
    def self.record(event, **fields)
      target = ENV.fetch("CONFORMANCE_TELEMETRY", nil)
      return unless target

      line = JSON.generate({ event: event, pid: Process.pid, monotonic: Process.clock_gettime(Process::CLOCK_MONOTONIC) }
                             .merge(fields))
      Fiber.blocking do
        File.open(target, "a") do |file|
          file.flock(File::LOCK_EX)
          file.puts(line)
        end
      end
    end
  end
end
