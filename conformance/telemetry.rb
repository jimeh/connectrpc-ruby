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

    def self.record(event, **fields)
      target = ENV.fetch("CONFORMANCE_TELEMETRY", nil)
      return unless target

      File.open(target, "a") do |file|
        file.flock(File::LOCK_EX)
        file.puts(JSON.generate({ event: event, pid: Process.pid, monotonic: Process.clock_gettime(Process::CLOCK_MONOTONIC) }
                                 .merge(fields)))
      end
    end
  end
end
