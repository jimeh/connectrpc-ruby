# frozen_string_literal: true

module Conformance
  module Framing
    MAX_SIZE = 16 * 1024 * 1024

    def self.read(io, type)
      prefix = io.read(4)
      return if prefix.nil?
      raise "Truncated frame prefix" unless prefix.bytesize == 4

      length = prefix.unpack1("N")
      raise "Oversized frame: #{length}" if length > MAX_SIZE

      bytes = io.read(length)
      raise "Truncated frame body" unless bytes && bytes.bytesize == length

      type.decode(bytes)
    end

    def self.write(io, message)
      bytes = message.to_proto
      raise "Oversized frame: #{bytes.bytesize}" if bytes.bytesize > MAX_SIZE

      io.write([bytes.bytesize].pack("N"), bytes)
      io.flush
    end
  end
end
