# frozen_string_literal: true

module ConnectRPC
  module Async
    # Capped, jittered delay between failed connection attempts. Jitter only
    # shortens a delay, so `max` is a hard maximum.
    class Backoff
      attr_reader :base, :multiplier, :jitter, :max

      def initialize(base: 1.0, multiplier: 1.6, jitter: 0.2, max: 30.0)
        @base = Internal.seconds(base, "base")
        @multiplier = Internal.finite(multiplier, "multiplier", "at least 1") { |value| value >= 1 }
        @jitter = Internal.finite(jitter, "jitter", "in 0...1") { |value| value >= 0 && value < 1 }
        @max = Internal.finite(max, "max", "at least base") { |value| value >= @base }
        freeze
      end

      def ==(other) = other.is_a?(Backoff) && values == other.values
      def eql?(other) = self == other
      def hash = [Backoff, *values].hash

      protected

      def values = [@base, @multiplier, @jitter, @max]
    end

    # Connection behavior for the HTTP/2 Transport. Every feature can be turned
    # off: `backoff: nil` and `max_pending_bytes: nil`.
    class ConnectionPolicy
      attr_reader :backoff, :wait_for_ready, :max_pending_bytes

      def initialize(backoff: Backoff.new, wait_for_ready: false, max_pending_bytes: 16 * 1024 * 1024)
        raise ArgumentError, "backoff must be a Backoff or nil" unless backoff.nil? || backoff.is_a?(Backoff)
        raise ArgumentError, "wait_for_ready must be true or false" unless [true, false].include?(wait_for_ready)
        raise ArgumentError, "wait_for_ready requires a backoff" if wait_for_ready && backoff.nil?
        unless max_pending_bytes.nil? || (max_pending_bytes.is_a?(Integer) && max_pending_bytes.positive?)
          raise ArgumentError, "max_pending_bytes must be a positive integer or nil"
        end

        @backoff = backoff
        @wait_for_ready = wait_for_ready
        @max_pending_bytes = max_pending_bytes
        freeze
      end

      def ==(other) = other.is_a?(ConnectionPolicy) && values == other.values
      def eql?(other) = self == other
      def hash = [ConnectionPolicy, *values].hash

      protected

      def values = [@backoff, @wait_for_ready, @max_pending_bytes]
    end

    # A connection observation. It carries counts and classifications only:
    # no addresses, certificates, payloads, URLs or exception text.
    ConnectionEvent = Data.define(:type, :state, :previous_state, :outcome, :duration, :active, :draining,
                                  :pending_calls, :pending_bytes)
  end
end
