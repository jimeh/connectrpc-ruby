# frozen_string_literal: true

module ConnectRPC
  module Async
    # Server HTTP/2 keepalive, PING enforcement and connection age, with
    # grpc-go's semantics. After `time` seconds without reading a frame the
    # server sends a PING and aborts the connection unless its ACK arrives
    # within `timeout`. Client PINGs closer than `min_time` apart (2 hours
    # apart without calls, unless `permit_without_calls`) earn strikes; the
    # third strike ends the connection with GOAWAY ENHANCE_YOUR_CALM
    # "too_many_pings". A connection older than `max_connection_age` (+/-10%)
    # drains gracefully, and is aborted `max_connection_age_grace` seconds
    # later. `time: nil` disables server PINGs, `min_time: nil` enforcement
    # and `max_connection_age: nil` the age limit.
    class ServerKeepalive
      attr_reader :time, :timeout, :min_time, :permit_without_calls, :max_connection_age, :max_connection_age_grace

      def initialize(time: 7200, timeout: 20, min_time: 5, permit_without_calls: true, max_connection_age: nil,
                     max_connection_age_grace: nil)
        @time = time.nil? ? nil : Internal.seconds(time, "time")
        @timeout = Internal.seconds(timeout, "timeout")
        @min_time = min_time.nil? ? nil : Internal.seconds(min_time, "min_time")
        unless [true, false].include?(permit_without_calls)
          raise ArgumentError, "permit_without_calls must be true or false"
        end

        @permit_without_calls = permit_without_calls
        @max_connection_age = max_connection_age.nil? ? nil : Internal.seconds(max_connection_age, "max_connection_age")
        unless max_connection_age_grace.nil?
          raise ArgumentError, "max_connection_age_grace requires max_connection_age" if @max_connection_age.nil?

          max_connection_age_grace = Internal.seconds(max_connection_age_grace, "max_connection_age_grace", zero: true)
        end
        @max_connection_age_grace = max_connection_age_grace
        freeze
      end

      def ==(other) = other.is_a?(ServerKeepalive) && values == other.values
      def eql?(other) = self == other
      def hash = [ServerKeepalive, *values].hash

      protected

      def values = [@time, @timeout, @min_time, @permit_without_calls, @max_connection_age, @max_connection_age_grace]
    end
  end
end
