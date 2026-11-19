# frozen_string_literal: true

module ConnectRPC
  # Client HTTP/2 keepalive. After `time` seconds without reading a frame,
  # the client sends a PING; only its matching ACK within `timeout` seconds
  # proves the connection alive. Otherwise the connection retires and its
  # calls fail with `unavailable`. Without calls in progress it pings only
  # with `permit_without_calls`. Transports that support keepalive document
  # exactly which reads reset their clock.
  class Keepalive
    attr_reader :time, :timeout, :permit_without_calls

    def initialize(time:, timeout: 20, permit_without_calls: false)
      @time = seconds(time, "time")
      @timeout = seconds(timeout, "timeout")
      unless [true, false].include?(permit_without_calls)
        raise ArgumentError, "permit_without_calls must be true or false"
      end

      @permit_without_calls = permit_without_calls
      freeze
    end

    def ==(other) = other.is_a?(Keepalive) && values == other.values
    def eql?(other) = self == other
    def hash = [Keepalive, *values].hash

    protected

    def values = [@time, @timeout, @permit_without_calls]

    private

    def seconds(value, name)
      return value if value.is_a?(Numeric) && value.real? && value.finite? && value.positive?

      raise ArgumentError, "#{name} must be finite positive seconds"
    end
  end
end
