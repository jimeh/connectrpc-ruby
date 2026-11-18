# frozen_string_literal: true

module ConnectRPC
  module Async
    module Internal
      # grpc-go 1.85's bandwidth-delay product estimator
      # (internal/transport/bdp_estimator.go). A receiver samples the DATA
      # bytes that arrive between writing a PING and reading its ACK. When a
      # sample is at least BETA of the current estimate and gives the highest
      # bandwidth seen so far, the estimate becomes GAMMA times the sample,
      # capped at `limit`, and the connection grows its receive windows to it.
      #
      # A sample starts with the first DATA after the previous one ended and
      # stays pending until its PING is written. Only an ACK carrying PING,
      # read after that write, ends it; early, duplicate and unsolicited ACKs
      # are ignored. Not thread-safe: one connection's reader and writer share
      # a reactor.
      class BDPEstimator
        PING = [2, 4, 16, 16, 9, 14, 7, 7].pack("C*").freeze
        ALPHA = 0.9
        BETA = 0.66
        GAMMA = 2
        # grpc-go averages the first RTT samples before weighting recent ones.
        BOOTSTRAP_SAMPLES = 10

        # The current estimate in bytes, and the smoothed round-trip time in
        # seconds.
        attr_reader :estimate, :rtt

        def initialize(window:, limit:, clock: MonotonicClock)
          @estimate = window
          @limit = limit
          @clock = clock
          @sample = 0
          @samples = 0
          @rtt = 0.0
          @bandwidth_max = 0.0
          @state = nil
          @sent_at = nil
        end

        # Counts `bytes` of received DATA (grpc-go's `add`). Returns true when
        # they start a new sample, whose PING the caller must send. No sample
        # starts once the estimate reached the limit.
        def starts_sample?(bytes)
          if @state
            @sample += bytes
            return false
          end
          return false if @estimate >= @limit

          @state = :pending
          @sample = bytes
          @samples += 1
          true
        end

        # The sample's PING is being written now.
        def sent
          return unless @state == :pending

          @state = :sent
          @sent_at = @clock.now
        end

        # The sample's PING will not be sent; the next DATA starts a new one.
        def abandon
          return unless @state == :pending

          @state = nil
          @sample = 0
          @samples -= 1
        end

        # Returns the grown estimate, or nil when the ACK ends no sample or
        # the sample does not meet the growth rule.
        def acknowledged(payload)
          return unless @state == :sent && payload == PING

          @state = nil
          rtt = @clock.now - @sent_at
          @rtt += if @samples < BOOTSTRAP_SAMPLES
                    (rtt - @rtt) / @samples
                  else
                    (rtt - @rtt) * ALPHA
                  end
          bandwidth = @sample / (@rtt * 1.5)
          @bandwidth_max = bandwidth if bandwidth > @bandwidth_max
          return unless @sample >= BETA * @estimate && bandwidth == @bandwidth_max && @estimate < @limit

          @estimate = [GAMMA * @sample, @limit].min
        end
      end
    end
  end
end
