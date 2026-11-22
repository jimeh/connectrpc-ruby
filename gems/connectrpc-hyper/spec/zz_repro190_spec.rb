# frozen_string_literal: true

# REPRO190 temporary diagnostic; remove before Ready. Tallies how the original
# grpc-go "hang" call ends, to see the peer's deadline reset on each lane.
RSpec.describe "REPRO190" do
  it "tallies grpc-go hang outcomes" do
    peer_client = client(peer["grpc_h2c"], transport: hyper)
    results = Array.new(20) do
      started = monotonic
      error = error_of { peer_client.unary(MSG.new(value: "hang"), timeout: 0.3) }
      elapsed = ((monotonic - started) * 1000).round
      [error.code, error.cause&.message, elapsed < 300 ? "early" : "late", elapsed]
    end
    warn "REPRO190 #{RUBY_PLATFORM} #{RUBY_VERSION}: #{results.map { |r| r.first(3) }.tally.inspect}"
    warn "REPRO190 elapsed ms: #{results.map(&:last).inspect}"
  end
end
