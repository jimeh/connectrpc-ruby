# frozen_string_literal: true

# REPRO190 temporary diagnostic; remove before Ready. Tallies how the original
# grpc-go "hang" call ends on each lane, and what ended the peer's handler:
# "deadline exceeded" means grpc-go's own timer beat the client's reset.
RSpec.describe "REPRO190" do
  it "tallies grpc-go hang outcomes" do
    from = peer.mark
    peer_client = client(peer["grpc_h2c"], transport: hyper)
    results = Array.new(40) do
      started = monotonic
      error = error_of { peer_client.unary(MSG.new(value: "hang"), timeout: 0.3) }
      [error.code, error.cause&.message, ((monotonic - started) * 1000).round]
    end
    peer.wait_count(/grpc unary-hang-done/, 40, from: from)
    server = peer.since(from).grep(/grpc unary-hang-done/).map { |event| event.split(" ", 3).last }
    warn "REPRO190 #{RUBY_PLATFORM} #{RUBY_VERSION}: #{results.map { |r| r.first(2) }.tally.inspect}"
    warn "REPRO190 server: #{server.tally.inspect}"
    warn "REPRO190 elapsed ms: #{results.map(&:last).inspect}"
  end
end
