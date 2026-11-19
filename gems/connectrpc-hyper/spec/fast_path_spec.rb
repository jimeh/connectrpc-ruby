# frozen_string_literal: true

RSpec.describe "Connect unary fast path" do
  # [native operations, GVL releases] made by this thread. An operation whose
  # result is ready at its first poll returns without releasing the GVL, so
  # only the operation count is exact.
  def counts = [native.operation_count, native.wait_count]
  def since(before) = counts.zip(before).map { |now, was| now - was }

  it "runs each call as one native operation that releases the GVL at most once, connecting or reusing" do
    transport = hyper(**tls_options)
    peer_client = client(peer["connect_tls"], transport: transport, protocol: :connect)
    calls = %w[cold warm].map do |value|
      before = counts
      expect(peer_client.unary(MSG.new(value: value), timeout: 5).message.value).to eq(value)
      since(before)
    end
    expect(calls).to all(match([1, be <= 1]))
  end

  it "keeps per-operation waits for gRPC unary, which core sends as a stream" do
    peer_client = client(peer["grpc_h2c"], transport: hyper)
    peer_client.unary(MSG.new(value: "warm"), timeout: 5)
    before = counts
    peer_client.unary(MSG.new(value: "x"), timeout: 5)
    # The first write connects and dispatches; head, body and finish follow,
    # each its own operation.
    operations, releases = since(before)
    expect(operations).to be > 1
    expect(releases).to be <= operations
  end
end
