# frozen_string_literal: true

RSpec.describe "Connect unary fast path" do
  def waits = native.wait_count

  it "releases the GVL once per call, connecting or reusing" do
    transport = hyper(**tls_options)
    peer_client = client(peer["connect_tls"], transport: transport, protocol: :connect)
    before = waits
    expect(peer_client.unary(MSG.new(value: "cold"), timeout: 5).message.value).to eq("cold")
    cold = waits - before
    before = waits
    expect(peer_client.unary(MSG.new(value: "warm"), timeout: 5).message.value).to eq("warm")
    expect([cold, waits - before]).to eq([1, 1])
  end

  it "keeps per-operation waits for gRPC unary, which core sends as a stream" do
    peer_client = client(peer["grpc_h2c"], transport: hyper)
    peer_client.unary(MSG.new(value: "warm"), timeout: 5)
    before = waits
    peer_client.unary(MSG.new(value: "x"), timeout: 5)
    # The first write connects and dispatches; head and body follow.
    expect(waits - before).to be_between(1, 5)
  end
end
