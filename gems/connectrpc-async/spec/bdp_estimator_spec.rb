# frozen_string_literal: true

RSpec.describe "HTTP/2 BDP estimator" do
  let(:clock) { Struct.new(:now).new(0.0) }
  let(:ping) { AsyncHelpers::INTERNAL::BDPEstimator::PING }

  def estimator(window: 65_536, limit: 16_777_216)
    AsyncHelpers::INTERNAL::BDPEstimator.new(window: window, limit: limit, clock: clock)
  end

  # One sample of `bytes` whose PING's ACK arrives `rtt` seconds after it was written.
  def sample(bdp, bytes, rtt: 0.05)
    expect(bdp.starts_sample?(bytes)).to be(true)
    bdp.sent
    clock.now += rtt
    bdp.acknowledged(ping)
  end

  it "uses grpc-go's BDP PING payload" do
    expect(ping.bytes).to eq([2, 4, 16, 16, 9, 14, 7, 7])
  end

  it "starts one sample per round trip and grows to twice a sample that fills two thirds of the estimate" do
    bdp = estimator
    expect(bdp.starts_sample?(16_384)).to be(true)
    3.times { expect(bdp.starts_sample?(16_384)).to be(false) }
    bdp.sent
    clock.now += 0.05
    # 64 KiB arrived in the round trip: at least 0.66 of the 64 KiB estimate.
    expect(bdp.acknowledged(ping)).to eq(131_072)
    expect(bdp.estimate).to eq(131_072)
  end

  it "does not grow on a sample below two thirds of the estimate" do
    bdp = estimator
    expect(sample(bdp, 43_000)).to be_nil
    expect(bdp.estimate).to eq(65_536)
    expect(sample(bdp, 43_254)).to eq(86_508)
  end

  it "grows only on a sample with the highest bandwidth seen" do
    bdp = estimator
    expect(sample(bdp, 65_536, rtt: 0.01)).to eq(131_072)
    # Large enough, but the slower round trip lowers the bandwidth below the first sample's.
    expect(sample(bdp, 100_000, rtt: 1.0)).to be_nil
    expect(bdp.estimate).to eq(131_072)
  end

  it "caps growth at the limit, then starts no more samples" do
    bdp = estimator(limit: 100_000)
    expect(sample(bdp, 65_536)).to eq(100_000)
    expect(bdp.starts_sample?(65_536)).to be(false)
    expect(bdp.acknowledged(ping)).to be_nil
  end

  it "counts an ACK only after its PING was written, and only once" do
    bdp = estimator
    expect(bdp.acknowledged(ping)).to be_nil
    expect(bdp.starts_sample?(65_536)).to be(true)
    # Unwritten: an early ACK neither ends nor corrupts the sample.
    expect(bdp.acknowledged(ping)).to be_nil
    expect(bdp.starts_sample?(1)).to be(false)
    bdp.sent
    clock.now += 0.05
    expect(bdp.acknowledged("unrelatd")).to be_nil
    expect(bdp.acknowledged(ping)).to eq(131_074)
    expect(bdp.acknowledged(ping)).to be_nil
    expect(bdp.starts_sample?(1)).to be(true)
  end

  it "lets the next DATA start a new sample after an abandoned one" do
    bdp = estimator
    expect(bdp.starts_sample?(65_536)).to be(true)
    bdp.abandon
    bdp.sent
    expect(bdp.acknowledged(ping)).to be_nil
    expect(sample(bdp, 65_536, rtt: 0.04)).to eq(131_072)
    # The abandoned sample does not count toward the RTT average.
    expect(bdp.rtt).to be_within(1e-9).of(0.04)
  end

  it "averages the first nine RTT samples, then weights each new one by 0.9" do
    bdp = estimator(limit: 65_537)
    rtts = Array.new(9) { |index| 0.01 * (index + 1) }
    rtts.each { |rtt| sample(bdp, 1, rtt: rtt) }
    expect(bdp.rtt).to be_within(1e-9).of(rtts.sum / 9)
    before = bdp.rtt
    sample(bdp, 1, rtt: 1.0)
    expect(bdp.rtt).to be_within(1e-9).of(before + ((1.0 - before) * 0.9))
  end
end
