# frozen_string_literal: true

require "spec_helper"

RSpec.describe ConnectRPC::Keepalive do
  it "loads with core and builds a frozen comparable value with grpc-go's timeout default" do
    keepalive = described_class.new(time: 300)
    expect([keepalive.time, keepalive.timeout, keepalive.permit_without_calls]).to eq([300, 20, false])
    expect(keepalive).to be_frozen
    expect(keepalive).to eq(described_class.new(time: 300, timeout: 20, permit_without_calls: false))
    expect(keepalive).to eql(described_class.new(time: 300))
    expect(keepalive.hash).to eq(described_class.new(time: 300).hash)
    expect(keepalive).not_to eq(described_class.new(time: 300, permit_without_calls: true))
    expect(keepalive).not_to eq(described_class.new(time: 300, timeout: 1))
    expect(keepalive).not_to eq(described_class.new(time: 301))
    expect(keepalive).not_to eq(Object.new)
    expect(described_class.new(time: 0.5, timeout: 0.25, permit_without_calls: true).time).to eq(0.5)
  end

  [
    [{}, ArgumentError, /time/], [{ time: 0 }, ArgumentError, "time must be finite positive seconds"],
    [{ time: -1 }, ArgumentError, "time must be finite positive seconds"],
    [{ time: Float::INFINITY }, ArgumentError, /time/], [{ time: "1" }, ArgumentError, /time/],
    [{ time: Complex(1, 1) }, ArgumentError, /time/],
    [{ time: 1, timeout: 0 }, ArgumentError, "timeout must be finite positive seconds"],
    [{ time: 1, timeout: Float::NAN }, ArgumentError, /timeout/],
    [{ time: 1, permit_without_calls: nil }, ArgumentError, "permit_without_calls must be true or false"]
  ].each do |options, error, message|
    it "rejects #{options.inspect}" do
      expect { described_class.new(**options) }.to raise_error(error, message)
    end
  end
end
