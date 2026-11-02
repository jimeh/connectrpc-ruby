# frozen_string_literal: true

RSpec.describe "connectrpc" do
  it "exposes the initial semantic version" do
    expect(ConnectRPC::VERSION).to eq("0.1.0")
    expect(ConnectRPC::VERSION).to match(/\A\d+\.\d+\.\d+\z/)
  end
end
