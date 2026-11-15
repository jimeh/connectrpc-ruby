# frozen_string_literal: true

RSpec.describe "connectrpc" do
  it "exposes the packaged semantic version" do
    expect(ConnectRPC::VERSION).to eq(Gem.loaded_specs.fetch("connectrpc").version.to_s)
    expect(ConnectRPC::VERSION).to match(/\A\d+\.\d+\.\d+\z/)
  end
end
