# frozen_string_literal: true

require_relative "support/server_keepalive_examples"

RSpec.describe ConnectRPC::Async::ServerKeepalive do
  it "defaults to grpc-go's tolerant server policy without an age limit" do
    keepalive = described_class.new
    expect([keepalive.time, keepalive.timeout, keepalive.min_time, keepalive.permit_without_calls,
            keepalive.max_connection_age, keepalive.max_connection_age_grace]).to eq([7200, 20, 5, true, nil, nil])
    expect(keepalive).to be_frozen
    expect(keepalive).to eq(described_class.new(time: 7200))
    expect(keepalive.hash).to eq(described_class.new.hash)
    expect(keepalive).not_to eq(described_class.new(min_time: nil))
    expect(described_class.new(time: nil, min_time: nil, max_connection_age: 1, max_connection_age_grace: 0)
      .max_connection_age_grace).to eq(0)
  end

  {
    { time: 0 } => /time must be finite positive/,
    { timeout: nil } => /timeout must be finite positive/,
    { min_time: -1 } => /min_time must be finite positive/,
    { permit_without_calls: nil } => /permit_without_calls must be true or false/,
    { max_connection_age: Float::INFINITY } => /max_connection_age must be finite positive/,
    { max_connection_age: 1, max_connection_age_grace: -1 } => /max_connection_age_grace must be finite nonnegative/,
    { max_connection_age_grace: 1 } => /max_connection_age_grace requires max_connection_age/
  }.each do |options, message|
    it "rejects #{options.inspect}" do
      expect { described_class.new(**options) }.to raise_error(ArgumentError, message)
    end
  end

  it "is the only keepalive a host or session accepts" do
    reactor do
      expect { ConnectRPC::Async::Host.new(server: server, keepalive: 300) }
        .to raise_error(ArgumentError, /keepalive must be a ServerKeepalive or nil/)
      expect { ConnectRPC::Async::ServerSession.new(application: ->(_) {}, keepalive: ConnectRPC::Async::Keepalive.new(time: 1)) }
        .to raise_error(ArgumentError, /keepalive must be a ServerKeepalive or nil/)
    end
  end
end

RSpec.describe "Async Host server keepalive" do
  def keepalive_host(**) = ConnectRPC::Async::Host.new(**).start
  def keepalive_session(host) = host_session(host)

  include_examples "HTTP/2 server keepalive"
end
