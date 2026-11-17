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

  it "validates the per-connection stream limit and request and response body timeouts" do
    reactor do
      session = ->(**options) { ConnectRPC::Async::ServerSession.new(application: ->(_) {}, **options) }
      [0, -1, 1.5, "2", nil, 0x1_0000_0000].each do |value|
        expect { session.call(max_concurrent_streams: value) }
          .to raise_error(ArgumentError, "max_concurrent_streams must be an integer from 1 to 4294967295")
      end
      %w[request_body_timeout response_body_timeout].each do |option|
        [0, -1, Float::INFINITY, "60"].each do |value|
          expect { session.call(option.to_sym => value) }
            .to raise_error(ArgumentError, "#{option} must be finite positive seconds")
        end
      end
      [session.call(max_concurrent_streams: 1), session.call(max_concurrent_streams: 0xffff_ffff),
       session.call(request_body_timeout: nil), session.call(request_body_timeout: 0.5),
       session.call(response_body_timeout: nil), session.call(response_body_timeout: 0.5)].each(&:abort)
      expect { ConnectRPC::Async::Host.new(server: server, max_concurrent_streams: 0) }
        .to raise_error(ArgumentError, /max_concurrent_streams/)
      expect { ConnectRPC::Async::Host.new(server: server, response_body_timeout: 0) }
        .to raise_error(ArgumentError, /response_body_timeout/)
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
