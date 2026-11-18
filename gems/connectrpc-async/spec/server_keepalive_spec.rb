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

  it "validates the per-connection stream limit, body timeouts and frame write timeout" do
    reactor do
      session = ->(**options) { ConnectRPC::Async::ServerSession.new(application: ->(_) {}, **options) }
      [0, -1, 1.5, "2", nil, 0x1_0000_0000].each do |value|
        expect { session.call(max_concurrent_streams: value) }
          .to raise_error(ArgumentError, "max_concurrent_streams must be an integer from 1 to 4294967295")
      end
      %w[request_body_timeout response_body_timeout frame_write_timeout].each do |option|
        [0, -1, Float::INFINITY, "60"].each do |value|
          expect { session.call(option.to_sym => value) }
            .to raise_error(ArgumentError, "#{option} must be finite positive seconds")
        end
      end
      [session.call(max_concurrent_streams: 1), session.call(max_concurrent_streams: 0xffff_ffff),
       session.call(request_body_timeout: nil), session.call(request_body_timeout: 0.5),
       session.call(response_body_timeout: nil), session.call(response_body_timeout: 0.5),
       session.call(frame_write_timeout: nil), session.call(frame_write_timeout: 0.5)].each(&:abort)
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

RSpec.describe "HTTP/2 frame write watchdog" do
  let(:watchdog) { AsyncHelpers::INTERNAL::WriteWatchdog }
  let(:clock) { Struct.new(:now).new(0.0) }

  # A struct tcp_info with tcpi_bytes_acked and tcpi_notsent_bytes, or one
  # cut short as from a kernel older than 4.6.
  def tcp_info(acked, unsent, size: 248)
    data = "\0".b * 248
    data[120, 8] = [acked].pack("Q")
    data[144, 4] = [unsent].pack("L")
    Socket::Option.new(:INET, :TCP, 11, data.byteslice(0, size))
  end

  # The socket returns each sample once; one more read raises.
  def resource(samples)
    raw = Object.new
    raw.define_singleton_method(:getsockopt) { |*| samples.shift.then { |sample| sample || raise("no sample left") } }
    double("resource", raw: raw)
  end

  # A control frame's check can reach an overdue write before the watchdog
  # task does. The resource double has no `abort`, so any abort raises.
  it "renews an overdue write when the peer acknowledged data while unsent data was queued" do
    stub_const("#{watchdog.name}::TCP_INFO", 11)
    samples = [tcp_info(1000, 10), tcp_info(1000, 10), tcp_info(2000, 0), tcp_info(3000, 10, size: 144),
               tcp_info(3000, 10)]
    writes = watchdog.new(resource(samples), clock: clock, timeout: 60, progress: true)
    writes.write_started
    clock.now = 60.0
    # The baseline, no new acks, acks without unsent data, then a short struct.
    4.times { expect(writes.remaining).to eq(0) }
    expect(writes.remaining).to eq(60)
    clock.now = 90.0
    expect(writes.remaining).to eq(30)
    expect(samples).to eq([])
    writes.write_finished
    expect(writes.remaining).to be_nil
  end

  it "keeps a fixed deadline without progress" do
    stub_const("#{watchdog.name}::TCP_INFO", 11)
    writes = watchdog.new(resource([]), clock: clock, timeout: 60)
    writes.write_started
    clock.now = 61.0
    expect(writes.remaining).to eq(-1)
  end

  # A hosting SPI socket need not offer getsockopt.
  it "keeps a fixed deadline on a socket without getsockopt" do
    stub_const("#{watchdog.name}::TCP_INFO", 11)
    writes = watchdog.new(double("resource", raw: Object.new), clock: clock, timeout: 60, progress: true)
    writes.write_started
    clock.now = 61.0
    expect(writes.remaining).to eq(-1)
  end

  # Write A finishes at once and write B blocks at 0.5, while the watchdog
  # still sleeps toward A's deadline. It first sees B at 60, and the peer
  # acknowledged data since A's sample, possibly during B, so B gets a whole
  # bound from then rather than failing at 60.5.
  it "gives a write it sees late a whole bound when the peer progressed meanwhile" do
    stub_const("#{watchdog.name}::TCP_INFO", 11)
    reactor do |task|
      fake = ConnectionPeer::FakeClock.new
      aborted = []
      host = double("resource", raw: resource([tcp_info(1000, 10), tcp_info(2000, 10), tcp_info(2000, 10)]).raw)
      allow(host).to receive(:maintain)
      allow(host).to receive(:abort) { |error| aborted << error }
      writes = watchdog.new(host, clock: fake, timeout: 60, progress: true)
      due = ->(at) { fake.sleepers.any? { |sleeper| (sleeper.due - at).abs < 1e-9 } }
      writes.start(task)
      writes.write_started
      wait_until(task, "A's deadline") { due.call(60) }
      writes.write_finished
      fake.advance(0.5)
      writes.write_started
      fake.advance(59.5)
      wait_until(task, "B's deadline from its first sample; pending #{fake.sleepers.map(&:due)}") { due.call(120) }
      fake.advance(59.9)
      expect(aborted).to eq([])
      fake.advance(0.1)
      wait_until(task, "abort without progress since 60") { aborted.any? }
    end
  end
end

RSpec.describe "Async Host server keepalive" do
  def keepalive_host(**) = ConnectRPC::Async::Host.new(**).start
  def keepalive_session(host) = host_session(host)

  include_examples "HTTP/2 server keepalive"
end
