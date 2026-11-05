# frozen_string_literal: true

RSpec.describe "Connect H1 final flush ownership" do
  # Exercise the pinned writer itself with a controlled output boundary and clock.
  # Body callbacks run after the final flush and must spend, not restart, grace.
  def writer_fixture(length: nil, head: false, version: "HTTP/1.1", absent: false, callback_delay: 0.7)
    clock = [10.0]
    events = []
    stream = Object.new
    stream.define_singleton_method(:write) { |bytes| events << [:write, bytes] }
    stream.define_singleton_method(:flush) { events << [:flush, clock.first] }
    stream.define_singleton_method(:close_write) { events << :close_write }
    stream.define_singleton_method(:close) { events << :stream_close }
    raw = Object.new
    raw.define_singleton_method(:closed?) { false }
    raw.define_singleton_method(:shutdown) { |how| events << [:shutdown, how] }
    raw.define_singleton_method(:close) { events << :raw_close }
    connection = ConnectRPC::Falcon.const_get(:HTTP1Connection).new(stream, version, state: :open)
    owner = ConnectRPC::Falcon.const_get(:HTTP1StreamInput).new(nil, raw: raw, connection: connection)
    allow(owner).to receive(:now) { clock.first }
    owner.processor_finished
    owner.head_resolved(505)
    resource = Struct.new(:input_retirement, :exchange, :active).new(owner)
    resource.define_singleton_method(:changed) { nil }
    resource.define_singleton_method(:failed!) { nil }
    connection.resource = resource
    chunks = length&.zero? ? [] : ["0\r\n\r\n", "later"]
    body = Object.new
    body.define_singleton_method(:length) { length }
    body.define_singleton_method(:empty?) { length&.zero? }
    body.define_singleton_method(:ready?) { false }
    body.define_singleton_method(:read) do
      # At the next read the terminal-looking application payload has flushed.
      events << [:before_read, owner.instance_variable_get(:@close_expires)]
      clock[0] += 0.01
      chunks.shift
    end
    body.define_singleton_method(:close) do |*|
      events << [:callback, owner.instance_variable_get(:@close_expires), clock.first]
      clock[0] += callback_delay
    end
    [connection, owner, absent ? nil : body, events, clock, version, head]
  end

  [
    { length: nil }, { length: 10 }, { length: 0 }, { absent: true },
    { length: 10, head: true }, { head: true }, { version: "HTTP/1.0" }
  ].each do |options|
    it "anchors the pinned writer's final flush before callbacks for #{options}" do
      reactor do
        connection, owner, body, events, clock, version, head = writer_fixture(**options)
        # Leave retirement until after inspecting the actual writer boundary.
        allow(owner).to receive(:close)
        connection.write_body(version, body, head)
        flush_at = events.select { |event| event.is_a?(Array) && event.first == :flush }.last[1]
        expect(owner.instance_variable_get(:@close_expires)).to eq(flush_at + 0.5)
        callbacks = events.select { |event| event.is_a?(Array) && event.first == :callback }
        callbacks.each { |event| expect(event[1]).to eq(event[2] + 0.5) }
        reads = events.select { |event| event.is_a?(Array) && event.first == :before_read }
        expect(reads.map(&:last)).to all(be_nil)
        next unless body

        expect(clock.first).to be > flush_at + 0.5
        expect(owner).not_to receive(:discard)
        expect(Async::Task.current).not_to receive(:sleep)
        allow(owner).to receive(:close).and_call_original
        owner.close(true)
        expect(events).to include([:shutdown, Socket::SHUT_WR], :raw_close)
      end
    end
  end

  it "never starts success grace after a failed final flush" do
    reactor do
      connection, owner, body, events, = writer_fixture
      eof = false
      allow(body).to receive(:read).and_wrap_original do |read|
        read.call.tap { |bytes| eof = bytes.nil? }
      end
      allow(connection.stream).to receive(:flush).and_wrap_original do |flush|
        raise IOError, "failed flush" if eof

        flush.call
      end
      expect { connection.write_body("HTTP/1.1", body) }.to raise_error(IOError, "failed flush")
      expect(owner.instance_variable_get(:@close_expires)).to be_nil
      expect(events).not_to include([:shutdown, Socket::SHUT_WR])
      expect(events).to include(:raw_close)
    end
  end

  it "spends delayed callback and discard time within one total 500 ms budget" do
    reactor do |task|
      connection, owner, body, events, clock, = writer_fixture(callback_delay: 0.2)
      allow(owner).to receive(:close)
      connection.write_body("HTTP/1.1", body)
      expires = owner.instance_variable_get(:@close_expires)
      allow(owner).to receive(:close).and_call_original
      expect(owner).to receive(:discard) { clock[0] += 0.1 }
      expect(task).to receive(:sleep) do |remaining|
        expect(remaining).to be_within(0.00001).of(0.2)
        clock[0] += remaining
      end
      owner.close(true)
      expect(clock.first).to eq(expires)
      expect(events).to include([:shutdown, Socket::SHUT_WR], :raw_close)
    end
  end
end
