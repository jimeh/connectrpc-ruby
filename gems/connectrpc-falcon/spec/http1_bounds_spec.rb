# frozen_string_literal: true

RSpec.describe "Connect H1 framed input retirement bounds" do
  let(:owner_type) { ConnectRPC::Falcon.const_get(:HTTP1StreamInput) }

  # The pinned Fixed and Chunked parsers are real; its connection boundary controls read sizes
  # and time without relying on TCP packetization or wall-clock scheduling.
  def persistence(owner) = owner.instance_variable_get(:@connection).persistence

  def fixture(framing, length: 1_000_000, fields: nil, **)
    events = []
    connection = Object.new
    connection.define_singleton_method(:readpartial) do |size|
      events << size
      "x" * size
    end
    connection.define_singleton_method(:close_read) { events << :close_read }
    connection.define_singleton_method(:receive_end_stream!) { nil }
    connection.define_singleton_method(:read_line) { "f4240" }
    framed = if framing == :fixed
               Protocol::HTTP1::Body::Fixed.new(connection, length)
             else
               Protocol::HTTP1::Body::Chunked.new(connection, Protocol::HTTP::Headers.new)
             end
    body = Struct.new(:body) do
      def read = body.read
      def close = body.close
    end.new(framed)
    raw = Object.new
    raw.define_singleton_method(:closed?) { false }
    raw.define_singleton_method(:close) { events << :raw_close }
    raw.define_singleton_method(:shutdown) { |_| nil }
    connection.define_singleton_method(:persistent=) { |value| (@persistence ||= []) << value }
    connection.define_singleton_method(:persistence) { @persistence || [] }
    connection.define_singleton_method(:close) { events << :connection_close }
    owner = owner_type.new(body, raw: raw, connection: connection, fields: fields, **)
    [owner, body, events]
  end

  it "reads a declared remainder below 256 KiB to completion before the head" do
    reactor do
      owner, _body, events = fixture(:fixed, length: 200_000)
      owner.head_resolved(200)
      owner.processor_finished
      owner.read_first
      expect(events).to eq(([65_536] * 3) + [3392])
      expect(owner.instance_variable_get(:@discarded)).to eq(200_000)
      owner.final_flushed
      owner.close(true)
      # A retired connection is never reused, even after complete framing.
      expect(events.last(2)).to eq(%i[raw_close connection_close])
    end
  end

  it "leaves a declared remainder of 256 KiB unread, as net/http does" do
    reactor do
      owner, _body, events = fixture(:fixed, length: 262_144)
      owner.head_resolved(200)
      owner.processor_finished
      owner.read_first
      expect(events).to be_empty
      owner.close(false)
      expect(events.grep(Integer)).to be_empty
    end
  end

  it "stops a chunked remainder at 256 KiB and aborts raw before close" do
    reactor do
      owner, _body, events = fixture(:chunked)
      owner.head_resolved(200)
      owner.processor_finished
      owner.read_first
      owner.read_first
      expect(events).to eq([65_536] * 4)
      expect(owner.instance_variable_get(:@discarded)).to eq(262_144)
      owner.final_flushed
      owner.close(true)
      expect(events).to eq(([65_536] * 4) + %i[raw_close connection_close close_read])
      owner.close(true)
      expect(events.size).to eq(7)
    end
  end

  it "releases an adapter remainder without counting it toward the limit" do
    reactor do
      owner, body, events = fixture(:chunked)
      input = AsyncHelpers::INTERNAL::Input.new(body)
      expect(input.read(5)).to eq("xxxxx")
      owner.input = input
      owner.head_resolved(200)
      owner.processor_finished
      expect(input.release_remainder).to eq(0)
      owner.read_first
      expect(events.grep(Integer)).to eq([65_536] * 5) # One processor read, four remainder reads.
      expect(owner.instance_variable_get(:@discarded)).to eq(262_144)
      owner.close(false)
    end
  end

  it "bounds the whole read by the Connect deadline despite repeated progress" do
    reactor do
      # The eligibility check, then one reading per read wait.
      allow_any_instance_of(owner_type).to receive(:now).and_return(10.0, 10.01, 10.06, 10.101)
      owner, _body, events = fixture(:chunked, expires: 10.1)
      owner.head_resolved(200)
      owner.processor_finished
      owner.read_first
      expect(events.grep(Integer)).to eq([65_536] * 2)
      owner.close(false)
    end
  end

  it "takes the read deadline from one valid connect-timeout-ms" do
    allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC).and_return(10.0)
    expect(owner_type.connect_expires([%w[connect-timeout-ms 100]])).to eq(10.1)
    [nil, [%w[connect-timeout-ms 0]], [%w[connect-timeout-ms 1x]],
     [%w[connect-timeout-ms 1], %w[connect-timeout-ms 2]]].each do |fields|
      expect(owner_type.connect_expires(fields)).to be_nil
    end
  end

  # net/http keeps a connection whose unread body ends within the limit.
  it "keeps a reusable connection when the read reaches EOF within the limit" do
    reactor do
      owner, _body, events = fixture(:fixed, length: 200_000, reuse: true)
      owner.head_resolved(200)
      owner.processor_finished
      owner.read_first
      expect(events.grep(Integer).sum).to eq(200_000)
      expect(owner).not_to be_retiring
      owner.final_flushed
      owner.close(true)
      expect(persistence(owner)).to be_empty
      expect(events).not_to include(:raw_close, :connection_close)
    end
  end

  it "closes a reusable connection after the reply when more remains" do
    reactor do
      [[:chunked, {}], [:fixed, { length: 262_144 }]].each do |framing, options|
        owner, _body, events = fixture(framing, reuse: true, **options)
        owner.head_resolved(200)
        owner.processor_finished
        # The connection header is decided after the read, before the head.
        expect(persistence(owner)).to be_empty
        owner.read_first
        expect(events.grep(Integer).sum).to eq(framing == :fixed ? 0 : 262_144)
        expect(persistence(owner)).to eq([false])
        expect(owner).to be_retiring
        owner.close(true)
        expect(events).to include(:raw_close, :connection_close)
      end
    end
  end

  it "skips the read for a 505 and an unanswered Expect: 100-continue" do
    reactor do
      [[505, nil, false], [200, [%w[Expect 100-continue]], false],
       [200, [%w[Expect 100-continue]], true]].each do |status, fields, reuse|
        owner, _body, events = fixture(:chunked, fields: fields, reuse: reuse)
        owner.head_resolved(status)
        expect(persistence(owner)).to eq([false])
        owner.processor_finished
        owner.read_first
        expect(events).to be_empty
        owner.close(false)
      end
    end
  end

  it "joins a timed Input read child before transferring its remainder" do
    reactor do |task|
      started = Async::Queue.new
      stopped = Async::Queue.new
      body = Object.new
      body.define_singleton_method(:read) do
        started.enqueue(Async::Task.current)
        begin
          Async::Condition.new.wait
        ensure
          stopped.enqueue(Async::Task.current)
        end
      end
      input = AsyncHelpers::INTERNAL::Input.new(body)
      reading = task.async do
        input.read(5, timeout: 0.02)
      rescue Async::TimeoutError
        :expired
      end
      child = started.dequeue
      expect(reading.wait).to eq(:expired)
      expect(stopped.dequeue).to equal(child)
      expect(child).to be_finished
      expect(input.release_remainder).to eq(0)
    end
  end
end
