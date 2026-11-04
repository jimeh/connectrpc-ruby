# frozen_string_literal: true

RSpec.describe "Connect H1 framed input retirement bounds" do
  let(:owner_type) { ConnectRPC::Falcon.const_get(:HTTP1StreamInput) }

  # The pinned Fixed and Chunked parsers are real; its connection boundary controls read sizes
  # and time without relying on TCP packetization or wall-clock scheduling.
  def fixture(framing)
    events = []
    connection = Object.new
    connection.define_singleton_method(:readpartial) do |length|
      events << length
      "x" * length
    end
    connection.define_singleton_method(:close_read) { events << :close_read }
    connection.define_singleton_method(:read_line) { "f4240" }
    framed = if framing == :fixed
               Protocol::HTTP1::Body::Fixed.new(connection, 1_000_000)
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
    connection.define_singleton_method(:persistent=) { |_| nil }
    connection.define_singleton_method(:retiring_raw=) { |_| nil }
    connection.define_singleton_method(:close) { events << :connection_close }
    owner = owner_type.new(body, raw: raw, connection: connection)
    [owner, body, events]
  end

  %i[fixed chunked].each do |framing|
    it "reserves exactly four complete 64 KiB reads and aborts raw before #{framing} close" do
      reactor do
        owner, _body, events = fixture(framing)
        owner.head_resolved(200)
        owner.processor_finished
        owner.final_flushed
        owner.close(true)
        expect(events).to eq(([65_536] * 4) + %i[raw_close connection_close close_read])
        expect(owner.instance_variable_get(:@discarded)).to eq(262_144)
        owner.close(true)
        expect(events.size).to eq(7)
      end
    end

    it "counts an adapter remainder once and reserves a full block before further #{framing} reads" do
      reactor do
        owner, body, events = fixture(framing)
        input = AsyncHelpers::INTERNAL::Input.new(body)
        expect(input.read(5)).to eq("xxxxx")
        owner.input = input
        owner.head_resolved(200)
        owner.processor_finished
        expect(input.release_remainder).to eq(0)
        owner.final_flushed
        owner.close(true)
        expect(events.grep(Integer)).to eq([65_536] * 4) # One processor read, three hygiene reads.
        expect(owner.instance_variable_get(:@discarded)).to eq(65_531 + (3 * 65_536))
      end
    end

    it "uses one absolute allowance despite repeated successful #{framing} progress" do
      reactor do
        owner, _body, events = fixture(framing)
        allow(owner).to receive(:now).and_return(10.0, 10.01, 10.06, 10.101)
        owner.head_resolved(200)
        owner.processor_finished
        # Control only the discard clock, independently of final-close grace.
        owner.send(:discard)
        owner.close(false)
        expect(events.grep(Integer)).to eq([65_536] * 2)
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
