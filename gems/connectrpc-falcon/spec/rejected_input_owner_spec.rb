# frozen_string_literal: true

RSpec.describe "Falcon rejected-input absolute time ownership" do
  let(:owner_type) { ConnectRPC::Falcon.const_get(:HTTP1RejectedInput) }

  # A real chunked parser over a connection boundary whose every 64 KiB read
  # advances the clock by 1/32 second.
  def fixture(clock)
    reads = []
    connection = Object.new
    connection.define_singleton_method(:readpartial) do |size|
      reads << size
      clock[:time] += 0.03125
      "x" * size
    end
    connection.define_singleton_method(:read_line) { "f4240" }
    connection.define_singleton_method(:persistent=) { |value| (@persistence ||= []) << value }
    connection.define_singleton_method(:persistence) { @persistence || [] }
    framed = Protocol::HTTP1::Body::Chunked.new(connection, Protocol::HTTP::Headers.new)
    body = Struct.new(:body, :closes) do
      def read = body.read
      def close = self.closes += 1
    end.new(framed, 0)
    resource = Object.new
    resource.define_singleton_method(:raw_failed?) { false }
    resource.singleton_class.attr_accessor(:input_retirement)
    raw = double("raw TCP", closed?: false)
    [connection, body, resource, raw, reads]
  end

  [[nil, 4], ["62500u", 1], ["0n", 0]].each do |timeout, expected_reads|
    it "never renews the original #{timeout || 'absent'} deadline on progress or repeat close" do
      reactor do
        clock = { time: 0.0 }
        connection, body, resource, raw, reads = fixture(clock)
        allow(ConnectRPC::Deadline).to receive(:new).and_wrap_original do |method, duration|
          method.call(duration, clock: -> { clock[:time] })
        end
        allow_any_instance_of(ConnectRPC::Falcon.const_get(:HTTP1StreamInput)).to receive(:now) { clock[:time] }
        owner = owner_type.new(body, raw: raw, connection: connection, resource: resource,
                                     headers: timeout ? { "grpc-timeout" => [timeout] } : {})
        allow(owner).to receive(:now) { clock[:time] }
        # Processing used 3/64 second. The earlier valid call deadline must
        # retain that elapsed time; cleanup cannot parse a fresh relative one.
        clock[:time] = 0.046875
        owner.close
        # Without a deadline the read stops at net/http's 256 KiB limit.
        expect(reads).to eq([65_536] * expected_reads)
        # The unfinished remainder retires the connection after the reply,
        # whose owner then closes the body.
        expect(connection.persistence).to eq([false])
        expect(resource.input_retirement).to be_retiring
        expect(body.closes).to eq(0)
        owner.close
        expect(reads.size).to eq(expected_reads)
        expect(body.closes).to eq(0)
      end
    end
  end

  it "keeps the connection and closes the body when the remainder finishes" do
    reactor do
      clock = { time: 0.0 }
      connection, body, resource, raw, reads = fixture(clock)
      lines = ["10000", "0", ""]
      connection.define_singleton_method(:read_line) { lines.shift }
      connection.define_singleton_method(:read) { |_| "\r\n" }
      connection.define_singleton_method(:receive_end_stream!) { nil }
      owner = owner_type.new(body, raw: raw, connection: connection, resource: resource, headers: {})
      owner.close
      expect(reads).to eq([65_536])
      expect(connection.persistence).to be_empty
      expect(resource.input_retirement).to be_nil
      expect(body.closes).to eq(1)
    end
  end

  it "does not start a tail read while an earlier processor cancellation is unwinding" do
    reactor do
      raw = double("raw TCP", closed?: false)
      body = double("framed HTTP/1 body")
      owner = owner_type.new(body, raw: raw, connection: double(:connection), headers: {})
      expect(raw).to receive(:close).ordered
      expect(body).to receive(:close).ordered
      expect(body).not_to receive(:read)
      canceled = Async::Cancel.new
      expect { owner.close(canceled) }.to(raise_error { |error| expect(error).to equal(canceled) })
    end
  end
end
