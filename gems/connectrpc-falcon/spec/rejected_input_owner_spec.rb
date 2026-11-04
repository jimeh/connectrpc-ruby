# frozen_string_literal: true

RSpec.describe "Falcon rejected-input absolute time ownership" do
  [[nil, 4], ["62500u", 1], ["0n", 0]].each do |timeout, expected_reads|
    it "never renews the original #{timeout || '100ms tail'} deadline on progress or repeat close" do
      reactor do
        time = 0.0
        closed = false
        reads = 0
        body_closed = 0
        raw = double("raw TCP", closed?: false)
        allow(raw).to receive(:close) { closed = true }
        allow(raw).to receive(:closed?) { closed }
        body = double("framed HTTP/1 body", empty?: false)
        allow(body).to receive(:read) do
          reads += 1
          time += 0.03125
          "x"
        end
        allow(body).to receive(:close) do
          expect(closed).to be(true)
          body_closed += 1
        end
        allow(ConnectRPC::Deadline).to receive(:new).and_wrap_original do |method, duration|
          method.call(duration, clock: -> { time })
        end
        owner_type = ConnectRPC::Falcon.const_get(:HTTP1RejectedInput)
        connection = double(:connection, :rejected_input= => nil)
        owner = owner_type.new(body, raw: raw, connection: connection,
                                     headers: timeout ? { "grpc-timeout" => [timeout] } : {})
        allow(owner).to receive(:now) { time }
        # Processing used 3/64 second. The earlier valid call deadline must
        # retain that elapsed time; cleanup cannot parse a fresh relative one.
        time = 0.046875
        expect { owner.close }.to raise_error(Async::TimeoutError)
        expect(reads).to eq(expected_reads)
        owner.close
        expect(body_closed).to eq(1)
      end
    end
  end

  it "does not start a tail allowance while an earlier processor cancellation is unwinding" do
    reactor do
      raw = double("raw TCP", closed?: false)
      body = double("framed HTTP/1 body")
      connection = double(:connection, :rejected_input= => nil)
      owner = ConnectRPC::Falcon.const_get(:HTTP1RejectedInput).new(body, raw: raw, connection: connection, headers: {})
      expect(raw).to receive(:close).ordered
      expect(body).to receive(:close).ordered
      expect(body).not_to receive(:read)
      canceled = Async::Cancel.new
      expect { owner.close(canceled) }.to(raise_error { |error| expect(error).to equal(canceled) })
    end
  end
end
