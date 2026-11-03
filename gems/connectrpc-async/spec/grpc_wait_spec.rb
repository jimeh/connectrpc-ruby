# frozen_string_literal: true

RSpec.describe "gRPC operation wait ceilings" do
  it "caps a nil-deadline transport operation and maps injected operation expiry to unavailable" do
    reactor do
      transport = ConnectRPC::Async::Transport.new(origin: "http://127.0.0.1:1")
      outgoing = ConnectRPC::StreamRequest.new(url: URI("http://127.0.0.1:1/test.GRPC/Call"), http_method: "POST",
                                               headers: {})
      operation = { request: outgoing, tasks: {}, grpc: true }
      expires_at = nil
      before = AsyncHelpers::INTERNAL.now
      allow(transport).to receive(:wait_event) do |_, expires|
        expires_at = expires
        raise Async::TimeoutError
      end
      expect do
        transport.send(:stream_operation, operation) { nil }
      end.to raise_error(ConnectRPC::Error) { |e|
        expect(e.code).to eq("unavailable")
        expect(e.cause).to be_nil
      }
      expect(expires_at - before).to be_within(0.1).of(9_999_999.999)
    ensure
      transport&.close(grace: 0)
    end
  end

  it "maps actual bounded server input and queued-output waits to unavailable with no logical deadline" do
    reactor do
      body = Protocol::HTTP::Body::Writable.new
      input = AsyncHelpers::INTERNAL::GRPCInput.new(body)
      expect { input.read(1, timeout: 0.001) }.to raise_error(ConnectRPC::Error) { |e|
        expect(e.code).to eq("unavailable")
        expect(e.cause).to be_nil
      }
      output_body = AsyncHelpers::INTERNAL::OutputBody.new
      output = AsyncHelpers::INTERNAL::GRPCServerOutput.new(output_body, nil)
      output.write("queued")
      expect(output_body.queued_bytes).to eq(6)
      expect { output.write("blocked", timeout: 0.001) }.to raise_error(ConnectRPC::Error) { |e|
        expect(e.code).to eq("unavailable")
        expect(e.cause).to be_nil
      }
    ensure
      body&.close
      output_body&.close
    end
  end
end
