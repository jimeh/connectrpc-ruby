# frozen_string_literal: true

RSpec.describe "HTTP/2 incremental response bodies" do
  it "expires a stalled body independently and bounds an unknown-length body before decode" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      listener = bound.sockets.first
      started = Async::Queue.new
      bodies = []
      peer = task.async do
        raw, = listener.accept
        connection = Async::HTTP::Protocol::HTTP2.server(raw)
        while (incoming = connection.requests.dequeue)
          if incoming.path == "/body"
            body = Protocol::HTTP::Body::Writable.new
            bodies << body
            incoming.send_response(Protocol::HTTP::Response[200, {}, body])
            body.write("prefix")
            started.enqueue(true)
          elsif incoming.path == "/excess"
            body = Protocol::HTTP::Body::Writable.new
            bodies << body
            incoming.send_response(Protocol::HTTP::Response[200, {}, body])
            body.write("x" * 20_000)
            body.close_write
          else
            incoming.send_response(Protocol::HTTP::Response[200, {}, ["healthy"]])
          end
          incoming.body&.close
        end
      ensure
        connection&.close
        AsyncHelpers::INTERNAL.raw_close(raw)
      end
      origin = "http://127.0.0.1:#{listener.local_address.ip_port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      stalled = task.async(finished: false) do
        transport.call(request("#{origin}/body", deadline: ConnectRPC::Deadline.new(0.03)))
      end
      started.dequeue
      expect { stalled.wait }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("deadline_exceeded") }
      expect { transport.call(request("#{origin}/excess", limit: 10)) }.to raise_error(ConnectRPC::Error) { |e|
        expect(e.code).to eq("resource_exhausted")
      }
      expect(transport.call(request("#{origin}/healthy")).body).to eq("healthy")
      expect(transport.instance_variable_get(:@resources).size).to eq(1)
      expect(transport.instance_variable_get(:@pool).resources.values.sum).to eq(0)
    ensure
      transport&.close(grace: 0)
      bodies&.each(&:close)
      peer&.cancel
      peer&.wait
      bound&.close
    end
  end
end

RSpec.describe "Explicit HTTP/2 body cancellation" do
  it "cancels a real partially received body with no deadline and reuses the healthy pooled connection" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      listener = bound.sockets.first
      started = Async::Queue.new
      bodies = []
      peer = task.async do
        raw, = listener.accept
        connection = Async::HTTP::Protocol::HTTP2.server(raw)
        while (incoming = connection.requests.dequeue)
          if incoming.path == "/body"
            body = Protocol::HTTP::Body::Writable.new
            bodies << body
            incoming.send_response(Protocol::HTTP::Response[200, {}, body])
            body.write("prefix")
            started.enqueue(true)
          else
            incoming.send_response(Protocol::HTTP::Response[200, {}, ["healthy"]])
          end
          incoming.body&.close
        end
      ensure
        connection&.close
        AsyncHelpers::INTERNAL.raw_close(raw)
      end
      origin = "http://127.0.0.1:#{listener.local_address.ip_port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      source = ConnectRPC::CancellationSource.new
      outgoing = request("#{origin}/body")
      outgoing.cancellation = source.token
      pending = task.async(finished: false) { transport.call(outgoing) }
      started.dequeue
      Thread.new { source.cancel }.value
      expect { pending.wait }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("canceled") }
      expect(transport.call(request("#{origin}/healthy")).body).to eq("healthy")
      expect(transport.instance_variable_get(:@resources).size).to eq(1)
      expect(transport.instance_variable_get(:@pool).resources.values.sum).to eq(0)
    ensure
      transport&.close(grace: 0)
      bodies&.each(&:close)
      peer&.cancel
      peer&.wait
      bound&.close
    end
  end
end
