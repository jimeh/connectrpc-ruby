# frozen_string_literal: true

RSpec.describe "HTTP/2 call admission by count and request bytes" do
  def pending_bytes(transport) = connection_manager(transport).pending_bytes

  def stalled_peer(task)
    bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
    gate = Async::Queue.new
    peer = ConnectionPeer::RecordingServer.new(task, bound.sockets.first, gate: gate)
    [bound, gate, peer, "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"]
  end

  it "rejects POST bodies, GET targets and full counts during an outage and reports each rejection" do
    reactor do |task|
      bound, gate, peer, origin = stalled_peer(task)
      events = []
      policy = ConnectRPC::Async::ConnectionPolicy.new(max_pending_bytes: 100)
      transport = recording_transport(origin, events, max_requests: 3, connection_policy: policy)
      admit = lambda do |outgoing|
        task.async(finished: false) { transport.call(outgoing) }
      end
      reject = lambda do |outgoing|
        expect { transport.call(outgoing) }.to unavailable_error
      end
      reject.call(request("#{origin}/oversized", body: "x" * 101))
      waiting = [admit.call(request("#{origin}/post", body: "x" * 60))]
      reject.call(request("#{origin}/post", body: "x" * 41))
      get = request("#{origin}/get?m=#{'x' * 30}", method: "GET")
      expect(get.url.request_uri.bytesize).to eq(37)
      waiting << admit.call(get)
      reject.call(request("#{origin}/abcd", method: "GET"))
      waiting << admit.call(request("#{origin}/post", body: "xyz"))
      expect(pending_bytes(transport)).to eq(100)
      reject.call(request("#{origin}/empty"))
      rejected = events.select { |event| event.type == :admission_rejected }
      expect(rejected.map { |event| [event.pending_calls, event.pending_bytes, event.state] }).to eq(
        [[0, 0, :idle], [1, 60, :connecting], [2, 97, :connecting], [3, 100, :connecting]]
      )
      expect(peer.accepted).to eq(1)
      expect(transport.close(grace: 0)).to be(false)
      waiting.each { |call| expect { call.wait }.to unavailable_error }
      expect(pending_bytes(transport)).to eq(0)
    ensure
      gate&.enqueue(true)
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  it "admits by count only without a byte budget and reserves no bytes for streams" do
    reactor do |task|
      bound, gate, peer, origin = stalled_peer(task)
      policy = ConnectRPC::Async::ConnectionPolicy.new(max_pending_bytes: nil)
      transport = ConnectRPC::Async::Transport.new(origin: origin, max_requests: 2, connection_policy: policy)
      large_body = "x" * ((16 * 1024 * 1024) + 1)
      large = task.async(finished: false) { transport.call(request("#{origin}/large", body: large_body)) }
      stream = task.async(finished: false) do
        transport.open_stream(ConnectRPC::StreamRequest.new(url: URI("#{origin}/stream"), http_method: "POST",
                                                            headers: {})) { nil }
      end
      wait_until(task, "both admitted") { transport.instance_variable_get(:@acquiring).size == 2 }
      expect(pending_bytes(transport)).to eq(large_body.bytesize)
      expect { transport.call(request("#{origin}/full")) }.to unavailable_error
      expect(transport.close(grace: 0)).to be(false)
      [large, stream].each { |call| expect { call.wait }.to unavailable_error }
      expect(pending_bytes(transport)).to eq(0)
    ensure
      gate&.enqueue(true)
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  it "reclaims bytes after success, cancellation, deadline, setup failure and reroute, reserving once" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      release = Async::Queue.new
      entered = Async::Queue.new
      peer = ConnectionPeer::RecordingServer.new(task, bound.sockets.first) do |incoming|
        if incoming.path == "/held"
          entered.enqueue(true)
          release.dequeue
        end
        incoming.send_response(Protocol::HTTP::Response[200, {}, ["ok"]]) unless incoming.stream.closed?
      end
      origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      expect(transport.call(request("#{origin}/ok", body: "abc")).body).to eq("ok")
      expect(pending_bytes(transport)).to eq(0)

      source = ConnectRPC::CancellationSource.new
      cancelled = request("#{origin}/held", body: "abcd")
      cancelled.cancellation = source.token
      call = task.async(finished: false) { transport.call(cancelled) }
      entered.dequeue
      expect(pending_bytes(transport)).to eq(4)
      source.cancel
      expect { call.wait }.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("canceled") }
      expect(pending_bytes(transport)).to eq(0)
      release.enqueue(true)

      expired = request("#{origin}/held", body: "abcde", deadline: ConnectRPC::Deadline.new(0.1))
      call = task.async(finished: false) { transport.call(expired) }
      entered.dequeue
      expect { call.wait }.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("deadline_exceeded") }
      expect(pending_bytes(transport)).to eq(0)
      release.enqueue(true)

      connection = transport.instance_variable_get(:@resources).first.connection
      connection.define_singleton_method(:synchronize) do |&block|
        close! unless @closed_at_dispatch
        @closed_at_dispatch = true
        super(&block)
      end
      call = task.async { transport.call(request("#{origin}/held", body: "reroute")) }
      entered.dequeue
      expect(peer.accepted).to eq(2)
      expect(pending_bytes(transport)).to eq(7)
      release.enqueue(true)
      expect(call.wait.body).to eq("ok")
      expect(pending_bytes(transport)).to eq(0)

      reserved = refusing_socket
      refused_origin = "http://127.0.0.1:#{reserved.local_address.ip_port}"
      refused = ConnectRPC::Async::Transport.new(origin: refused_origin)
      expect { refused.call(request("#{refused_origin}/x", body: "abcdef")) }.to unavailable_error
      expect(pending_bytes(refused)).to eq(0)
    ensure
      3.times { release&.enqueue(true) }
      transport&.close(grace: 0)
      refused&.close(grace: 0)
      reserved&.close
      peer&.stop
      bound&.close
    end
  end
end
