# frozen_string_literal: true

require "protocol/http/body/writable"

RSpec.describe "HTTP/2 refused Connect unary retry" do
  # Peer state shared with the examples: per-path counts, the latest incoming
  # request per path, /long holds and the requests the backend processed.
  def peer_state
    { seen: Hash.new(0), streams: {}, held: Hash.new { |hash, path| hash[path] = Async::Queue.new },
      invocations: [] }
  end

  # Refuses the first `refusals` matching streams with REFUSED_STREAM
  # (`:reset`), a GOAWAY below their id (`:goaway`) or no answer (`:hold`),
  # optionally after interim or final response HEADERS.
  def refusing_peer(task, listener, state, kind: :reset, before: nil, refusals: 1,
                    refused: ->(path) { path.start_with?("/refused") })
    ConnectionPeer::RecordingServer.new(task, listener) do |incoming|
      path = incoming.path
      count = state[:seen][path] += 1
      state[:streams][path] = incoming
      if refused.call(path) && count <= refusals
        refuse(incoming, kind, before)
      else
        process(incoming, path, state)
      end
    end
  end

  def process(incoming, path, state)
    state[:held][path].dequeue if path.start_with?("/long")
    state[:invocations] << [path, incoming.read]
    state[:on_invoke]&.call(path)
    incoming.send_response(Protocol::HTTP::Response[200, {}, [path]])
  rescue Protocol::HTTP2::Error, IOError
    # The client aborted a held stream; there is nothing to answer.
    nil
  end

  def refuse(incoming, kind, before)
    case before
    when :interim then incoming.stream.send_headers([[":status", "103"]])
    when :final then incoming.send_response(Protocol::HTTP::Response[200, {}, Protocol::HTTP::Body::Writable.new])
    end
    case kind
    when :reset then incoming.stream.send_reset_stream(Protocol::HTTP2::Error::REFUSED_STREAM)
    when :goaway then goaway(incoming.stream.connection, [incoming.stream.id - 2, 0].max)
    end
  end

  def goaway(server, last_stream_id, code = Protocol::HTTP2::Error::NO_ERROR)
    frame = Protocol::HTTP2::GoawayFrame.new
    frame.pack(last_stream_id, code, "")
    server.write_frame(frame)
  end

  def origin_for(bound) = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
  def acquiring(transport) = transport.instance_variable_get(:@acquiring)
  def resources(transport) = transport.instance_variable_get(:@resources)
  def attempt(transport) = connection_manager(transport).instance_variable_get(:@attempt)

  def released(state)
    state[:held].each_value { |queue| queue.enqueue(true) }
  end

  [["POST", :reset], ["POST", :goaway], ["GET", :reset], ["GET", :goaway]].each do |verb, kind|
    it "sends a #{verb} refused by #{kind} exactly once more with one handler invocation" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        state = peer_state
        peer = refusing_peer(task, bound.sockets.first, state, kind: kind)
        origin = origin_for(bound)
        events = []
        transport = recording_transport(origin, events)
        outgoing = if verb == "GET"
                     request("#{origin}/refused?message=abc", method: "GET")
                   else
                     request("#{origin}/refused", body: "payload")
                   end
        path = verb == "GET" ? "/refused?message=abc" : "/refused"
        expect(transport.call(outgoing).body).to eq(path)
        expect(peer.headers_frames.size).to eq(2)
        expect(state[:invocations]).to eq([[path, verb == "GET" ? nil : "payload"]])
        expect(peer.accepted).to eq(kind == :reset ? 1 : 2)
        expect(attempt_outcomes(events)).to eq(kind == :reset ? [:ready] : %i[ready ready])
        expect(lease_usage(transport)).to eq(0)
      ensure
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end
  end

  it "reuses a healthy connection after a standalone REFUSED_STREAM while a sibling completes" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      state = peer_state
      peer = refusing_peer(task, bound.sockets.first, state)
      origin = origin_for(bound)
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      long = task.async { transport.call(request("#{origin}/long")).body }
      wait_until(task, "sibling accepted") { state[:streams].key?("/long") }
      expect(transport.call(request("#{origin}/refused")).body).to eq("/refused")
      expect(long).not_to be_finished
      state[:held]["/long"].enqueue(true)
      expect(long.wait).to eq("/long")
      expect(state[:invocations].map(&:first)).to eq(%w[/refused /long])
      expect([peer.accepted, peer.headers_frames.size]).to eq([1, 3])
      expect(lease_usage(transport)).to eq(0)
    ensure
      released(state) if state
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  it "waits for capacity when the draining set is full and aborts no accepted sibling" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      state = peer_state
      peer = refusing_peer(task, bound.sockets.first, state, kind: :goaway)
      origin = origin_for(bound)
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      first = task.async { transport.call(request("#{origin}/long-a")).body }
      wait_until(task, "first sibling accepted") { state[:streams].key?("/long-a") }
      goaway(peer.connections[0], state[:streams].fetch("/long-a").stream.id)
      draining = connection_manager(transport).instance_variable_get(:@draining)
      wait_until(task, "first connection draining") { draining.size == 1 }
      second = task.async { transport.call(request("#{origin}/long-b")).body }
      wait_until(task, "second sibling accepted on a replacement") { state[:streams].key?("/long-b") }
      refused = task.async { transport.call(request("#{origin}/refused")).body }
      wait_until(task, "refused retry waits") { state[:seen]["/refused"] == 1 && acquiring(transport).size == 1 }
      expect([attempt(transport), peer.accepted]).to eq([nil, 2])
      state[:held]["/long-a"].enqueue(true)
      expect(first.wait).to eq("/long-a")
      expect(refused.wait).to eq("/refused")
      state[:held]["/long-b"].enqueue(true)
      expect(second.wait).to eq("/long-b")
      expect(state[:invocations].map(&:first)).to eq(%w[/long-a /refused /long-b])
      expect([peer.accepted, peer.headers_frames.size]).to eq([3, 4])
      expect(lease_usage(transport)).to eq(0)
    ensure
      released(state) if state
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  %i[reset goaway].each do |kind|
    it "fails a second #{kind} refusal with unavailable after two transmissions" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        state = peer_state
        peer = refusing_peer(task, bound.sockets.first, state, kind: kind, refusals: 2)
        origin = origin_for(bound)
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        expect { transport.call(request("#{origin}/refused")) }.to unavailable_error
        expect(peer.headers_frames.size).to eq(2)
        expect(state[:invocations]).to be_empty
        expect(lease_usage(transport)).to eq(0)
      ensure
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end
  end

  %i[interim final].product(%i[reset goaway]).each do |before, kind|
    it "never resends after #{before} response HEADERS and a #{kind} refusal" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        state = peer_state
        peer = refusing_peer(task, bound.sockets.first, state, kind: kind, before: before)
        origin = origin_for(bound)
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        expect { transport.call(request("#{origin}/refused")) }.to unavailable_error
        expect(peer.headers_frames.size).to eq(1)
        expect(state[:invocations]).to be_empty
        expect(lease_usage(transport)).to eq(0)
      ensure
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end
  end

  it "retires a refused stream's in-flight DATA output before resending" do
    # A generous cleanup allowance keeps the release of the held frame inside
    # the budget on a loaded machine; the stuck example covers its expiry.
    stub_const("ConnectRPC::Async::Internal::CLEANUP_SECONDS", 5)
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      state = peer_state
      peer = refusing_peer(task, bound.sockets.first, state)
      origin = origin_for(bound)
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      expect(transport.call(request("#{origin}/warm")).body).to eq("/warm")
      resource = resources(transport).first
      gate = FrameGate.new(resource.socket, frame_type: Protocol::HTTP2::DataFrame::TYPE)
      resource.connection.stream.instance_variable_set(:@io, gate)
      gate.armed = true
      call = task.async(finished: false) { transport.call(request("#{origin}/refused", body: "x" * 4096)).body }
      gate.started.dequeue
      operation = transport.instance_variable_get(:@calls).keys.first
      output = gate.writer
      retired_at_resend = nil
      state[:on_invoke] = ->(path) { retired_at_resend = output.finished? if path == "/refused" }
      wait_until(task, "refusal observed while DATA is held") { operation[:response].nil? }
      expect(output).not_to be_finished
      gate.release.enqueue(true)
      expect(call.wait).to eq("/refused")
      expect(retired_at_resend).to be(true)
      expect(state[:invocations].map(&:first)).to eq(%w[/warm /refused])
      expect([peer.accepted, peer.headers_frames.size]).to eq([1, 3])
      expect(lease_usage(transport)).to eq(0)
    ensure
      gate&.release&.enqueue(true)
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  it "fails without resending when a refused stream's DATA frame stays stuck" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      state = peer_state
      peer = refusing_peer(task, bound.sockets.first, state)
      origin = origin_for(bound)
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      expect(transport.call(request("#{origin}/warm")).body).to eq("/warm")
      resource = resources(transport).first
      gate = FrameGate.new(resource.socket, frame_type: Protocol::HTTP2::DataFrame::TYPE)
      resource.connection.stream.instance_variable_set(:@io, gate)
      gate.armed = true
      call = task.async(finished: false) { transport.call(request("#{origin}/refused", body: "x" * 4096)) }
      gate.started.dequeue
      # The stuck frame outlives the cleanup allowance; the call fails and
      # its connection retires, so nothing is sent again.
      expect { call.wait }.to unavailable_error
      expect(resource).to be_finished
      expect(peer.headers_frames.size).to eq(2)
      expect(transport.call(request("#{origin}/after")).body).to eq("/after")
      expect(state[:invocations].map(&:first)).to eq(%w[/warm /after])
      expect([peer.accepted, peer.headers_frames.size]).to eq([2, 3])
      expect(lease_usage(transport)).to eq(0)
    ensure
      gate&.release&.enqueue(true)
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  %i[deadline cancellation close].each do |ending|
    it "ends a refused retry waiting for capacity on #{ending} without resending" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        state = peer_state
        peer = refusing_peer(task, bound.sockets.first, state, kind: :goaway)
        origin = origin_for(bound)
        policy = ConnectRPC::Async::ConnectionPolicy.new(max_draining: 0)
        transport = ConnectRPC::Async::Transport.new(origin: origin, connection_policy: policy)
        long = task.async(finished: false) { transport.call(request("#{origin}/long")).body }
        wait_until(task, "long stream accepted") { state[:streams].key?("/long") }
        source = ConnectRPC::CancellationSource.new
        # The deadline variant measures real elapsed time: its expiry while
        # the retry waits is the boundary under test.
        outgoing = request("#{origin}/refused", deadline: ending == :deadline ? ConnectRPC::Deadline.new(1) : nil)
        outgoing.cancellation = source.token if ending == :cancellation
        refused = task.async(finished: false) { transport.call(outgoing) }
        wait_until(task, "refused retry waits") { state[:seen]["/refused"] == 1 && acquiring(transport).size == 1 }
        case ending
        when :deadline
          expect { refused.wait }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("deadline_exceeded") }
        when :cancellation
          source.cancel
          expect { refused.wait }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("canceled") }
        else
          closer = task.async { transport.close(grace: 0) }
          expect { refused.wait }.to unavailable_error
          expect { long.wait }.to unavailable_error
          closer.wait
        end
        unless ending == :close
          state[:held]["/long"].enqueue(true)
          expect(long.wait).to eq("/long")
        end
        expect([peer.accepted, peer.headers_frames.size]).to eq([1, 2])
        expect(state[:seen]["/refused"]).to eq(1)
        expect(lease_usage(transport)).to eq(0)
      ensure
        released(state) if state
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end
  end

  it "never resends a refused native gRPC unary call or Connect stream" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      state = peer_state
      peer = refusing_peer(task, bound.sockets.first, state, refusals: 2, refused: ->(_path) { true })
      origin = origin_for(bound)
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      grpc = client(transport, origin, protocol: :grpc)
      expect { grpc.echo(AsyncHelpers::MESSAGE.new(name: "once")) }.to unavailable_error
      expect(peer.headers_frames.size).to eq(1)
      method = ConnectRPC::ServerStreamMethod.new(service: "test.Refused", name: "Watch",
                                                  request_type: AsyncHelpers::MESSAGE,
                                                  response_type: AsyncHelpers::MESSAGE)
      core = ConnectRPC::Client.new(base_url: origin, transport: transport,
                                    binding: ConnectRPC::Bindings::GoogleProtobuf.new)
      expect do
        core.server_stream(method, AsyncHelpers::MESSAGE.new(name: "once")) { |reader| reader.each { |_| nil } }
      end.to unavailable_error
      expect(peer.headers_frames.size).to eq(2)
      expect(state[:seen].values).to eq([1, 1])
      expect(state[:invocations]).to be_empty
      expect(peer.accepted).to eq(1)
      expect(lease_usage(transport)).to eq(0)
    ensure
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  it "fails a refused unary call after one transmission when retry_refused is off" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      state = peer_state
      peer = refusing_peer(task, bound.sockets.first, state)
      origin = origin_for(bound)
      policy = ConnectRPC::Async::ConnectionPolicy.new(retry_refused: false)
      transport = ConnectRPC::Async::Transport.new(origin: origin, connection_policy: policy)
      expect { transport.call(request("#{origin}/refused")) }.to unavailable_error
      expect(peer.headers_frames.size).to eq(1)
      expect(state[:invocations]).to be_empty
      expect(lease_usage(transport)).to eq(0)
    ensure
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  it "fails streams at or below an error GOAWAY's last id and resends only a stream above it" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      state = peer_state
      peer = refusing_peer(task, bound.sockets.first, state, kind: :hold)
      origin = origin_for(bound)
      events = []
      transport = recording_transport(origin, events)
      long = task.async(finished: false) { transport.call(request("#{origin}/long")) }
      wait_until(task, "long stream accepted") { state[:streams].key?("/long") }
      refused = task.async(finished: false) { transport.call(request("#{origin}/refused")).body }
      wait_until(task, "second stream accepted") { state[:streams].key?("/refused") }
      goaway(peer.connections.first, state[:streams].fetch("/long").stream.id, Protocol::HTTP2::Error::INTERNAL_ERROR)
      expect { long.wait }.to unavailable_error
      expect(refused.wait).to eq("/refused")
      expect(state[:seen]).to eq("/long" => 1, "/refused" => 2)
      expect(state[:invocations].map(&:first)).to eq(["/refused"])
      expect([peer.accepted, peer.headers_frames.size]).to eq([2, 3])
      expect(attempt_outcomes(events)).to eq(%i[ready ready])
      expect(lease_usage(transport)).to eq(0)
    ensure
      released(state) if state
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end
end
