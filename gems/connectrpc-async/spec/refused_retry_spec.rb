# frozen_string_literal: true

require "protocol/http/body/writable"

RSpec.describe "HTTP/2 refused Connect unary retry" do
  # Peer state shared with the examples: per-path counts, the latest incoming
  # request per path, each transmission's connect-timeout-ms, /long holds and
  # the requests the backend processed.
  def peer_state
    { seen: Hash.new(0), streams: {}, timeouts: Hash.new { |hash, path| hash[path] = [] },
      held: Hash.new { |hash, path| hash[path] = Async::Queue.new }, invocations: [] }
  end

  # Refuses the first `refusals` matching streams with a reset carrying
  # `code` (`:reset`), a GOAWAY below their id (`:goaway`) or no answer
  # (`:hold`), optionally after interim or final response HEADERS; `:headers`
  # sends final HEADERS without a response body. With `coalesce`, the
  # refusal's frames reach the socket in one write.
  def refusing_peer(task, listener, state, kind: :reset, before: nil, refusals: 1,
                    code: Protocol::HTTP2::Error::REFUSED_STREAM, coalesce: false,
                    refused: ->(path) { path.start_with?("/refused") })
    ConnectionPeer::RecordingServer.new(task, listener) do |incoming|
      path = incoming.path
      count = state[:seen][path] += 1
      state[:streams][path] = incoming
      state[:timeouts][path] << incoming.headers["connect-timeout-ms"]&.to_s
      if refused.call(path) && count <= refusals
        coalesced(coalesce && incoming.stream.connection) { refuse(incoming, kind, before, code) }
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

  def refuse(incoming, kind, before, code)
    case before
    when :interim then incoming.stream.send_headers([[":status", "103"]])
    when :headers then incoming.stream.send_headers([[":status", "200"]])
    when :final then incoming.send_response(Protocol::HTTP::Response[200, {}, Protocol::HTTP::Body::Writable.new])
    end
    case kind
    when :reset then incoming.stream.send_reset_stream(code)
    when :goaway then goaway(incoming.stream.connection, [incoming.stream.id - 2, 0].max)
    end
  end

  # While a client writer holds its stream, io-stream's flush-before-read
  # blocks the client's reader, which then sees only frames already buffered.
  # The frames must be written without yielding: the server's own reader
  # flushes its stream before each read.
  def coalesced(server)
    return yield unless server

    framer = server.instance_variable_get(:@framer)
    framer.define_singleton_method(:flush) { nil }
    begin
      yield
    ensure
      framer.singleton_class.remove_method(:flush)
      framer.flush
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
  def operation(transport) = transport.instance_variable_get(:@calls).keys.first

  # Holds the next DATA frame (optionally only the given stream's) after its
  # frame header, before its payload reaches the socket.
  def gate_data(transport, stream_id: nil)
    resource = resources(transport).first
    gate = FrameGate.new(resource.socket, frame_type: Protocol::HTTP2::DataFrame::TYPE, stream_id: stream_id)
    resource.connection.stream.instance_variable_set(:@io, gate)
    gate.armed = true
    [resource, gate]
  end

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
        deadline = ConnectRPC::Deadline.new(30)
        outgoing = if verb == "GET"
                     request("#{origin}/refused?message=abc", method: "GET", deadline: deadline)
                   else
                     request("#{origin}/refused", body: "payload", deadline: deadline)
                   end
        path = verb == "GET" ? "/refused?message=abc" : "/refused"
        expect(transport.call(outgoing).body).to eq(path)
        expect(peer.headers_frames.size).to eq(2)
        # The resend carries a timeout rebuilt from the remaining deadline.
        first, resent = state[:timeouts][path].map { |value| Integer(value) }
        expect(state[:timeouts][path].size).to eq(2)
        expect(resent).to be_positive
        expect(resent).to be <= first
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

  # A REFUSED_STREAM resend reuses the healthy connection, whose write guard
  # would serialize it anyway; a resend above a GOAWAY's last id goes to a
  # replacement connection, so only the retirement join orders it.
  [[:reset, 1], [:goaway, 2]].each do |kind, accepted|
    it "retires a refused stream's in-flight DATA output before resending (#{kind})" do
      # A generous cleanup allowance keeps the release of the held frame inside
      # the budget on a loaded machine; the stuck example covers its expiry.
      stub_const("ConnectRPC::Async::Internal::CLEANUP_SECONDS", 5)
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        state = peer_state
        peer = refusing_peer(task, bound.sockets.first, state, kind: kind)
        origin = origin_for(bound)
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        expect(transport.call(request("#{origin}/warm")).body).to eq("/warm")
        _, gate = gate_data(transport)
        call = task.async(finished: false) { transport.call(request("#{origin}/refused", body: "x" * 4096)).body }
        gate.started.dequeue
        current = operation(transport)
        output = gate.writer
        retired_at_resend = nil
        state[:on_invoke] = ->(path) { retired_at_resend = output.finished? if path == "/refused" }
        wait_until(task, "refusal observed while DATA is held") { current[:response].nil? }
        expect(output).not_to be_finished
        # The held output keeps the lease, so nothing redials or resends yet.
        expect([peer.accepted, peer.headers_frames.size, lease_usage(transport)]).to eq([1, 2, 1])
        gate.release.enqueue(true)
        expect(call.wait).to eq("/refused")
        expect(retired_at_resend).to be(true)
        expect(state[:invocations].map(&:first)).to eq(%w[/warm /refused])
        expect([peer.accepted, peer.headers_frames.size]).to eq([accepted, 3])
        expect(lease_usage(transport)).to eq(0)
      ensure
        gate&.release&.enqueue(true)
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
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

  # A refused stream's output can still be inside a held DATA write when the
  # call ends. Cancelling the call must not cut that frame short on a
  # connection that stays usable: the stuck frame fails its connection.
  {
    deadline: ["deadline_exceeded", {}, 1],
    cancellation: ["canceled", {}, 1],
    no_retry: ["unavailable", { retry_refused: false }, 1],
    second_refusal: ["unavailable", {}, 2]
  }.each do |ending, (code, settings, transmissions)|
    it "fails the connection raw-first when a refused stream's held DATA outlives the call (#{ending})" do
      joining = %i[deadline cancellation].include?(ending)
      # These calls end while the retry joins the held output, so that join's
      # allowance must outlast them.
      stub_const("ConnectRPC::Async::Internal::CLEANUP_SECONDS", 30) if joining
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        state = peer_state
        peer = refusing_peer(task, bound.sockets.first, state, refusals: ending == :second_refusal ? 2 : 1)
        origin = origin_for(bound)
        policy = ConnectRPC::Async::ConnectionPolicy.new(**settings)
        transport = ConnectRPC::Async::Transport.new(origin: origin, connection_policy: policy)
        expect(transport.call(request("#{origin}/warm")).body).to eq("/warm")
        # After /warm (stream 1) and the first /refused (stream 3), a second
        # refusal holds the resend's DATA on stream 5.
        resource, gate = gate_data(transport, stream_id: ending == :second_refusal ? 5 : nil)
        source = ConnectRPC::CancellationSource.new
        # The deadline variant measures real elapsed time: its expiry during
        # the retry's output join is the boundary under test.
        outgoing = request("#{origin}/refused", body: "x" * 4096,
                                                deadline: ending == :deadline ? ConnectRPC::Deadline.new(2) : nil)
        outgoing.cancellation = source.token if ending == :cancellation
        call = task.async(finished: false) { transport.call(outgoing) }
        gate.started.dequeue
        output = gate.writer
        if joining
          current = operation(transport)
          wait_until(task, "retry joins the held output", within: 0.5) do
            current[:response].nil? && current[:refused_output]
          end
          # Whole-operation cleanup keeps its usual allowance.
          stub_const("ConnectRPC::Async::Internal::CLEANUP_SECONDS", 0.1)
          source.cancel if ending == :cancellation
        end
        expect { call.wait }.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq(code) }
        expect(resource).to be_finished
        expect(output).to be_finished
        expect(lease_usage(transport)).to eq(0)
        expect(peer.headers_frames.size).to eq(transmissions + 1)
        expect(transport.call(request("#{origin}/after")).body).to eq("/after")
        expect(state[:invocations].map(&:first)).to eq(%w[/warm /after])
        expect([peer.accepted, peer.headers_frames.size]).to eq([2, transmissions + 2])
        expect(lease_usage(transport)).to eq(0)
      ensure
        gate&.release&.enqueue(true)
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end
  end

  it "keeps the connection when a refused stream's held DATA completes within cleanup" do
    # A generous cleanup allowance keeps the release inside the budget on a
    # loaded machine.
    stub_const("ConnectRPC::Async::Internal::CLEANUP_SECONDS", 5)
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      state = peer_state
      peer = refusing_peer(task, bound.sockets.first, state)
      origin = origin_for(bound)
      policy = ConnectRPC::Async::ConnectionPolicy.new(retry_refused: false)
      transport = ConnectRPC::Async::Transport.new(origin: origin, connection_policy: policy)
      expect(transport.call(request("#{origin}/warm")).body).to eq("/warm")
      resource, gate = gate_data(transport)
      call = task.async(finished: false) { transport.call(request("#{origin}/refused", body: "x" * 4096)) }
      gate.started.dequeue
      current = operation(transport)
      output = gate.writer
      wait_until(task, "cleanup joins the held output") { current[:cleaned] }
      # The call keeps its lease until the output retires.
      expect([output.finished?, lease_usage(transport)]).to eq([false, 1])
      gate.release.enqueue(true)
      expect { call.wait }.to unavailable_error
      expect([resource.finished?, output.finished?]).to eq([false, true])
      expect(lease_usage(transport)).to eq(0)
      expect(transport.call(request("#{origin}/after")).body).to eq("/after")
      expect([peer.accepted, peer.headers_frames.size]).to eq([1, 3])
    ensure
      gate&.release&.enqueue(true)
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  # A reset after final response HEADERS fails the body read, outside the
  # retry's header wait, and stops the held output once. Cleanup retires that
  # output before cancelling the call, or fails the connection raw-first.
  {
    refused_stream: Protocol::HTTP2::Error::REFUSED_STREAM,
    cancel: Protocol::HTTP2::Error::CANCEL
  }.to_a.product(%i[stuck released]).each do |(name, code), outcome|
    it "retires held DATA after final HEADERS and a #{name} reset when the frame is #{outcome}" do
      released = outcome == :released
      # A generous allowance keeps the release inside cleanup's join on a
      # loaded machine; the stuck variant keeps the usual allowance.
      stub_const("ConnectRPC::Async::Internal::CLEANUP_SECONDS", 5) if released
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        state = peer_state
        peer = refusing_peer(task, bound.sockets.first, state, before: :headers, code: code, coalesce: true)
        origin = origin_for(bound)
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        expect(transport.call(request("#{origin}/warm")).body).to eq("/warm")
        resource, gate = gate_data(transport)
        call = task.async(finished: false) { transport.call(request("#{origin}/refused", body: "x" * 4096)) }
        gate.started.dequeue
        current = operation(transport)
        output = gate.writer
        if released
          wait_until(task, "cleanup joins the held output") { current[:cleaned] }
          # The call keeps its lease until the output retires.
          expect([output.finished?, lease_usage(transport)]).to eq([false, 1])
          gate.release.enqueue(true)
        end
        expect { call.wait }.to unavailable_error
        # The call failed reading the body, after the header wait returned.
        expect(current.fetch(:response).stream.response_headers?).to be(true)
        expect([resource.finished?, output.finished?]).to eq([!released, true])
        expect(lease_usage(transport)).to eq(0)
        expect(peer.headers_frames.size).to eq(2)
        expect(transport.call(request("#{origin}/after")).body).to eq("/after")
        expect(state[:invocations].map(&:first)).to eq(%w[/warm /after])
        expect([peer.accepted, peer.headers_frames.size]).to eq([released ? 1 : 2, 3])
        expect(lease_usage(transport)).to eq(0)
      ensure
        gate&.release&.enqueue(true)
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end
  end

  # A caller can abandon a call while its retry joins the refused output,
  # without a ConnectRPC cancellation or deadline: the output then retires
  # during whole-operation cleanup, and the call must not reroute, dial or
  # resend after it.
  [%i[stop reset], %i[stop goaway], %i[cancellation goaway], %i[timeout reset]].each do |ending, kind|
    it "never resends or dials after cleanup starts during the retry join (#{ending}, #{kind})" do
      # The retry's own join must outlast the caller's ending.
      stub_const("ConnectRPC::Async::Internal::CLEANUP_SECONDS", 30)
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        state = peer_state
        peer = refusing_peer(task, bound.sockets.first, state, kind: kind)
        origin = origin_for(bound)
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        expect(transport.call(request("#{origin}/warm")).body).to eq("/warm")
        resource, gate = gate_data(transport)
        source = ConnectRPC::CancellationSource.new
        outgoing = request("#{origin}/refused", body: "x" * 4096)
        outgoing.cancellation = source.token if ending == :cancellation
        # The timeout variant measures real elapsed time: Async's own timer
        # interrupting the caller during the retry join is the boundary under
        # test. The wait for the join is bounded well inside it.
        call = task.async(finished: false) do |caller|
          ending == :timeout ? caller.with_timeout(2) { transport.call(outgoing) } : transport.call(outgoing)
        end
        gate.started.dequeue
        current = operation(transport)
        output = gate.writer
        wait_until(task, "retry joins the held output", within: 0.5) do
          current[:response].nil? && current[:refused_output]
        end
        case ending
        when :stop then call.stop
        when :cancellation then source.cancel
        end
        wait_until(task, "cleanup joins the held output", within: 5) { current[:cleaned] }
        # The client reader fails a connection whose GOAWAY leaves no streams,
        # which also retires the held output; a reset keeps the connection.
        expect([output.finished?, lease_usage(transport)]).to eq([false, 1]) if kind == :reset
        gate.release.enqueue(true)
        begin
          call.wait
        rescue ConnectRPC::Error
          nil
        end
        wait_until(task, "call cleaned up") { transport.instance_variable_get(:@calls).empty? }
        expect(output).to be_finished
        # A drained GOAWAY connection closes once its last lease returns.
        expect(resource).not_to be_finished if kind == :reset
        expect(lease_usage(transport)).to eq(0)
        expect([state[:seen]["/refused"], peer.headers_frames.size]).to eq([1, 2])
        expect([attempt(transport), peer.accepted]).to eq([nil, 1])
        expect(transport.call(request("#{origin}/after")).body).to eq("/after")
        expect(state[:invocations].map(&:first)).to eq(%w[/warm /after])
        expect([peer.accepted, peer.headers_frames.size]).to eq([kind == :goaway ? 2 : 1, 3])
        expect(lease_usage(transport)).to eq(0)
      ensure
        gate&.release&.enqueue(true)
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end
  end

  it "never resends when the HEADERS write fails during dispatch" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      state = peer_state
      peer = refusing_peer(task, bound.sockets.first, state)
      origin = origin_for(bound)
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      expect(transport.call(request("#{origin}/warm")).body).to eq("/warm")
      connection = resources(transport).first.connection
      streams = []
      failures = []
      allow(connection).to receive(:create_response).and_wrap_original do |original|
        original.call.tap do |response|
          streams << response.stream
          allow(response.stream).to receive(:send_headers).and_raise(IOError, "HEADERS write failed")
        end
      end
      allow(connection).to receive(:write_request).and_wrap_original do |original, *args|
        original.call(*args)
      rescue StandardError => e
        failures << e
        raise
      end
      expect { transport.call(request("#{origin}/refused", body: "payload")) }.to unavailable_error
      # The pinned async-http reports the failed write as a refusal, which
      # does not prove the server never processed the stream.
      expect(failures.map(&:class)).to eq([Protocol::HTTP::RefusedError])
      expect(streams.size).to eq(1)
      expect(streams.first).to have_received(:send_headers).once
      expect(peer.headers_frames.size).to eq(1)
      expect(state[:invocations].map(&:first)).to eq(%w[/warm])
      expect(lease_usage(transport)).to eq(0)
    ensure
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
        # the retry waits is the boundary under test. The wait for the retry
        # is bounded well inside that deadline, so it is observed first.
        outgoing = request("#{origin}/refused", deadline: ending == :deadline ? ConnectRPC::Deadline.new(2) : nil)
        outgoing.cancellation = source.token if ending == :cancellation
        refused = task.async(finished: false) { transport.call(outgoing) }
        wait_until(task, "refused retry waits", within: 0.5) do
          state[:seen]["/refused"] == 1 && acquiring(transport).size == 1
        end
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
