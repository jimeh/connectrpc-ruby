# frozen_string_literal: true

require "protocol/http/body/writable"

RSpec.describe "HTTP/2 client keepalive" do
  def origin_for(bound) = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
  def resources(transport) = transport.instance_variable_get(:@resources)
  def monitor_task(resource) = resource.instance_variable_get(:@maintenance).first
  def closed_outcomes(events) = events.select { |event| event.type == :connection_closed }.map(&:outcome)
  def holds = Hash.new { |hash, path| hash[path] = Async::Queue.new }

  def keepalive_policy(time: 10, timeout: 5, permit: false, **)
    keepalive = ConnectRPC::Async::Keepalive.new(time: time, timeout: timeout, permit_without_calls: permit)
    ConnectRPC::Async::ConnectionPolicy.new(keepalive: keepalive, **)
  end

  # Records each request path, holds /long paths until released and streams
  # /stream bodies the example writes.
  def keepalive_peer(task, listener, held, invocations, bodies: [], settings: {})
    ConnectionPeer::RecordingServer.new(task, listener, settings: settings) do |incoming|
      invocations << incoming.path
      if incoming.path == "/stream"
        body = Protocol::HTTP::Body::Writable.new
        bodies << body
        incoming.send_response(Protocol::HTTP::Response[200, {}, body])
      else
        held[incoming.path].dequeue if incoming.path.start_with?("/long")
        incoming.send_response(Protocol::HTTP::Response[200, {}, [incoming.path]])
      end
    rescue Protocol::HTTP2::Error, IOError
      # The client retired a held stream; there is nothing to answer.
      nil
    end
  end

  def await_due(task, clock, due)
    wait_until(task, "a clock sleep due at #{due}; pending #{clock.sleepers.map(&:due)}") do
      clock.sleepers.any? { |sleeper| sleeper.due == due }
    end
  end

  def payload(sequence) = [sequence].pack("Q>")

  # The client acknowledges a peer PING only after processing every earlier frame.
  def ping_through(task, peer, server, data = "through!")
    server.send_ping(data)
    wait_until(task, "peer PING acknowledged") { peer.pings.any? { |_, ack, bytes| ack && bytes == data } }
  end

  def goaway(server, last_stream_id, code = Protocol::HTTP2::Error::NO_ERROR, debug = "")
    frame = Protocol::HTTP2::GoawayFrame.new
    frame.pack(last_stream_id, code, debug)
    server.write_frame(frame)
  end

  # Holds the next client frame of `frame_type` on the wire; see FrameGate.
  def gate(resource, frame_type, whole: false)
    gate = FrameGate.new(resource.socket, frame_type: frame_type, whole: whole)
    resource.connection.stream.instance_variable_set(:@io, gate)
    gate.armed = true
    gate
  end

  it "retires a black-holed connection by time + timeout without resending its call" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      held = holds
      invocations = []
      peer = keepalive_peer(task, bound.sockets.first, held, invocations)
      peer.ack_pings = false
      origin = origin_for(bound)
      events = []
      transport = recording_transport(origin, events, connection_policy: keepalive_policy)
      clock = install_clock(transport)
      long = task.async(finished: false) { transport.call(request("#{origin}/long")) }
      wait_until(task, "long call invoked") { invocations == ["/long"] }
      resource = resources(transport).first
      monitor = monitor_task(resource)
      await_due(task, clock, 10)
      clock.advance(10)
      wait_until(task, "keepalive PING on the wire") { peer.ping_payloads == [payload(1)] }
      await_due(task, clock, 15)
      expect(resource).not_to be_finished
      clock.advance(5)
      expect { long.wait }.to unavailable_error
      expect([resource.finished?, clock.now]).to eq([true, 15])
      # The supervisor's own abort completed retirement, including itself.
      wait_until(task, "retirement completed") { resource.send(:retirement_completed?) }
      expect(monitor).to be_finished
      expect(closed_outcomes(events)).to eq([:keepalive_timeout])
      expect(transport.call(request("#{origin}/after")).body).to eq("/after")
      expect(invocations).to eq(%w[/long /after])
      expect([peer.accepted, peer.headers_frames.size]).to eq([2, 2])
      expect(closed_outcomes(events)).to eq([:keepalive_timeout])
      expect(lease_usage(transport)).to eq(0)
    ensure
      held&.each_value { |queue| queue.enqueue(true) }
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  it "retires only the black-holed connection while its sibling keeps serving" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      held = holds
      invocations = []
      settings = { Protocol::HTTP2::Settings::MAXIMUM_CONCURRENT_STREAMS => 1 }
      peer = keepalive_peer(task, bound.sockets.first, held, invocations, settings: settings)
      peer.black_holes << 0
      origin = origin_for(bound)
      events = []
      transport = recording_transport(origin, events, connection_limit: 2, connection_policy: keepalive_policy)
      clock = install_clock(transport)
      first = task.async(finished: false) { transport.call(request("#{origin}/long-a")) }
      wait_until(task, "first call invoked") { invocations == ["/long-a"] }
      second = task.async(finished: false) { transport.call(request("#{origin}/long-b")) }
      wait_until(task, "second call invoked") { invocations == %w[/long-a /long-b] }
      black_holed, healthy = resources(transport)
      wait_until(task, "both monitors sleeping") { clock.sleepers.count { |sleeper| sleeper.due == 10 } == 2 }
      clock.advance(10)
      # The healthy connection's ACK schedules its next probe from that read.
      await_due(task, clock, 20)
      await_due(task, clock, 15)
      expect(peer.ping_payloads(0)).to eq([payload(1)])
      expect(peer.ping_payloads(1)).to eq([payload(1)])
      clock.advance(5)
      expect { first.wait }.to unavailable_error
      expect([black_holed.finished?, healthy.finished?]).to eq([true, false])
      held["/long-b"].enqueue(true)
      expect(second.wait.body).to eq("/long-b")
      expect(transport.call(request("#{origin}/after")).body).to eq("/after")
      expect(peer.accepted).to eq(2)
      expect(connection_manager(transport).connections.keys).to eq([healthy.connection])
      expect(closed_outcomes(events)).to eq([:keepalive_timeout])
    ensure
      held&.each_value { |queue| queue.enqueue(true) }
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  # The #72 hazard: a peer-reset stream's request frame held mid-write keeps
  # the guard with no registered stream, and a later call waits behind it.
  it "retires a connection whose reset stream's frame never finishes" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      reset = Async::Queue.new
      peer = ConnectionPeer::RecordingServer.new(task, bound.sockets.first) do |incoming|
        if incoming.path == "/stream"
          reset.dequeue
          incoming.stream.send_reset_stream(Protocol::HTTP2::Error::CANCEL)
        else
          incoming.send_response(Protocol::HTTP::Response[200, {}, [incoming.path]])
        end
      rescue Protocol::HTTP2::Error, IOError
        nil
      end
      origin = origin_for(bound)
      events = []
      # Static windows: BDP PINGs would join the exact PING list asserted here.
      transport = recording_transport(origin, events, connection_policy: keepalive_policy,
                                                      dynamic_receive_windows: false)
      clock = install_clock(transport)
      expect(transport.call(request("#{origin}/warm")).body).to eq("/warm")
      resource = resources(transport).first
      data = gate(resource, Protocol::HTTP2::DataFrame::TYPE)
      outgoing = ConnectRPC::StreamRequest.new(url: URI("#{origin}/stream"), http_method: "POST", headers: {})
      proceed = Async::Queue.new
      call = task.async(finished: false) do
        transport.open_stream(outgoing) do |exchange|
          exchange.write("x" * 4096)
          proceed.dequeue
        end
      end
      data.started.dequeue
      stream = transport.instance_variable_get(:@calls).keys.first.fetch(:response).stream
      reset.enqueue(true)
      wait_until(task, "peer reset observed while DATA is held") { stream.closed? }
      proceed.enqueue(true)
      call.wait
      expect([resource.connection.streams.size, lease_usage(transport), resource.finished?]).to eq([0, 0, false])
      sibling = task.async(finished: false) { transport.call(request("#{origin}/after")).body }
      wait_until(task, "sibling waits behind the held frame") { guard_waiters(resource.connection) == 1 }
      await_due(task, clock, 10)
      clock.advance(10)
      wait_until(task, "keepalive writer waits behind the held frame") { guard_waiters(resource.connection) == 2 }
      await_due(task, clock, 15)
      clock.advance(5)
      wait_until(task, "connection retired") { resource.finished? }
      expect(clock.now).to eq(15)
      # The waiting call had sent nothing, so it reroutes to a new connection.
      expect(sibling.wait).to eq("/after")
      expect(data).not_to be_written
      expect(peer.ping_payloads).to be_empty
      expect(transport.call(request("#{origin}/next")).body).to eq("/next")
      expect(peer.accepted).to eq(2)
      expect(closed_outcomes(events)).to eq([:keepalive_timeout])
      expect(lease_usage(transport)).to eq(0)
    ensure
      data&.release&.enqueue(true)
      reset&.enqueue(true)
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  describe "probe timing" do
    it "defers the probe from the last frame read and pings only after time without reads" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        held = holds
        invocations = []
        peer = keepalive_peer(task, bound.sockets.first, held, invocations)
        origin = origin_for(bound)
        transport = ConnectRPC::Async::Transport.new(origin: origin, connection_policy: keepalive_policy(timeout: 7))
        clock = install_clock(transport)
        long = task.async(finished: false) { transport.call(request("#{origin}/long")) }
        wait_until(task, "long call invoked") { invocations == ["/long"] }
        resource = resources(transport).first
        await_due(task, clock, 10)
        clock.advance(5)
        # The client's reply to this PING is held mid-write by its reader, yet
        # the read was already timestamped.
        ack = gate(resource, Protocol::HTTP2::PingFrame::TYPE)
        peer.connections.first.send_ping("peerping")
        ack.started.dequeue
        clock.advance(5)
        await_due(task, clock, 15)
        expect(peer.ping_payloads).to be_empty
        ack.release.enqueue(true)
        wait_until(task, "held reply written") { ack.written? }
        clock.advance(5)
        wait_until(task, "keepalive PING sent") { peer.ping_payloads == [payload(1)] }
        # The ACK, read at 15, schedules the next probe at 25.
        await_due(task, clock, 25)
        held["/long"].enqueue(true)
        expect(long.wait.body).to eq("/long")
        expect(resource).not_to be_finished
      ensure
        held&.each_value { |queue| queue.enqueue(true) }
        ack&.release&.enqueue(true)
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end

    it "stays dormant without work until a stream opens, then probes at once" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        invocations = []
        peer = keepalive_peer(task, bound.sockets.first, holds, invocations)
        origin = origin_for(bound)
        # Static windows: BDP PINGs would join the exact PING list asserted here.
        transport = ConnectRPC::Async::Transport.new(origin: origin, connection_policy: keepalive_policy,
                                                     dynamic_receive_windows: false)
        clock = install_clock(transport)
        expect(transport.call(request("#{origin}/warm")).body).to eq("/warm")
        await_due(task, clock, 10)
        clock.advance(10)
        wait_until(task, "monitor dormant") { clock.sleepers.empty? }
        clock.advance(100)
        expect(peer.ping_payloads).to be_empty
        expect(transport.call(request("#{origin}/next")).body).to eq("/next")
        wait_until(task, "PING on waking") { peer.ping_payloads == [payload(1)] }
        expect(resources(transport).size).to eq(1)
      ensure
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end

    it "pings an idle connection with permit_without_calls" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        peer = keepalive_peer(task, bound.sockets.first, holds, [])
        origin = origin_for(bound)
        # Static windows: BDP PINGs would join the exact PING list asserted here.
        transport = ConnectRPC::Async::Transport.new(origin: origin, connection_policy: keepalive_policy(permit: true),
                                                     dynamic_receive_windows: false)
        clock = install_clock(transport)
        expect(transport.call(request("#{origin}/warm")).body).to eq("/warm")
        2.times do |index|
          await_due(task, clock, 10 * (index + 1))
          clock.advance(10)
          wait_until(task, "idle PING #{index + 1}") { peer.ping_payloads.size == index + 1 }
        end
        await_due(task, clock, 30)
        expect(peer.ping_payloads).to eq([payload(1), payload(2)])
        expect(resources(transport).first).not_to be_finished
      ensure
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end

    %i[unrelated_ack data inbound_ping].each do |frame|
      it "does not accept #{frame} after an unacknowledged PING as liveness" do
        reactor do |task|
          bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
          invocations = []
          bodies = []
          peer = keepalive_peer(task, bound.sockets.first, holds, invocations, bodies: bodies)
          peer.ack_pings = false
          origin = origin_for(bound)
          events = []
          transport = recording_transport(origin, events, connection_policy: keepalive_policy)
          clock = install_clock(transport)
          call = task.async(finished: false) { transport.call(request("#{origin}/stream")) }
          wait_until(task, "streaming response started") { bodies.size == 1 }
          resource = resources(transport).first
          await_due(task, clock, 10)
          clock.advance(10)
          wait_until(task, "keepalive PING on the wire") { peer.ping_payloads == [payload(1)] }
          server = peer.connections.first
          case frame
          when :unrelated_ack
            ack = Protocol::HTTP2::PingFrame.new
            ack.pack("unrelatd")
            ack.acknowledgement!
            server.write_frame(ack)
          when :data then bodies.first.write("chunk")
          end
          ping_through(task, peer, server)
          await_due(task, clock, 15)
          clock.advance(5)
          expect { call.wait }.to unavailable_error
          expect(resource).to be_finished
          expect(closed_outcomes(events)).to eq([:keepalive_timeout])
        ensure
          transport&.close(grace: 0)
          peer&.stop
          bound&.close
        end
      end
    end

    it "accepts a matching ACK read before the PING writer returns, without waiting for the writer" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        held = holds
        invocations = []
        peer = keepalive_peer(task, bound.sockets.first, held, invocations)
        origin = origin_for(bound)
        events = []
        transport = recording_transport(origin, events, connection_policy: keepalive_policy)
        clock = install_clock(transport)
        long = task.async(finished: false) { transport.call(request("#{origin}/long")) }
        wait_until(task, "long call invoked") { invocations == ["/long"] }
        resource = resources(transport).first
        await_due(task, clock, 10)
        ping = gate(resource, Protocol::HTTP2::PingFrame::TYPE, whole: true)
        clock.advance(10)
        ping.started.dequeue
        # The ACK cancels the probe deadline and schedules the next probe from
        # its read while the writer is still held.
        wait_until(task, "PING reached the peer") { peer.ping_payloads == [payload(1)] }
        wait_until(task, "only the next probe's sleep, due at 20") { clock.sleepers.map(&:due) == [20] }
        clock.advance(5)
        expect(resource).not_to be_finished
        ping.release.enqueue(true)
        held["/long"].enqueue(true)
        expect(long.wait.body).to eq("/long")
        expect([resource.finished?, closed_outcomes(events)]).to eq([false, []])
      ensure
        held&.each_value { |queue| queue.enqueue(true) }
        ping&.release&.enqueue(true)
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end

    it "rejects a matching ACK read after the deadline, before the deadline's timer runs" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        held = holds
        invocations = []
        peer = keepalive_peer(task, bound.sockets.first, held, invocations)
        peer.ack_pings = false
        origin = origin_for(bound)
        events = []
        transport = recording_transport(origin, events, connection_policy: keepalive_policy)
        clock = install_clock(transport)
        long = task.async(finished: false) { transport.call(request("#{origin}/long")) }
        wait_until(task, "long call invoked") { invocations == ["/long"] }
        resource = resources(transport).first
        await_due(task, clock, 10)
        clock.advance(10)
        wait_until(task, "keepalive PING on the wire") { peer.ping_payloads == [payload(1)] }
        await_due(task, clock, 15)
        ack = Protocol::HTTP2::PingFrame.new
        ack.pack(payload(1))
        ack.acknowledgement!
        clock.advance(5)
        # The reader's steps for an ACK read now, without yielding to the timer.
        resource.frame_read
        resource.connection.receive_ping(ack)
        wait_until(task, "connection retired") { resource.finished? }
        expect { long.wait }.to unavailable_error
        expect(closed_outcomes(events)).to eq([:keepalive_timeout])
      ensure
        held&.each_value { |queue| queue.enqueue(true) }
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end

    it "retires at once when the PING write fails" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        held = holds
        invocations = []
        peer = keepalive_peer(task, bound.sockets.first, held, invocations)
        origin = origin_for(bound)
        events = []
        transport = recording_transport(origin, events, connection_policy: keepalive_policy)
        clock = install_clock(transport)
        long = task.async(finished: false) { transport.call(request("#{origin}/long")) }
        wait_until(task, "long call invoked") { invocations == ["/long"] }
        resource = resources(transport).first
        broken = gate(resource, Protocol::HTTP2::PingFrame::TYPE)
        broken.define_singleton_method(:write) do |buffer|
          raise Errno::EPIPE if buffer.to_s.getbyte(3) == Protocol::HTTP2::PingFrame::TYPE

          super(buffer)
        end
        await_due(task, clock, 10)
        clock.advance(10)
        expect { long.wait }.to unavailable_error
        expect([resource.finished?, clock.now, peer.ping_payloads]).to eq([true, 10, []])
        expect(closed_outcomes(events)).to eq([:keepalive_timeout])
      ensure
        held&.each_value { |queue| queue.enqueue(true) }
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end

    it "does not accept a frame read while the PING waits behind a held frame" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        held = holds
        invocations = []
        peer = keepalive_peer(task, bound.sockets.first, held, invocations)
        origin = origin_for(bound)
        events = []
        # Static windows: BDP PINGs would join the exact PING list asserted here.
        transport = recording_transport(origin, events, connection_policy: keepalive_policy,
                                                        dynamic_receive_windows: false)
        clock = install_clock(transport)
        expect(transport.call(request("#{origin}/warm")).body).to eq("/warm")
        resource = resources(transport).first
        data = gate(resource, Protocol::HTTP2::DataFrame::TYPE)
        outgoing = ConnectRPC::StreamRequest.new(url: URI("#{origin}/long-upload"), http_method: "POST",
                                                 headers: {})
        upload = task.async(finished: false) do
          transport.open_stream(outgoing) do |exchange|
            exchange.write("x" * 4096)
            exchange.response_head
          end
        end
        data.started.dequeue
        await_due(task, clock, 10)
        clock.advance(10)
        wait_until(task, "PING writer waits behind the held DATA") { guard_waiters(resource.connection) == 1 }
        await_due(task, clock, 15)
        # Read before the PING was flushed; its own reply waits behind the guard.
        peer.connections.first.send_ping("preflush")
        wait_until(task, "reader's reply waits behind the held DATA") { guard_waiters(resource.connection) == 2 }
        clock.advance(5)
        expect { upload.wait }.to unavailable_error
        expect([resource.finished?, peer.ping_payloads]).to eq([true, []])
        expect(closed_outcomes(events)).to eq([:keepalive_timeout])
      ensure
        data&.release&.enqueue(true)
        held&.each_value { |queue| queue.enqueue(true) }
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end
  end

  describe "maintenance ownership" do
    it "stops a dormant monitor and drains gracefully at close" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        peer = keepalive_peer(task, bound.sockets.first, holds, [])
        origin = origin_for(bound)
        events = []
        transport = recording_transport(origin, events, connection_policy: keepalive_policy)
        clock = install_clock(transport)
        expect(transport.call(request("#{origin}/warm")).body).to eq("/warm")
        resource = resources(transport).first
        monitor = monitor_task(resource)
        await_due(task, clock, 10)
        clock.advance(10)
        wait_until(task, "monitor dormant") { clock.sleepers.empty? }
        expect(resource).to be_empty
        expect(transport.close(grace: 5)).to be(true)
        expect(monitor).to be_finished
        expect(peer.goaways).to eq([[0, 0, Protocol::HTTP2::Error::NO_ERROR, ""]])
        expect(closed_outcomes(events)).to eq([])
      ensure
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end

    it "stops a monitor whose PING writer is blocked before the graceful drain waits" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        reset = Async::Queue.new
        peer = ConnectionPeer::RecordingServer.new(task, bound.sockets.first) do |incoming|
          if incoming.path == "/stream"
            reset.dequeue
            incoming.stream.send_reset_stream(Protocol::HTTP2::Error::CANCEL)
          else
            incoming.send_response(Protocol::HTTP::Response[200, {}, [incoming.path]])
          end
        rescue Protocol::HTTP2::Error, IOError
          nil
        end
        origin = origin_for(bound)
        events = []
        # Static windows: BDP PINGs would join the exact PING list asserted here.
        transport = recording_transport(origin, events, connection_policy: keepalive_policy,
                                                        dynamic_receive_windows: false)
        clock = install_clock(transport)
        expect(transport.call(request("#{origin}/warm")).body).to eq("/warm")
        resource = resources(transport).first
        monitor = monitor_task(resource)
        data = gate(resource, Protocol::HTTP2::DataFrame::TYPE)
        outgoing = ConnectRPC::StreamRequest.new(url: URI("#{origin}/stream"), http_method: "POST", headers: {})
        proceed = Async::Queue.new
        call = task.async(finished: false) do
          transport.open_stream(outgoing) do |exchange|
            exchange.write("x" * 4096)
            proceed.dequeue
          end
        end
        data.started.dequeue
        stream = transport.instance_variable_get(:@calls).keys.first.fetch(:response).stream
        reset.enqueue(true)
        wait_until(task, "peer reset observed while DATA is held") { stream.closed? }
        proceed.enqueue(true)
        call.wait
        await_due(task, clock, 10)
        clock.advance(10)
        wait_until(task, "PING writer waits behind the held DATA") { guard_waiters(resource.connection) == 1 }
        closer = task.async(finished: false) { transport.close(grace: 5) }
        wait_until(task, "monitor stopped while the frame is still held") { monitor.finished? }
        expect([data.written?, resource.finished?]).to eq([false, false])
        data.release.enqueue(true)
        expect(closer.wait).to be(true)
        expect(data).to be_written
        expect(peer.ping_payloads).to be_empty
        expect(closed_outcomes(events)).to eq([])
      ensure
        data&.release&.enqueue(true)
        reset&.enqueue(true)
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end
  end

  describe "connection_closed events" do
    it "reports a peer EOF that wins over a pending keepalive deadline once" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        held = holds
        invocations = []
        peer = keepalive_peer(task, bound.sockets.first, held, invocations)
        peer.ack_pings = false
        origin = origin_for(bound)
        events = []
        transport = recording_transport(origin, events, connection_policy: keepalive_policy)
        clock = install_clock(transport)
        long = task.async(finished: false) { transport.call(request("#{origin}/long")) }
        wait_until(task, "long call invoked") { invocations == ["/long"] }
        resource = resources(transport).first
        await_due(task, clock, 10)
        clock.advance(10)
        await_due(task, clock, 15)
        AsyncHelpers::INTERNAL.raw_close(peer.raws.first)
        expect { long.wait }.to unavailable_error
        expect(resource).to be_finished
        clock.advance(5)
        expect(closed_outcomes(events)).to eq([:closed])
      ensure
        held&.each_value { |queue| queue.enqueue(true) }
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end

    it "reports a drained GOAWAY and a drain timeout, and survives an interrupted observer" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        held = holds
        invocations = []
        peer = keepalive_peer(task, bound.sockets.first, held, invocations)
        origin = origin_for(bound)
        events = []
        transport = nil
        observer = lambda do |event|
          events << event
          next unless event.type == :connection_closed

          # Reentry raises inside the observer, which the manager ignores.
          transport.call(request("#{origin}/reentry"))
        end
        policy = ConnectRPC::Async::ConnectionPolicy.new(drain_timeout: 5)
        transport = ConnectRPC::Async::Transport.new(origin: origin, connection_policy: policy,
                                                     connection_observer: observer)
        clock = install_clock(transport)
        long = task.async(finished: false) { transport.call(request("#{origin}/long-a")) }
        wait_until(task, "first call invoked") { invocations == ["/long-a"] }
        first = resources(transport).first
        goaway(peer.connections[0], 1)
        held["/long-a"].enqueue(true)
        expect(long.wait.body).to eq("/long-a")
        wait_until(task, "drained connection closed") { first.raw.closed? }
        expect(closed_outcomes(events)).to eq([:goaway])
        long = task.async(finished: false) { transport.call(request("#{origin}/long-b")) }
        wait_until(task, "second call invoked") { invocations.include?("/long-b") }
        goaway(peer.connections[1], 3)
        wait_until(task, "drain timer armed") { clock.sleeper(5) }
        clock.sleeper(5).fire
        expect { long.wait }.to unavailable_error
        expect(closed_outcomes(events)).to eq(%i[goaway drain_timeout])
        expect(invocations).not_to include("/reentry")
        expect(transport.call(request("#{origin}/after")).body).to eq("/after")
        expect(transport.close(grace: 5)).to be(true)
        # Transport close reports no connection_closed.
        expect(closed_outcomes(events)).to eq(%i[goaway drain_timeout])
      ensure
        held&.each_value { |queue| queue.enqueue(true) }
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end
  end

  it "doubles later connections' interval from the one a too_many_pings GOAWAY closed" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      held = holds
      invocations = []
      settings = { Protocol::HTTP2::Settings::MAXIMUM_CONCURRENT_STREAMS => 1 }
      peer = keepalive_peer(task, bound.sockets.first, held, invocations, settings: settings)
      origin = origin_for(bound)
      events = []
      transport = recording_transport(origin, events, connection_limit: 2, connection_policy: keepalive_policy)
      clock = install_clock(transport)
      calls = %w[/long-a /long-b].map do |path|
        call = task.async(finished: false) { transport.call(request("#{origin}#{path}")) }
        wait_until(task, "#{path} invoked") { invocations.include?(path) }
        call
      end
      wait_until(task, "both monitors sleeping") { clock.sleepers.count { |sleeper| sleeper.due == 10 } == 2 }
      clock.advance(10)
      wait_until(task, "both connections pinged") { peer.ping_payloads.size == 2 }
      # Both offending connections used 10 seconds, so later ones use 20, not 40.
      [0, 1].each do |index|
        goaway(peer.connections[index], 1, Protocol::HTTP2::Error::ENHANCE_YOUR_CALM, "too_many_pings")
      end
      calls.each { |call| expect { call.wait }.to unavailable_error }
      expect(closed_outcomes(events)).to eq(%i[goaway goaway])
      third = task.async(finished: false) { transport.call(request("#{origin}/long-c")) }
      wait_until(task, "third call invoked") { invocations.include?("/long-c") }
      replacement = resources(transport).last
      expect(replacement.keepalive.time).to eq(20)
      await_due(task, clock, 30)
      clock.advance(10)
      expect(peer.ping_payloads(2)).to be_empty
      clock.advance(10)
      wait_until(task, "doubled-interval PING") { peer.ping_payloads(2) == [payload(1)] }
      held["/long-c"].enqueue(true)
      expect(third.wait.body).to eq("/long-c")
    ensure
      held&.each_value { |queue| queue.enqueue(true) }
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end
end
