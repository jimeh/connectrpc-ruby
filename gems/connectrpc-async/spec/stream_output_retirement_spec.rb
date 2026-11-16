# frozen_string_literal: true

# A peer reset stops a stream's request output once, deferred while that
# output is inside a frame write. Unlike a Connect unary call, whose output
# is a child of the call task, a stream's output belongs to its completed
# dispatch task, which cleanup never cancels. However the call ends, the
# frame must finish before the connection writes anything else; a second
# cancellation would cut it short on a connection that stays reusable.
RSpec.describe "HTTP/2 stream output after a peer reset" do
  def origin_for(bound) = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
  def operation(transport) = transport.instance_variable_get(:@calls).keys.first

  # Holds the next DATA frame after its first five bytes, so the frame is
  # incomplete on the wire until the example releases it.
  def gate_data(transport)
    resource = transport.instance_variable_get(:@resources).first
    gate = FrameGate.new(resource.socket, frame_type: Protocol::HTTP2::DataFrame::TYPE)
    resource.connection.stream.instance_variable_set(:@io, gate)
    gate.armed = true
    [resource, gate]
  end

  # Resets /stream once the example releases it; answers everything else.
  def resetting_peer(task, listener, reset)
    ConnectionPeer::RecordingServer.new(task, listener) do |incoming|
      if incoming.path == "/stream"
        reset.dequeue
        incoming.stream.send_reset_stream(Protocol::HTTP2::Error::CANCEL)
      else
        incoming.send_response(Protocol::HTTP::Response[200, {}, [incoming.path]])
      end
    rescue Protocol::HTTP2::Error, IOError
      nil
    end
  end

  # Ends the stream call after the reset: by leaving the block, or by ending a
  # pending exchange operation that waits on the held output.
  endings = {
    return: nil,
    raise: RuntimeError,
    abort: nil,
    deadline: "deadline_exceeded",
    cancellation: "canceled",
    stop: nil
  }.freeze

  %i[connect grpc].product(endings.keys).each do |protocol, ending|
    it "finishes a reset #{protocol} stream's held DATA frame before reuse (#{ending})" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        reset = Async::Queue.new
        peer = resetting_peer(task, bound.sockets.first, reset)
        origin = origin_for(bound)
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        expect(transport.call(request("#{origin}/warm")).body).to eq("/warm")
        resource, gate = gate_data(transport)
        source = ConnectRPC::CancellationSource.new
        # The deadline variant measures real elapsed time: its expiry while
        # finish waits on the held output is the boundary under test.
        outgoing = ConnectRPC::StreamRequest.new(
          url: URI("#{origin}/stream"), http_method: "POST", headers: {},
          deadline: ending == :deadline ? ConnectRPC::Deadline.new(2) : nil,
          cancellation: ending == :cancellation ? source.token : nil
        )
        proceed = Async::Queue.new
        call = task.async(finished: false) do
          method = protocol == :grpc ? :open_grpc_stream : :open_stream
          transport.public_send(method, outgoing) do |exchange|
            exchange.write("x" * 4096)
            proceed.dequeue
            case ending
            when :abort then exchange.abort
            when :raise then raise "block failed"
            when :deadline, :cancellation, :stop then exchange.finish
            end
          end
        end
        gate.started.dequeue
        output = gate.writer
        current = operation(transport)
        stream = current.fetch(:response).stream
        expect(stream.output_task).to equal(output)
        reset.enqueue(true)
        wait_until(task, "peer reset observed while DATA is held") { stream.closed? }
        proceed.enqueue(true)
        if %i[cancellation stop].include?(ending)
          wait_until(task, "finish waits on the held output") { current.fetch(:tasks).keys.one?(&:alive?) }
          ending == :cancellation ? source.cancel : call.stop
        end
        case endings.fetch(ending)
        when nil then call.wait
        when String
          expect { call.wait }.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq(endings[ending]) }
        else expect { call.wait }.to raise_error(endings[ending])
        end
        # The call returned its lease without cutting the held frame.
        expect([output.finished?, resource.finished?, lease_usage(transport)]).to eq([false, false, 0])
        sibling = task.async(finished: false) { transport.call(request("#{origin}/after")).body }
        wait_until(task, "sibling waits behind the held frame") { guard_waiters(resource.connection) == 1 }
        gate.release.enqueue(true)
        expect(sibling.wait).to eq("/after")
        expect(gate).to be_written
        expect([output.finished?, resource.finished?, peer.accepted]).to eq([true, false, 1])
        expect(lease_usage(transport)).to eq(0)
      ensure
        gate&.release&.enqueue(true)
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end
  end
end
