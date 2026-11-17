# frozen_string_literal: true

require "protocol/http/body/writable"

# Retirement closes raw TCP, and Resource#abort marks the resource finished,
# before close! marks the connection closed. A consumer that reads buffered
# input in that window must not attempt a stream WINDOW_UPDATE on it.
RSpec.describe "Input reclaim inside the retirement window" do
  def origin_for(bound) = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"

  # Records [connection closed, resource finished, raw closed] when the
  # stream first reclaims input credit, proving which window the read hit.
  def reclaim_states(stream, &)
    states = []
    trace = TracePoint.new(:call) do |event|
      next unless event.method_id == :reclaim_input && event.self.equal?(stream) && states.empty?

      connection = stream.connection
      states << [connection.closed?, connection.resource.finished?, connection.resource.raw.closed?]
    end
    trace.enable(&)
    states
  end

  it "returns buffered response bytes to a client read inside the window" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      held = Async::Queue.new
      peer = ConnectionPeer::RecordingServer.new(task, bound.sockets.first) do |incoming|
        body = Protocol::HTTP::Body::Writable.new
        incoming.send_response(Protocol::HTTP::Response[200, {}, body])
        body.write("x" * 40_000)
        held.dequeue
      rescue Protocol::HTTP2::Error, IOError
        nil
      end
      origin = origin_for(bound)
      # The 40,000 buffered bytes must limit a 64 KiB window.
      transport = ConnectRPC::Async::Transport.new(origin: origin, stream_receive_window: 65_536)
      outgoing = ConnectRPC::StreamRequest.new(url: URI("#{origin}/stream"), http_method: "POST", headers: {})
      transport.open_stream(outgoing) do |exchange|
        exchange.close_send
        exchange.response_head
        operation = exchange.instance_variable_get(:@operation)
        resource = operation.fetch(:resource)
        stream = operation.fetch(:response).stream
        window = stream.instance_variable_get(:@local_window)
        # Unread input has spent enough stream credit that a read would ask
        # the peer for more.
        wait_until(task, "buffered input limits the stream window") { window.limited? }
        wake = Async::Condition.new
        result = nil
        reader = task.async do
          wake.wait
          result = begin
            exchange.read(16_384).bytesize
          rescue ConnectRPC::Error => e
            e
          end
        end
        states = reclaim_states(stream) do
          # Wake the reader, then start retirement before it can run.
          wake.signal
          resource.abort
          reader.wait
        end
        expect(states).to eq([[false, true, true]])
        expect(result).to eq(16_384)
      end
    ensure
      held&.enqueue(true)
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  # Session retirement closes every raw socket before it aborts each
  # resource, and listener cleanup can yield in between.
  it "returns a buffered request message to a handler read after session raw close and before resource abort" do
    reactor do |task|
      wake = Async::Condition.new
      entered = Async::Queue.new
      outcome = Async::Queue.new
      method = ConnectRPC::BidiStreamMethod.new(service: "direct.Streams", name: "Chat", request_type: String,
                                                response_type: String)
      binding = Object.new
      binding.define_singleton_method(:encode) { |message, **| message }
      binding.define_singleton_method(:decode) { |message, **| message }
      server = ConnectRPC::Server.new(
        services: [ConnectRPC::Service.new(name: method.service, methods: [method])], binding: binding,
        handlers: { method.path => lambda { |duplex, _|
          entered.enqueue(true)
          wake.wait
          outcome.enqueue(begin
            duplex.receive.bytesize
          rescue StandardError => e
            e
          end)
        } }
      )
      host = ConnectRPC::Async::Host.new(server: server).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      outgoing = ConnectRPC::StreamRequest.new(url: URI("#{origin}/direct.Streams/Chat"), http_method: "POST",
                                               headers: { "content-type" => ["application/connect+proto"] })
      session = host.instance_variable_get(:@session)
      states = nil
      transport.open_stream(outgoing) do |exchange|
        entered.dequeue
        # One 40,000-byte enveloped message, sent in frame-sized fragments.
        message = [0, 40_000].pack("CN") + ("x" * 40_000)
        message.bytes.each_slice(16_384) { |slice| exchange.write(slice.pack("C*")) }
        resource = session.instance_variable_get(:@resources).first
        stream = resource.connection.streams.values.first
        window = stream.instance_variable_get(:@local_window)
        wait_until(task, "buffered request limits the stream window") { window.limited? }
        states = reclaim_states(stream) do
          # Wake the handler, then retire the session before it can run.
          wake.signal
          session.abort
        end
      rescue ConnectRPC::Error
        # The client stream fails with its retired connection.
        nil
      end
      expect(states).to eq([[false, false, true]])
      expect(outcome.dequeue).to eq(40_000)
    ensure
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end
end
