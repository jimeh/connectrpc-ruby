# frozen_string_literal: true

require_relative "support/retirement_helpers"

RSpec.describe "HTTP/2 dispatch-guard reroute" do
  def close_at_guard(connection)
    # A source-coupled race fixture closes after acquisition, at the dispatch
    # guard, while leaving the real manager, dispatch and socket logic intact.
    connection.define_singleton_method(:synchronize) do |&block|
      close! unless @closed_at_dispatch
      @closed_at_dispatch = true
      super(&block)
    end
  end

  def stream_method
    ConnectRPC::ServerStreamMethod.new(service: "test.Reroute", name: "Watch",
                                       request_type: AsyncHelpers::MESSAGE, response_type: AsyncHelpers::MESSAGE)
  end

  def outcome_errors(errors)
    observer = Object.new
    observer.define_singleton_method(:processing_finished) { |outcome| errors << outcome.error }
    subscriber = Object.new
    subscriber.define_singleton_method(:start) { |_| observer }
    ConnectRPC::Instrumentation.new(subscribers: [subscriber])
  end

  it "reroutes an unsent stream to a new connection with one invocation" do
    reactor do
      method = stream_method
      observed = []
      target = ConnectRPC::Server.new(
        services: [ConnectRPC::Service.new(name: method.service, methods: [method])],
        binding: ConnectRPC::Bindings::GoogleProtobuf.new,
        handlers: { method.path => lambda { |message, writer, _|
          observed << message.name
          writer.send_message(message)
        } }
      )
      host = ConnectRPC::Async::Host.new(server: target).start
      origin = "http://127.0.0.1:#{host.port}"
      events = []
      transport = recording_transport(origin, events)
      core = ConnectRPC::Client.new(base_url: origin, transport: transport,
                                    binding: ConnectRPC::Bindings::GoogleProtobuf.new)
      core.server_stream(method, AsyncHelpers::MESSAGE.new(name: "warm")) { |reader| reader.each { |_| nil } }
      record = transport.instance_variable_get(:@resources).first
      close_at_guard(record.connection)
      core.server_stream(method, AsyncHelpers::MESSAGE.new(name: "once")) do |reader|
        expect(reader.receive.name).to eq("once")
        expect(reader.receive).to be_nil
      end
      expect(observed).to eq(%w[warm once])
      expect(attempt_outcomes(events)).to eq(%i[ready ready])
      expect(record.raw).to be_closed
      expect(lease_usage(transport)).to eq(0)
    ensure
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  %i[before after].each do |point|
    it "fails the call without looping when the reroute release fails #{point} accounting" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        peer = ConnectionPeer::RecordingServer.new(task, bound.sockets.first)
        origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
        events = []
        transport = recording_transport(origin, events)
        expect(transport.call(request("#{origin}/warm")).body).to eq("ok")
        record = transport.instance_variable_get(:@resources).first
        close_at_guard(record.connection)
        original = IOError.new("release failure")
        manager = connection_manager(transport)
        releases = 0
        allow(manager).to receive(:release).and_wrap_original do |release, connection|
          releases += 1
          raise original if point == :before

          release.call(connection)
          raise original
        end
        errors = []
        typed = client(transport, origin, instrumentation: outcome_errors(errors))
        expect { typed.echo(AsyncHelpers::MESSAGE.new(name: "once")) }.to unavailable_error
        expect(errors).to eq([original])
        expect(releases).to eq(1)
        expect(record).to be_finished
        expect(manager.connections).to be_empty
        expect(lease_usage(transport)).to eq(0)
        expect(attempt_outcomes(events)).to eq([:ready])
        expect(peer.headers_frames.size).to eq(1)
      ensure
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end
  end

  it "releases an unsent lease once when the call is cancelled while that release blocks" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      peer = ConnectionPeer::RecordingServer.new(task, bound.sockets.first)
      origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      expect(transport.call(request("#{origin}/warm")).body).to eq("ok")
      record = transport.instance_variable_get(:@resources).first
      gate = RetirementHelpers::CleanupGate.new
      # A retained task whose unwind blocks retirement, and so the release.
      record.track(transport.instance_variable_get(:@lifetime).async(finished: false) do
        Async::Condition.new.wait
      ensure
        gate.hold
      end)
      close_at_guard(record.connection)
      manager = connection_manager(transport)
      releases = 0
      allow(manager).to receive(:release).and_wrap_original do |release, connection|
        releases += 1
        release.call(connection)
      end
      source = ConnectRPC::CancellationSource.new
      outgoing = request("#{origin}/cancelled")
      outgoing.cancellation = source.token
      call = task.async(finished: false) { transport.call(outgoing) }
      gate.entered.dequeue
      expect(manager.connections).to be_empty
      expect(lease_usage(transport)).to eq(0)
      source.cancel
      expect { call.wait }.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("canceled") }
      gate.release
      wait_until(task, "old resource retired") { record.send(:retirement_completed?) }
      expect(releases).to eq(1)
      expect(lease_usage(transport)).to eq(0)
      expect(peer.headers_frames.size).to eq(1)
      expect(peer.accepted).to eq(1)
      expect(transport.instance_variable_get(:@calls)).to be_empty
    ensure
      gate&.release
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end
end

RSpec.describe "HTTP/2 no replay of possibly delivered requests" do
  def probe_peer(task, listener, failure)
    ConnectionPeer::RecordingServer.new(task, listener) do |incoming|
      if incoming.path.start_with?("/probe")
        case failure
        when :reset then incoming.stream.send_reset_stream(Protocol::HTTP2::Error::INTERNAL_ERROR)
        when :partial then incoming.stream.connection.close
        else
          incoming.body&.join
          incoming.stream.connection.close
        end
      else
        incoming.send_response(Protocol::HTTP::Response[200, {}, ["ok"]])
      end
    end
  end

  [["POST", :reset], ["POST", :partial], ["POST", :complete], ["GET", :reset],
   ["GET", :complete]].each do |verb, failure|
    it "sends a #{verb} once after #{failure} failure and reconnects only for a separate call" do
      reactor do |task|
        bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
        peer = probe_peer(task, bound.sockets.first, failure)
        origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
        events = []
        transport = recording_transport(origin, events)
        probe = if verb == "GET"
                  request("#{origin}/probe?m=abc", method: "GET")
                else
                  request("#{origin}/probe", body: "x" * 4096)
                end
        expect { transport.call(probe) }.to unavailable_error
        expect(peer.headers_frames.size).to eq(1)
        expect(peer.requests.size).to eq(1)
        expect(attempt_outcomes(events)).to eq([:ready])
        expect(peer.accepted).to eq(1)
        expect(transport.call(request("#{origin}/separate")).body).to eq("ok")
        expect(peer.accepted).to eq(failure == :reset ? 1 : 2)
        expect(peer.headers_frames.size).to eq(2)
      ensure
        transport&.close(grace: 0)
        peer&.stop
        bound&.close
      end
    end
  end

  it "never retransmits a request whose HEADERS frame was only partly written" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      peer = ConnectionPeer::RecordingServer.new(task, bound.sockets.first)
      origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
      events = []
      transport = recording_transport(origin, events)
      expect(transport.call(request("#{origin}/warm")).body).to eq("ok")
      resource = transport.instance_variable_get(:@resources).first
      gate = FrameGate.new(resource.socket, frame_type: Protocol::HTTP2::HeadersFrame::TYPE)
      resource.connection.stream.instance_variable_set(:@io, gate)
      gate.armed = true
      call = task.async(finished: false) { transport.call(request("#{origin}/partial")) }
      gate.started.dequeue
      peer.connections.each(&:close)
      gate.release.enqueue(true)
      expect { call.wait }.to unavailable_error
      expect(peer.headers_frames.size).to eq(1)
      expect(peer.accepted).to eq(1)
      expect(attempt_outcomes(events)).to eq([:ready])
      expect(transport.call(request("#{origin}/separate")).body).to eq("ok")
      expect(peer.accepted).to eq(2)
      expect(peer.requests.size).to eq(2)
    ensure
      gate&.release&.enqueue(true)
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end
end
