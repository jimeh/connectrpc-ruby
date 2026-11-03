# frozen_string_literal: true

RSpec.describe "gRPC blocked frame cleanup" do
  it "uses the one destructive cleanup budget for a stuck DATA frame and fails its sibling without replay" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      names = []
      contexts = []
      target = server(protocol: :grpc, echo: lambda { |message, context|
        names << message.name
        if message.name == "sibling"
          contexts << context
          entered.enqueue(true)
          release.dequeue
        end
        message
      })
      host = ConnectRPC::Async::Host.new(server: target).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      core = ConnectRPC::Client.new(base_url: origin, transport: transport, protocol: :grpc,
                                    binding: ConnectRPC::Bindings::GoogleProtobuf.new)
      expect(core.call(AsyncHelpers::ECHO::ECHO, AsyncHelpers::MESSAGE.new(name: "warm")).message.name).to eq("warm")
      sibling = task.async do
        core.call(AsyncHelpers::ECHO::ECHO, AsyncHelpers::MESSAGE.new(name: "sibling"), timeout: nil)
      rescue ConnectRPC::Error => e
        e
      end
      entered.dequeue
      resource = transport.instance_variable_get(:@resources).first
      gate = FrameGate.new(resource.socket, frame_type: 0)
      resource.connection.stream.instance_variable_set(:@io, gate)
      gate.armed = true
      source = ConnectRPC::CancellationSource.new
      blocked = task.async do
        core.call(AsyncHelpers::ECHO::ECHO, AsyncHelpers::MESSAGE.new(name: "x" * 500_000), timeout: nil,
                                                                                            cancellation: source.token)
      rescue ConnectRPC::Error => e
        e
      end
      gate.started.dequeue
      expect(blocked).not_to be_finished
      source.cancel
      expect(blocked.wait.code).to eq("canceled")
      expect(sibling.wait.code).to eq("unavailable")
      expect(source.token.reason).to eq(:canceled)
      expect(resource.raw).to be_closed
      expect(resource).to be_finished
      expect(names).to eq(%w[warm sibling])
      expect(contexts.first.cancellation_reason).to be_nil
      expect(transport.instance_variable_get(:@calls)).to be_empty
    ensure
      gate&.release&.enqueue(true)
      release&.enqueue(true)
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end
end
