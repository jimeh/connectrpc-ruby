# frozen_string_literal: true

require "delegate"

RSpec.describe "Health Watch through a server stream interceptor on the Async host" do
  let(:health) { ConnectRPC::Health::Registry.new }

  # Records each status the Watch handler sends, then delegates.
  let(:recording) do
    Class.new(SimpleDelegator) do
      def initialize(stream, sent)
        super(stream)
        @sent = sent
      end

      def send_message(message)
        @sent << message.status
        __getobj__.send_message(message)
      end
    end
  end

  %i[connect grpc].each do |protocol|
    it "passes Watch over #{protocol} through the interceptor and its wrapper" do
      health.set("demo", :serving)
      seen = []
      sent = []
      recording = self.recording
      watching = Object.new
      watching.define_singleton_method(:server_stream) do |call, &continuation|
        seen << [call.class, call.method.path, call.method.cardinality, call.protocol, call.request.service]
        call.wrap_stream { |stream| recording.new(stream, sent) }
        continuation.call
        seen << :returned
      end
      reactor do |task|
        host = ConnectRPC::Async::Host.new(server: health_server(health, interceptors: [watching])).start
        origin = "http://127.0.0.1:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        received = Async::Queue.new
        call = task.async(finished: false) do
          health_client(transport, origin, protocol: protocol)
            .watch(HealthHelpers::REQUEST.new(service: "demo"), timeout: nil) do |reader|
              reader.each { |message| received.enqueue(message.status) }
            end
          :ok
        end
        expect(received.dequeue).to eq(:SERVING)
        health.set("demo", :not_serving)
        expect(received.dequeue).to eq(:NOT_SERVING)
        health.shutdown
        expect(call.wait).to eq(:ok)
        wait_until(task, "the interceptor to return") { seen.last == :returned }
      ensure
        transport&.close(grace: 0)
        host&.close(grace: 0)
      end
      expect(seen).to eq([[ConnectRPC::ServerStreamCall::ServerStreaming, HealthHelpers::HEALTH::WATCH.path,
                           :server_stream, protocol, "demo"], :returned])
      expect(sent).to eq(%i[SERVING NOT_SERVING])
      expect(health.watcher_count).to eq(0)
    end
  end
end
