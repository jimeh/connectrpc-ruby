# frozen_string_literal: true

RSpec.describe "Health over the Falcon host" do
  let(:health) { ConnectRPC::Health::Registry.new }
  let(:request) { HealthHelpers::REQUEST }

  def watch_statuses(task, client, service)
    received = Async::Queue.new
    call = task.async(finished: false) do
      client.watch(request.new(service: service), timeout: nil) do |reader|
        reader.each { |message| received.enqueue(message.status) }
      end
      :ok
    end
    [received, call]
  end

  it "serves Check, List and a Connect Watch over HTTP/1.1 until shutdown" do
    health.set("demo.v1.Demo", :serving)
    reactor do |task|
      falcon(server: health_server(health)) do |_host, origin|
        transport = ConnectRPC::Async::HTTP1Transport.new(origin: origin)
        client = health_client(transport, origin)
        expect(client.check(request.new(service: "demo.v1.Demo")).message.status).to eq(:SERVING)
        expect(client.list(Grpc::Health::V1::HealthListRequest.new).message.statuses.keys.sort)
          .to eq(["", "demo.v1.Demo"])
        received, call = watch_statuses(task, client, "demo.v1.Demo")
        expect(received.dequeue).to eq(:SERVING)
        health.set("demo.v1.Demo", :not_serving)
        expect(received.dequeue).to eq(:NOT_SERVING)
        health.set("demo.v1.Demo", :serving)
        expect(received.dequeue).to eq(:SERVING)
        health.shutdown
        expect(received.dequeue).to eq(:NOT_SERVING)
        expect(call.wait).to eq(:ok)
        expect(health.watcher_count).to eq(0)
      ensure
        transport&.close(grace: 0)
      end
    end
  end

  it "serves a native gRPC Watch over HTTP/2" do
    reactor do |task|
      falcon(server: health_server(health)) do |_host, origin|
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        client = health_client(transport, origin, protocol: :grpc)
        expect(client.check(request.new(service: "")).message.status).to eq(:SERVING)
        received, call = watch_statuses(task, client, "later")
        expect(received.dequeue).to eq(:SERVICE_UNKNOWN)
        health.set("later", :not_serving)
        expect(received.dequeue).to eq(:NOT_SERVING)
        health.shutdown
        expect(call.wait).to eq(:ok)
        expect(received).to be_empty
      ensure
        transport&.close(grace: 0)
      end
    end
  end
end
