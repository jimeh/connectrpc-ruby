# frozen_string_literal: true

RSpec.describe "Health over the Async HTTP/2 host" do
  let(:health) { ConnectRPC::Health::Registry.new }
  let(:request) { HealthHelpers::REQUEST }

  def with_host(protocol: :auto, **)
    reactor do |task|
      host = ConnectRPC::Async::Host.new(server: health_server(health, protocol: protocol), **).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      yield task, host, transport, origin
    ensure
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  # Starts a Watch in its own task; statuses arrive on the returned queue and
  # the task's value is :ok after a normal end or the client's error.
  def start_watch(task, client, service, **)
    received = Async::Queue.new
    call = task.async(finished: false) do
      client.watch(request.new(service: service), timeout: nil, **) do |reader|
        reader.each { |message| received.enqueue(message.status) }
      end
      :ok
    rescue ConnectRPC::Error => e
      e
    end
    [received, call]
  end

  %i[connect grpc].each do |protocol|
    it "serves Check, List and Watch to the generated #{protocol} client" do
      health.set("demo.v1.Demo", :serving)
      with_host do |task, _host, transport, origin|
        client = health_client(transport, origin, protocol: protocol)
        expect(client.check(request.new(service: "")).message.status).to eq(:SERVING)
        expect { client.check(request.new(service: "missing")) }
          .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("not_found") }
        statuses = client.list(Grpc::Health::V1::HealthListRequest.new).message.statuses
        expect(statuses.keys.sort).to eq(["", "demo.v1.Demo"])
        received, call = start_watch(task, client, "demo.v1.Demo")
        expect(received.dequeue).to eq(:SERVING)
        health.set("demo.v1.Demo", :not_serving)
        expect(received.dequeue).to eq(:NOT_SERVING)
        health.shutdown
        expect(call.wait).to eq(:ok)
        expect(received).to be_empty
        expect(health.watcher_count).to eq(0)
      end
    end
  end

  it "wakes a reactor watcher from a plain thread's set" do
    with_host do |task, _host, transport, origin|
      received, call = start_watch(task, health_client(transport, origin), "later")
      expect(received.dequeue).to eq(:SERVICE_UNKNOWN)
      Thread.new { health.set("later", :serving) }.join
      expect(received.dequeue).to eq(:SERVING)
      Thread.new { health.shutdown }.join
      expect(received.dequeue).to eq(:NOT_SERVING)
      expect(call.wait).to eq(:ok)
    end
  end

  it "ends an idle watcher and frees its slot when the peer cancels" do
    with_host do |task, _host, transport, origin|
      source = ConnectRPC::CancellationSource.new
      received, call = start_watch(task, health_client(transport, origin), "", cancellation: source.token)
      # Wire progress: the initial status reached the client before the reset.
      expect(received.dequeue).to eq(:SERVING)
      expect(health.watcher_count).to eq(1)
      source.cancel
      expect(call.wait).to be_a(ConnectRPC::Error).and(have_attributes(code: "canceled"))
      wait_until(task, "watcher release after peer CANCEL") { health.watcher_count.zero? }
    end
  end

  it "releases a watcher when the H2 connection is lost" do
    with_host do |task, _host, transport, origin|
      received, call = start_watch(task, health_client(transport, origin), "")
      expect(received.dequeue).to eq(:SERVING)
      transport.close(grace: 0)
      expect(call.wait).to be_a(ConnectRPC::Error)
      wait_until(task, "watcher release after connection loss") { health.watcher_count.zero? }
    end
  end

  it "releases a watcher when the host closes without grace" do
    with_host do |task, host, transport, origin|
      received, call = start_watch(task, health_client(transport, origin), "")
      expect(received.dequeue).to eq(:SERVING)
      host.close(grace: 0)
      expect(call.wait).to be_a(ConnectRPC::Error)
      expect(health.watcher_count).to eq(0)
    end
  end

  it "releases a watcher when grace expires without a registry shutdown" do
    with_host do |task, host, transport, origin|
      received, call = start_watch(task, health_client(transport, origin), "")
      expect(received.dequeue).to eq(:SERVING)
      host.close(grace: 0.2)
      expect(call.wait).to be_a(ConnectRPC::Error)
      expect(health.watcher_count).to eq(0)
    end
  end

  it "drains well before grace when the registry shuts down first" do
    with_host do |task, host, transport, origin|
      received, call = start_watch(task, health_client(transport, origin), "")
      expect(received.dequeue).to eq(:SERVING)
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      health.shutdown
      expect(host.close(grace: 30)).to be(true)
      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 5
      expect(received.dequeue).to eq(:NOT_SERVING)
      expect(call.wait).to eq(:ok)
      expect(health.watcher_count).to eq(0)
    end
  end

  it "keeps a watcher after a non-CANCEL reset until the next status change" do
    health.set("demo", :serving)
    with_host do |task, host, transport, origin|
      reset = Async::Queue.new
      call = task.async(finished: false) do
        health_client(transport, origin).watch(request.new(service: "demo"), timeout: nil) do |reader|
          reader.receive
          operation = transport.instance_variable_get(:@calls).keys.first
          operation.fetch(:response).stream.send_reset_stream(Protocol::HTTP2::Error::NO_ERROR)
          reset.enqueue(true)
          reader.abandon
        end
      rescue ConnectRPC::Error => e
        e
      end
      reset.dequeue
      call.wait
      resource = host_session(host).instance_variable_get(:@resources).first
      wait_until(task, "server stream closed by the reset") { resource.connection.streams.empty? }
      # The reset is not a cancellation signal, so the watcher stays admitted.
      expect(health.watcher_count).to eq(1)
      health.set("demo", :not_serving)
      wait_until(task, "residual watcher release at the next status change") { health.watcher_count.zero? }
    end
  end
end
