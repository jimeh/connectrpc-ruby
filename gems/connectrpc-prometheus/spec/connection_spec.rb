# frozen_string_literal: true

RSpec.describe "Async client connection metrics" do
  def state(transport, name) = value(:grpc_client_connection_state, transport: transport, state: name)

  it "records a refused attempt, its transient failure state and the state transitions" do
    reactor do |task|
      reserved = refusing_socket
      origin = "http://127.0.0.1:#{reserved.local_address.ip_port}"
      observer = metrics.connection_observer(transport: "refused")
      expect(state("refused", "idle")).to eq(1)
      transport = ConnectRPC::Async::Transport.new(origin: origin, connection_observer: observer,
                                                   connection_policy: ConnectRPC::Async::ConnectionPolicy.new)
      expect { transport.call(request("#{origin}/x", body: "abc")) }.to unavailable_error
      wait_until(task, "refused attempt") { total(:grpc_client_connection_attempts_total, transport: "refused") == 1 }
      expect(value(:grpc_client_connection_attempts_total, transport: "refused", outcome: "refused")).to eq(1)
      expect(value(:grpc_client_connection_attempt_seconds, transport: "refused").fetch("+Inf")).to eq(1)
      expect(value(:grpc_client_connection_state_changes_total, transport: "refused", state: "connecting")).to eq(1)
      expect(state("refused", "transient_failure")).to eq(1)
      expect(%w[idle connecting ready closing closed].map { |name| state("refused", name) }).to all(eq(0))
    ensure
      transport&.close(grace: 0)
      reserved&.close
    end
  end

  it "records a ready attempt against a local host and closing on transport close" do
    reactor do |task|
      host = ConnectRPC::Async::Host.new(server: server).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin,
                                                   connection_observer: metrics.connection_observer(transport: "local"))
      expect(client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "ready")).message.name).to eq("ready")
      expect(value(:grpc_client_connection_attempts_total, transport: "local", outcome: "ready")).to eq(1)
      expect(state("local", "ready")).to eq(1)
      transport.close(grace: 0)
      wait_until(task, "closed state") { state("local", "closed") == 1 }
      expect(%w[idle connecting ready transient_failure closing].map { |name| state("local", name) }).to all(eq(0))
    ensure
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  it "counts admission rejections" do
    reactor do
      reserved = refusing_socket
      origin = "http://127.0.0.1:#{reserved.local_address.ip_port}"
      policy = ConnectRPC::Async::ConnectionPolicy.new(max_pending_bytes: 4)
      observer = metrics.connection_observer(transport: "budget")
      transport = ConnectRPC::Async::Transport.new(origin: origin, connection_policy: policy,
                                                   connection_observer: observer)
      expect { transport.call(request("#{origin}/x", body: "too large")) }.to unavailable_error
      expect(value(:grpc_client_admission_rejected_total, transport: "budget")).to eq(1)
      expect(total(:grpc_client_connection_attempts_total, transport: "budget")).to eq(0)
    ensure
      transport&.close(grace: 0)
      reserved&.close
    end
  end
end
