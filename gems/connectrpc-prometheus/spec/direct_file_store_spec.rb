# frozen_string_literal: true

RSpec.describe "Prometheus DirectFileStore across forked workers" do
  around do |example|
    original = Prometheus::Client.config.data_store
    Dir.mktmpdir("connectrpc-prometheus-store-") do |directory|
      Prometheus::Client.config.data_store = Prometheus::Client::DataStores::DirectFileStore.new(dir: directory)
      example.run
    ensure
      Prometheus::Client.config.data_store = original
    end
  end

  let(:event) { Data.define(:type, :state, :previous_state, :outcome, :duration) }

  it "sums RPC counters and connection state gauges written by separate processes" do
    skip "fork is unavailable" unless Process.respond_to?(:fork)

    metrics # Registered before fork, as in Puma cluster or Unicorn masters.
    binding = ConnectRPC::Bindings::GoogleProtobuf.new
    service = Example::V1::EchoService
    pids = %w[first second].map do |transport_name|
      fork do
        server = ConnectRPC::Server.new(services: [service::SERVICE], binding: binding, instrumentation: bus,
                                        handlers: service.handlers(echo: ->(message, _) { message }, fail: ->(*) {}))
        core = ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding,
                                      transport: ConnectRPC::Testing::Transport.new(server: server))
        service::Client.new(client: core).echo(Example::V1::EchoMessage.new(name: transport_name))
        observer = metrics.connection_observer(transport: "shared")
        observer.call(event.new(type: :state_changed, state: :ready, previous_state: :idle, outcome: nil,
                                duration: nil))
        exit!(0)
      rescue Exception => e # rubocop:disable Lint/RescueException -- report any child failure through its status
        warn e.full_message
        exit!(1)
      end
    end
    statuses = pids.map { |pid| Process.wait2(pid).last }
    expect(statuses).to all(be_success)
    expect(value(:grpc_server_started_total, MetricsHelpers::ECHO_LABELS)).to eq(2.0)
    expect(value(:grpc_server_handled_total, MetricsHelpers::ECHO_LABELS.merge(grpc_code: "OK"))).to eq(2.0)
    expect(value(:grpc_server_handling_seconds, MetricsHelpers::ECHO_LABELS).fetch("+Inf")).to eq(2.0)
    expect(value(:grpc_client_connection_state, transport: "shared", state: "ready")).to eq(2.0)
    expect(value(:grpc_client_connection_state, transport: "shared", state: "idle")).to eq(0.0)
    # DirectFileStore sorts label names in exposition.
    expect(exposition).to include('grpc_client_connection_state{state="ready",transport="shared"} 2.0')
    expect(exposition).not_to include("pid=")
  end
end
