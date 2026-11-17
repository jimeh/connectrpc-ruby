# frozen_string_literal: true

require "timeout"

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

  let(:event) { Data.define(:type, :state, :previous_state, :outcome, :duration, :active, :draining, :pending_calls) }

  def counts_event(active, draining, pending_calls)
    event.new(type: :counts_changed, state: :ready, previous_state: nil, outcome: nil, duration: nil,
              active: active, draining: draining, pending_calls: pending_calls)
  end

  def gauges(transport)
    [value(:grpc_client_connections, transport: transport, state: "active"),
     value(:grpc_client_connections, transport: transport, state: "draining"),
     value(:grpc_client_pending_calls, transport: transport)]
  end

  # Runs the block in a forked worker; failures surface through its exit status.
  def worker
    fork do
      yield
      exit!(0)
    rescue Exception => e # rubocop:disable Lint/RescueException -- report any child failure through its status
      warn e.full_message
      exit!(1)
    end
  end

  def worker_seconds = 10

  # Reads one byte, or nil at EOF, failing after a deadline.
  def read_within(reader)
    raise "worker did not signal within #{worker_seconds}s" unless reader.wait_readable(worker_seconds)

    reader.read(1)
  end

  # Waits for a worker's exit status under a deadline and forgets it once reaped.
  def reap(pending, pid)
    status = Timeout.timeout(worker_seconds) { Process.wait2(pid).last }
    pending.delete(pid)
    status
  end

  # Kills and reaps workers a failed example left running.
  def abandon(pending)
    pending&.each do |pid|
      Process.kill(:KILL, pid)
      Process.wait(pid)
    rescue Errno::ESRCH, Errno::ECHILD
      nil
    end
  end

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
                                duration: nil, active: 0, draining: 0, pending_calls: 0))
        exit!(0)
      rescue Exception => e # rubocop:disable Lint/RescueException -- report any child failure through its status
        warn e.full_message
        exit!(1)
      end
    end
    statuses = pids.dup.map { |pid| reap(pids, pid) }
    expect(statuses).to all(be_success)
    expect(value(:grpc_server_started_total, MetricsHelpers::ECHO_LABELS)).to eq(2.0)
    expect(value(:grpc_server_handled_total, MetricsHelpers::ECHO_LABELS.merge(grpc_code: "OK"))).to eq(2.0)
    expect(value(:grpc_server_handling_seconds, MetricsHelpers::ECHO_LABELS).fetch("+Inf")).to eq(2.0)
    expect(value(:grpc_client_connection_state, transport: "shared", state: "ready")).to eq(2.0)
    expect(value(:grpc_client_connection_state, transport: "shared", state: "idle")).to eq(0.0)
    # DirectFileStore sorts label names in exposition.
    expect(exposition).to include('grpc_client_connection_state{state="ready",transport="shared"} 2.0')
    expect(exposition).not_to include("pid=")
  ensure
    abandon(pids)
  end

  it "sums connection count gauges across workers and follows a worker back to zero" do
    skip "fork is unavailable" unless Process.respond_to?(:fork)

    metrics
    ready_reader, ready_writer = IO.pipe
    zero_reader, zero_writer = IO.pipe
    busy = worker do
      [ready_reader, zero_writer].each(&:close)
      observer = metrics.connection_observer(transport: "shared")
      observer.call(counts_event(2, 1, 3))
      ready_writer.write("1")
      zero_reader.read(1)
      observer.call(counts_event(0, 0, 0))
    end
    quiet = worker { metrics.connection_observer(transport: "shared").call(counts_event(1, 0, 1)) }
    pending = [busy, quiet]
    # Without the parent's copies, a worker that dies early ends the read with EOF.
    [ready_writer, zero_reader].each(&:close)
    expect(reap(pending, quiet)).to be_success
    expect(read_within(ready_reader)).to eq("1")
    expect(gauges("shared")).to eq([3.0, 1.0, 4.0])
    zero_writer.write("1")
    expect(reap(pending, busy)).to be_success
    expect(gauges("shared")).to eq([1.0, 0.0, 1.0])
  ensure
    abandon(pending)
    [ready_reader, ready_writer, zero_reader, zero_writer].each { |io| io&.close }
  end
end
