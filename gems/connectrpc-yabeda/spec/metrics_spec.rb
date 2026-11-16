# frozen_string_literal: true

RSpec.describe ConnectRPC::Yabeda::Metrics do
  let(:labels) do
    { grpc_type: "unary", grpc_service: "example.v1.EchoService", grpc_method: "Echo", rpc_protocol: "connect" }
  end
  let(:errors) { [] }

  def bus(metrics) = ConnectRPC::Instrumentation.new(subscribers: [metrics.subscriber], on_error: ->(e) { errors << e })
  def metric(name) = Yabeda.metrics.fetch(name)

  it "declares before Yabeda.configure! and records server and client values" do
    metrics = described_class.new
    expect(Yabeda.metrics).to be_empty
    Yabeda.configure!
    echo(bus(metrics))
    expect { echo(bus(metrics), fail: true) }.to raise_error(ConnectRPC::Error)
    %w[server client].each do |role|
      expect(adapter.counters.fetch(metric("grpc_#{role}_started_total")).fetch(labels)).to eq(1)
      expect(adapter.counters.fetch(metric("grpc_#{role}_handled_total")).fetch(labels.merge(grpc_code: "OK"))).to eq(1)
      failed = labels.merge(grpc_method: "Fail", grpc_code: "NotFound")
      expect(adapter.counters.fetch(metric("grpc_#{role}_handled_total")).fetch(failed)).to eq(1)
      expect(adapter.counters.fetch(metric("grpc_#{role}_msg_sent_total")).fetch(labels)).to eq(1)
      expect(adapter.histograms.fetch(metric("grpc_#{role}_msg_sent")).fetch(labels)).to eq(7)
      expect(adapter.histograms.fetch(metric("grpc_#{role}_handling")).fetch(labels)).to be_a(Float)
    end
    expect(errors).to be_empty
  end

  it "declares immediately after Yabeda.configure! and records a custom group" do
    Yabeda.configure!
    metrics = described_class.new(group: "rpc", duration_buckets: [1, 2])
    expect(metrics.group).to eq(:rpc)
    expect(metric("rpc_client_handling").buckets).to eq([1, 2])
    echo(bus(metrics))
    expect(adapter.counters.fetch(metric("rpc_server_started_total")).fetch(labels)).to eq(1)
    expect(errors).to be_empty
  end

  it "reports recording before Yabeda.configure! through on_error without failing the RPC" do
    metrics = described_class.new
    expect(echo(bus(metrics)).message.name).to eq("hello")
    expect(errors).not_to be_empty
    expect(errors).to all(be_a(KeyError))
  end

  it "records connection events through the state gauge and rejects a repeated transport name" do
    metrics = described_class.new
    Yabeda.configure!
    observer = metrics.connection_observer(transport: "billing")
    event = Data.define(:type, :state, :previous_state, :outcome, :duration)
    observer.call(event.new(type: :state_changed, state: :connecting, previous_state: :idle, outcome: nil,
                            duration: nil))
    observer.call(event.new(type: :attempt_finished, state: :ready, previous_state: nil, outcome: :ready,
                            duration: 0.5))
    gauge = adapter.gauges.fetch(metric("grpc_client_connection_state"))
    expect(gauge.fetch({ transport: "billing", state: "connecting" })).to eq(1)
    expect(gauge.fetch({ transport: "billing", state: "idle" })).to eq(0)
    expect(gauge.fetch({ transport: "billing", state: "closed" })).to eq(0)
    expect(adapter.counters.fetch(metric("grpc_client_connection_attempts_total"))
                  .fetch({ transport: "billing", outcome: "ready" })).to eq(1)
    expect(adapter.histograms.fetch(metric("grpc_client_connection_attempt")).fetch({ transport: "billing" }))
      .to eq(0.5)
    expect { metrics.connection_observer(transport: "billing") }.to raise_error(ArgumentError)
  end

  it "builds a connection observer before Yabeda.configure! and publishes its state on collect" do
    metrics = described_class.new
    observer = metrics.connection_observer(transport: "early")
    expect(Yabeda).not_to be_configured
    Yabeda.configure!
    Yabeda.collect!
    gauge = adapter.gauges.fetch(metric("grpc_client_connection_state"))
    expect(gauge.fetch({ transport: "early", state: "idle" })).to eq(1)
    expect(gauge.fetch({ transport: "early", state: "ready" })).to eq(0)
    event = Data.define(:type, :state, :previous_state, :outcome, :duration)
    observer.call(event.new(type: :state_changed, state: :connecting, previous_state: :idle, outcome: nil,
                            duration: nil))
    expect(gauge.fetch({ transport: "early", state: "connecting" })).to eq(1)
    expect(gauge.fetch({ transport: "early", state: "idle" })).to eq(0)
    expect(adapter.counters.fetch(metric("grpc_client_connection_state_changes_total"))
                  .fetch({ transport: "early", state: "connecting" })).to eq(1)
  end

  it "produces yabeda-prometheus names equal to the shared definitions" do
    metrics = described_class.new
    Yabeda.configure!
    # yabeda-prometheus 0.9.1 Yabeda::Prometheus::Adapter#build_name:
    #   [metric.group, metric.name, metric.unit].compact.join('_').to_sym
    exported = Yabeda.metrics.values.to_h do |declared|
      [[declared.group, declared.name, declared.unit].compact.join("_"), declared]
    end
    expected = metrics.definitions.instruments.values.to_h do |instrument|
      [metrics.definitions.full_name(instrument), instrument]
    end
    expect(exported.keys).to match_array(expected.keys)
    expect(expected.keys).to include("grpc_server_handling_seconds", "grpc_client_msg_received_bytes",
                                     "grpc_server_handled_total", "grpc_client_connection_state")
    exported.each do |name, declared|
      instrument = expected.fetch(name)
      expect(declared.comment).to eq(instrument.help)
      expect(declared.tags).to eq(instrument.labels)
      expect(declared.buckets).to eq(instrument.buckets) if instrument.kind == :histogram
      expect(declared.aggregation).to eq(:sum) if instrument.kind == :gauge
      expect(declared).to be_a({ counter: Yabeda::Counter, histogram: Yabeda::Histogram,
                                 gauge: Yabeda::Gauge }.fetch(instrument.kind))
    end
  end
end
