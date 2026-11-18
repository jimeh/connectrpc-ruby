# frozen_string_literal: true

require "open3"
require "rbconfig"
require "stringio"
require "connectrpc/metrics"

RSpec.describe "Backend-neutral metrics" do
  let(:recorder) do
    Class.new do
      attr_reader :calls

      def initialize = @calls = []
      def increment(key, labels, by) = @calls << [:increment, key, labels, by]
      def observe(key, labels, value) = @calls << [:observe, key, labels, value]
      def set(key, labels, value) = @calls << [:set, key, labels, value]

      def total(key, **labels)
        @calls.select { |op, name, values| op == :increment && name == key && labels <= values }.sum(&:last)
      end
    end.new
  end
  let(:definitions) { ConnectRPC::Metrics::Definitions.new }
  let(:metrics) { ConnectRPC::Metrics::Subscriber.new(recorder: recorder, definitions: definitions) }
  let(:bus) { ConnectRPC::Instrumentation.new(subscribers: [metrics]) }
  let(:binding) do
    Object.new.tap do |codec|
      codec.define_singleton_method(:encode) { |message, **| message.b }
      codec.define_singleton_method(:decode) { |bytes, **| bytes }
    end
  end
  let(:rpc) do
    ConnectRPC::Method.new(service: "metrics.v1.Echo", name: "Say", request_type: :request, response_type: :response)
  end
  let(:base) { { grpc_type: "unary", grpc_service: "metrics.v1.Echo", grpc_method: "Say", rpc_protocol: "connect" } }

  def server(handler = ->(message, _) { message }, **)
    ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: rpc.service, rpcs: [rpc])], binding: binding,
                           handlers: { rpc.path => handler }, instrumentation: bus, **)
  end

  def post(target, path: rpc.path, body: "hello")
    target.call(path: path, http_method: "POST", headers: { "content-type" => ["application/proto"] },
                input: StringIO.new(body))
  end

  def client(target)
    transport = Object.new
    transport.define_singleton_method(:call) do |request|
      target.call(path: request.url.path, http_method: request.http_method, headers: request.headers,
                  input: StringIO.new(request.body))
    end
    ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding, transport: transport,
                           instrumentation: bus)
  end

  def in_flight(role)
    started = recorder.total(:"#{role}_started_total")
    started - recorder.total(:"#{role}_handled_total") - recorder.total(:"#{role}_rejected_total")
  end

  describe "completion classification" do
    it "records a successful unary RPC on both roles with base labels and message sizes" do
      expect(client(server).call(rpc, "hello").message).to eq("hello")
      %i[server client].each do |role|
        expect(recorder.calls).to include([:increment, :"#{role}_started_total", base, 1],
                                          [:increment, :"#{role}_handled_total", base.merge(grpc_code: "OK"), 1],
                                          [:increment, :"#{role}_msg_received_total", base, 1],
                                          [:observe, :"#{role}_msg_received_bytes", base, 5],
                                          [:increment, :"#{role}_msg_sent_total", base, 1],
                                          [:observe, :"#{role}_msg_sent_bytes", base, 5])
        handling = recorder.calls.find { |op, key| op == :observe && key == :"#{role}_handling_seconds" }
        expect(handling[2]).to eq(base)
        expect(handling[3]).to be_a(Float).and(be >= 0)
        expect(in_flight(role)).to eq(0)
      end
      expect(recorder.total(:server_rejected_total)).to eq(0)
    end

    it "maps a returned error code to its Go name" do
      target = server(->(*) { raise ConnectRPC::Error.new(:failed_precondition, "no") })
      expect { client(target).call(rpc, "x") }.to raise_error(ConnectRPC::Error)
      expect(recorder.total(:server_handled_total, grpc_code: "FailedPrecondition")).to eq(1)
      expect(recorder.total(:client_handled_total, grpc_code: "FailedPrecondition")).to eq(1)
    end

    it "labels undeclared paths _OTHER with an unknown type" do
      expect(post(server, path: "/metrics.v1.Echo/Missing").status).to eq(501)
      other = { grpc_type: "unknown", grpc_service: "_OTHER", grpc_method: "_OTHER", rpc_protocol: "connect" }
      expect(recorder.calls).to include([:increment, :server_started_total, other, 1],
                                        [:increment, :server_handled_total, other.merge(grpc_code: "Unimplemented"), 1])
    end

    it "classifies an escaping exception without a code as handled Unknown" do
      escape = Class.new(Exception) # rubocop:disable Lint/InheritException -- Exercise a non-StandardError escape.
      target = server(->(*) { raise escape, "fatal" })
      expect { post(target) }.to raise_error(escape)
      expect(recorder.total(:server_handled_total, grpc_code: "Unknown")).to eq(1)
      expect(recorder.total(:server_rejected_total)).to eq(0)
      expect(in_flight(:server)).to eq(0)
    end

    it "classifies a client failure without a code as handled Unknown" do
      transport = Object.new
      transport.define_singleton_method(:call) { |_| raise "connection reset" }
      failing = ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding, transport: transport,
                                       instrumentation: bus)
      expect { failing.call(rpc, "x") }.to raise_error(RuntimeError)
      expect(recorder.calls).to include([:increment, :client_handled_total, base.merge(grpc_code: "Unknown"), 1])
      expect(in_flight(:client)).to eq(0)
    end

    it "classifies host-rejected and admission-rejected lifecycles by decision" do
      target = server
      %i[admission_rejected host_rejected].each do |decision|
        target.start_call(path: rpc.path, headers: {}).finish(delivery: :completed, decision: decision)
        expect(recorder.calls.last).to eq([:increment, :server_rejected_total, base.merge(reason: decision.to_s), 1])
      end
      expect(recorder.total(:server_handled_total)).to eq(0)
      expect(in_flight(:server)).to eq(0)
    end

    it "classifies a processed lifecycle without an outcome as unprocessed" do
      require "connectrpc/bindings/google_protobuf"
      target = server(protocol: %i[connect grpc], status_codec: ConnectRPC::Bindings::GoogleProtobuf.new)
      handle = target.start_call(path: rpc.path, headers: { "content-type" => ["application/grpc"] })
      response = target.call_grpc(path: rpc.path, http_method: "GET", headers: {}, input: nil, output: nil,
                                  publish: ->(_) {}, lifecycle: handle, http2: true)
      expect(response.status).to eq(405)
      handle.finish(delivery: :completed, http_status: 405)
      grpc = base.merge(rpc_protocol: "grpc")
      expect(recorder.calls).to eq([[:increment, :server_started_total, grpc, 1],
                                    [:increment, :server_rejected_total, grpc.merge(reason: "unprocessed"), 1]])
    end

    it "classifies a code-less outcome without an error as unprocessed" do
      target = server
      handle = target.start_call(path: rpc.path, headers: {})
      handle.begin_processing(target, rpc.path)
      handle.processing_finished(code: nil, http_status: 505)
      handle.finish(delivery: :completed)
      expect(recorder.calls.last).to eq([:increment, :server_rejected_total, base.merge(reason: "unprocessed"), 1])
    end

    it "counts handled RPCs whose delivery aborted" do
      target = server
      handle = target.start_call(path: rpc.path, headers: { "content-type" => ["application/proto"] })
      target.call(path: rpc.path, http_method: "POST", headers: { "content-type" => ["application/proto"] },
                  input: StringIO.new("hi"), lifecycle: handle)
      handle.finish(delivery: :aborted)
      expect(recorder.total(:server_handled_total, grpc_code: "OK")).to eq(1)
      expect(recorder.calls.last).to eq([:increment, :server_delivery_aborted_total, base, 1])
    end
  end

  it "maps all seventeen Connect codes to Go names and anything else to Unknown" do
    codes = %w[ok canceled unknown invalid_argument deadline_exceeded not_found already_exists permission_denied
               resource_exhausted failed_precondition aborted out_of_range unimplemented internal unavailable
               data_loss unauthenticated]
    expect(codes.map { |code| ConnectRPC::Metrics.code(code) }).to eq(
      %w[OK Canceled Unknown InvalidArgument DeadlineExceeded NotFound AlreadyExists PermissionDenied
         ResourceExhausted FailedPrecondition Aborted OutOfRange Unimplemented Internal Unavailable DataLoss
         Unauthenticated]
    )
    expect(ConnectRPC::Metrics.code("OK")).to eq("OK")
    expect(ConnectRPC::Metrics.code("cancelled")).to eq("Unknown")
  end

  it "labels every declaration type from the declared method" do
    types = { ConnectRPC::ServerStreamMethod => "server_stream", ConnectRPC::ClientStreamMethod => "client_stream",
              ConnectRPC::BidiStreamMethod => "bidi_stream", ConnectRPC::Method => "unary" }
    types.each do |kind, type|
      declaration = kind.new(service: "a.B", name: "C", request_type: :q, response_type: :r)
      call = ConnectRPC::Instrumentation::Call.new(role: :client, protocol: :grpc_web, method: declaration,
                                                   server_address: "secret.example", server_port: 443, carrier: {})
      expect(ConnectRPC::Metrics.labels(call)).to eq(grpc_type: type, grpc_service: "a.B", grpc_method: "C",
                                                     rpc_protocol: "grpc_web")
    end
  end

  describe ConnectRPC::Metrics::Definitions do
    it "defines the go-grpc-prometheus compatible metric set" do
      names = definitions.instruments.values.to_h { |instrument| [definitions.full_name(instrument), instrument.kind] }
      expect(names).to eq(
        "grpc_server_started_total" => :counter, "grpc_server_handled_total" => :counter,
        "grpc_server_handling_seconds" => :histogram, "grpc_server_msg_received_total" => :counter,
        "grpc_server_msg_sent_total" => :counter, "grpc_server_msg_received_bytes" => :histogram,
        "grpc_server_msg_sent_bytes" => :histogram, "grpc_server_rejected_total" => :counter,
        "grpc_server_delivery_aborted_total" => :counter,
        "grpc_client_started_total" => :counter, "grpc_client_handled_total" => :counter,
        "grpc_client_handling_seconds" => :histogram, "grpc_client_msg_received_total" => :counter,
        "grpc_client_msg_sent_total" => :counter, "grpc_client_msg_received_bytes" => :histogram,
        "grpc_client_msg_sent_bytes" => :histogram,
        "grpc_client_connection_attempts_total" => :counter, "grpc_client_connection_attempt_seconds" => :histogram,
        "grpc_client_connection_state_changes_total" => :counter, "grpc_client_connection_state" => :gauge,
        "grpc_client_admission_rejected_total" => :counter, "grpc_client_connections" => :gauge,
        "grpc_client_pending_calls" => :gauge
      )
      definitions.instruments.each do |key, instrument|
        expect(key.to_s).to eq([instrument.name, instrument.unit].compact.join("_"))
        expect(instrument.help).not_to be_empty
      end
      expect(definitions.fetch(:server_handling_seconds).buckets).to eq(
        [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
      )
      expect(definitions.fetch(:client_msg_sent_bytes).buckets.last).to eq(4_194_304)
      expect(definitions.fetch(:server_handled_total).labels).to eq(
        %i[grpc_type grpc_service grpc_method rpc_protocol grpc_code]
      )
      expect(definitions.fetch(:client_connections).labels).to eq(%i[transport state])
      expect(definitions.fetch(:client_pending_calls).labels).to eq(%i[transport])
    end

    it "swaps only the prefix and accepts custom buckets" do
      custom = described_class.new(namespace: :rpc, duration_buckets: [1, 2], size_buckets: [10])
      expect(custom.full_name(custom.fetch(:client_handling_seconds))).to eq("rpc_client_handling_seconds")
      expect(custom.fetch(:client_connection_attempt_seconds).buckets).to eq([1, 2])
      expect(custom.fetch(:server_msg_received_bytes).buckets).to eq([10])
    end

    [["Grpc", {}], ["1grpc", {}], ["", {}], [nil, {}], ["grpc", { duration_buckets: [] }],
     ["grpc", { duration_buckets: [1, 1] }], ["grpc", { size_buckets: [2, 1] }],
     ["grpc", { size_buckets: [0, 1] }], ["grpc", { size_buckets: nil }],
     ["grpc", { duration_buckets: [Float::INFINITY] }],
     ["grpc", { duration_buckets: ["1"] }]].each do |namespace, options|
      it "rejects namespace #{namespace.inspect} with #{options}" do
        expect { described_class.new(namespace: namespace, **options) }.to raise_error(ArgumentError)
      end
    end
  end

  describe ConnectRPC::Metrics::ConnectionObserver do
    let(:event_type) do
      Data.define(:type, :state, :previous_state, :outcome, :duration, :active, :draining, :pending_calls)
    end

    def event(type, state: :idle, previous_state: nil, outcome: nil, duration: nil, counts: [0, 0, 0])
      active, draining, pending_calls = counts
      event_type.new(type: type, state: state, previous_state: previous_state, outcome: outcome, duration: duration,
                     active: active, draining: draining, pending_calls: pending_calls)
    end

    def observer(name = "billing") = described_class.new(recorder: recorder, definitions: definitions, transport: name)

    # The last value set for each state series.
    def state_gauge
      recorder.calls.each_with_object({}) do |(op, key, labels, value), gauge|
        gauge[labels.fetch(:state)] = value if op == :set && key == :client_connection_state
      end
    end

    # The last value set for each count series.
    def count_gauges
      recorder.calls.each_with_object({}) do |(op, key, labels, value), gauge|
        next unless op == :set

        gauge[labels.fetch(:state)] = value if key == :client_connections
        gauge["pending_calls"] = value if key == :client_pending_calls
      end
    end

    it "publishes state gauges on request and maps state, attempt and admission events" do
      observer = self.observer
      expect(recorder.calls).to be_empty
      expect(observer.publish_state).to be_nil
      initial = recorder.calls.dup
      expect(initial).to eq(%w[idle connecting ready transient_failure closing closed].map do |state|
        [:set, :client_connection_state, { transport: "billing", state: state }, state == "idle" ? 1 : 0]
      end + [[:set, :client_connections, { transport: "billing", state: "active" }, 0],
             [:set, :client_connections, { transport: "billing", state: "draining" }, 0],
             [:set, :client_pending_calls, { transport: "billing" }, 0]])
      recorder.calls.clear
      observer.call(event(:state_changed, state: :connecting, previous_state: :idle))
      observer.call(event(:attempt_finished, state: :connecting, outcome: :refused, duration: 0.25))
      observer.call(event(:admission_rejected, state: :transient_failure))
      observer.call(event(:unknown_future_type))
      expect(recorder.calls).to eq(
        [[:increment, :client_connection_state_changes_total, { transport: "billing", state: "connecting" }, 1],
         [:set, :client_connection_state, { transport: "billing", state: "idle" }, 0],
         [:set, :client_connection_state, { transport: "billing", state: "connecting" }, 1],
         [:increment, :client_connection_attempts_total, { transport: "billing", outcome: "refused" }, 1],
         [:observe, :client_connection_attempt_seconds, { transport: "billing" }, 0.25],
         [:increment, :client_admission_rejected_total, { transport: "billing" }, 1]]
      )
    end

    it "bounds attempt outcome labels to the documented set, recording anything else as other" do
      observer = self.observer
      known = %i[ready refused timeout tls protocol closed other]
      [*known, :surprise, "ready", nil].each do |outcome|
        observer.call(event(:attempt_finished, state: :connecting, outcome: outcome))
      end
      outcomes = recorder.calls.filter_map do |op, key, labels|
        labels.fetch(:outcome) if op == :increment && key == :client_connection_attempts_total
      end
      expect(outcomes).to eq([*known.map(&:to_s), "other", "other", "other"])
    end

    it "records nothing for connection_closed events with unchanged counts" do
      observer = self.observer
      %i[keepalive_timeout drain_timeout goaway closed].each do |reason|
        observer.call(event(:connection_closed, state: :ready, outcome: reason))
      end
      expect(recorder.calls).to be_empty
    end

    it "sets only changed count series from events of every type" do
      observer = self.observer
      observer.call(event(:state_changed, state: :connecting, previous_state: :idle, counts: [0, 0, 1]))
      observer.call(event(:counts_changed, state: :ready, counts: [1, 0, 1]))
      observer.call(event(:counts_changed, state: :ready, counts: [1, 0, 0]))
      observer.call(event(:attempt_finished, state: :ready, outcome: :ready, duration: 0.1, counts: [1, 0, 0]))
      observer.call(event(:connection_closed, state: :ready, outcome: :goaway, counts: [1, 1, 0]))
      observer.call(event(:unknown_future_type, state: :ready, counts: [1, 0, 0]))
      expect(recorder.calls.select { |_, key| %i[client_connections client_pending_calls].include?(key) }).to eq(
        [[:set, :client_pending_calls, { transport: "billing" }, 1],
         [:set, :client_connections, { transport: "billing", state: "active" }, 1],
         [:set, :client_pending_calls, { transport: "billing" }, 0],
         [:set, :client_connections, { transport: "billing", state: "draining" }, 1],
         [:set, :client_connections, { transport: "billing", state: "draining" }, 0]]
      )
      recorder.calls.clear
      observer.publish_state
      expect(count_gauges).to eq("active" => 1, "draining" => 0, "pending_calls" => 0)
    end

    it "zeros its own last published state, not the event's previous state" do
      observer = self.observer
      observer.publish_state
      observer.call(event(:state_changed, state: :connecting, previous_state: :idle))
      observer.call(event(:state_changed, state: :ready, previous_state: :connecting))
      # A replacement connection manager starts at idle while ready is still published.
      observer.call(event(:state_changed, state: :connecting, previous_state: :idle))
      expect(state_gauge.select { |_, value| value == 1 }).to eq("connecting" => 1)
      expect(state_gauge.fetch("ready")).to eq(0)
    end

    it "publishes the tracked state when a state change failed to record" do
      # Like Yabeda before configure!: recording raises until the metrics exist.
      sink = recorder
      ready = false
      gated = Object.new
      %i[increment observe set].each do |op|
        gated.define_singleton_method(op) { |*args| ready ? sink.public_send(op, *args) : raise(KeyError, "missing") }
      end
      observer = described_class.new(recorder: gated, definitions: definitions, transport: "billing")
      expect { observer.call(event(:state_changed, state: :ready, previous_state: :connecting, counts: [2, 1, 3])) }
        .to raise_error(KeyError)
      ready = true
      observer.publish_state
      expect(state_gauge.select { |_, value| value == 1 }).to eq("ready" => 1)
      expect(state_gauge.size).to eq(6)
      expect(count_gauges).to eq("active" => 2, "draining" => 1, "pending_calls" => 3)
    end

    ["", "Billing", "a-b", "x" * 65, :billing, nil].each do |name|
      it "rejects transport name #{name.inspect}" do
        expect { described_class.new(recorder: recorder, definitions: definitions, transport: name) }
          .to raise_error(ArgumentError)
      end
    end
  end

  it "is not loaded by the core entry point" do
    script = 'require "connectrpc"; abort "metrics loaded" if defined?(ConnectRPC::Metrics); ' \
             'require "connectrpc/metrics"; abort "missing" unless ConnectRPC::Metrics::Subscriber'
    _output, error, status = Open3.capture3(RbConfig.ruby, "-I", File.expand_path("../lib", __dir__), "-e", script)
    expect(status.success?).to be(true), error
  end
end
