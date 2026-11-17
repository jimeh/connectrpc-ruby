# frozen_string_literal: true

# Names, help text, label names and code strings follow go-grpc-middleware
# providers/prometheus at commit 157054342fa0f666a2aa39c9661312b8dc318cd7
# (server_metrics.go, client_metrics.go, client_options.go, constants.go):
# https://github.com/grpc-ecosystem/go-grpc-middleware/tree/157054342fa0f666a2aa39c9661312b8dc318cd7/providers/prometheus
RSpec.describe "Prometheus exposition" do
  let(:binding) { ConnectRPC::Bindings::GoogleProtobuf.new }
  let(:service) { Example::V1::EchoService }

  def call_echo(instrumentation, fail_code: nil)
    handlers = service.handlers(echo: ->(message, _) { message },
                                fail: ->(*) { raise ConnectRPC::Error.new(fail_code, "safe") })
    server = ConnectRPC::Server.new(services: [service::SERVICE], binding: binding, handlers: handlers,
                                    instrumentation: instrumentation)
    core = ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding,
                                  transport: ConnectRPC::Testing::Transport.new(server: server),
                                  instrumentation: instrumentation)
    typed = service::Client.new(client: core)
    message = Example::V1::EchoMessage.new(name: "hello")
    typed.echo(message)
    typed.fail(message) if fail_code
  rescue ConnectRPC::Error
    nil
  end

  def section(text, name)
    text.lines.map(&:chomp).grep(/\A(# (HELP|TYPE) )?#{name}[ {]/)
  end

  # The exact counter block: TYPE, upstream HELP text, then one sample per label set.
  def counter_block(name, help, *samples)
    ["# TYPE #{name} counter", "# HELP #{name} #{help}", *samples.map { |labels| "#{name}{#{labels}} 1.0" }]
  end

  it "exports go-grpc-prometheus names, help text, labels and codes" do
    call_echo(bus, fail_code: :failed_precondition)
    text = exposition
    echo = 'grpc_type="unary",grpc_service="example.v1.EchoService",grpc_method="Echo",rpc_protocol="connect"'
    failed = echo.sub('"Echo"', '"Fail"')
    ok = %(#{echo},grpc_code="OK")
    precondition = %(#{failed},grpc_code="FailedPrecondition")
    expected = {
      "grpc_server_started_total" => ["Total number of RPCs started on the server.", echo, failed],
      "grpc_server_handled_total" => ["Total number of RPCs completed on the server, regardless of success or " \
                                      "failure.", ok, precondition],
      "grpc_client_handled_total" => ["Total number of RPCs completed by the client, regardless of success or " \
                                      "failure.", ok, precondition],
      "grpc_server_msg_received_total" => ["Total number of RPC stream messages received on the server.", echo,
                                           failed],
      "grpc_client_msg_sent_total" => ["Total number of gRPC stream messages sent by the client.", echo, failed]
    }
    expected.each do |name, (help, *samples)|
      expect(section(text, name)).to eq(counter_block(name, help, *samples))
    end
    handling = section(text, "grpc_server_handling_seconds_bucket")
    expect(handling.map { |line| line[/le="([^"]+)"/, 1] }.uniq).to eq(
      %w[0.005 0.01 0.025 0.05 0.1 0.25 0.5 1 2.5 5 10 +Inf]
    )
    expect(text).to include("# TYPE grpc_server_handling_seconds histogram\n" \
                            "# HELP grpc_server_handling_seconds Histogram of response latency (seconds) of gRPC " \
                            "that had been application-level handled by the server.\n")
    expect(text).to include("grpc_server_handling_seconds_count{#{echo}} 1.0")
    expect(text).to include("grpc_client_msg_received_bytes_bucket{#{echo},le=\"64\"} 1.0")
    expect(text).to include("# TYPE grpc_client_connection_state gauge")
  end

  it "exports connection count gauges with help, labels and initial zero samples" do
    metrics.connection_observer(transport: "billing")
    text = exposition
    expect(section(text, "grpc_client_connections")).to eq(
      ["# TYPE grpc_client_connections gauge",
       "# HELP grpc_client_connections Current client connections by state: active accepts new calls; draining " \
       "no longer does and closes when its calls end.",
       'grpc_client_connections{transport="billing",state="active"} 0.0',
       'grpc_client_connections{transport="billing",state="draining"} 0.0']
    )
    expect(section(text, "grpc_client_pending_calls")).to eq(
      ["# TYPE grpc_client_pending_calls gauge",
       "# HELP grpc_client_pending_calls Current client calls waiting for a connection.",
       'grpc_client_pending_calls{transport="billing"} 0.0']
    )
  end

  it "swaps only the prefix with a namespace and rejects a second registration on one registry" do
    custom = ConnectRPC::Prometheus::Metrics.new(registry: registry, namespace: "rpc", size_buckets: [10, 100])
    call_echo(ConnectRPC::Instrumentation.new(subscribers: [custom.subscriber]))
    expect(registry.get(:rpc_server_handled_total)).not_to be_nil
    expect(registry.get(:grpc_server_handled_total)).to be_nil
    expect(exposition).to include('rpc_server_msg_sent_bytes_bucket{grpc_type="unary",' \
                                  'grpc_service="example.v1.EchoService",grpc_method="Echo",rpc_protocol="connect",' \
                                  'le="10"} 1.0')
    expect { ConnectRPC::Prometheus::Metrics.new(registry: registry, namespace: "rpc") }
      .to raise_error(Prometheus::Client::Registry::AlreadyRegisteredError)
  end

  it "rejects a repeated transport name on one Metrics" do
    first = metrics.connection_observer(transport: "billing")
    expect(first.transport).to eq("billing")
    expect { metrics.connection_observer(transport: "billing") }.to raise_error(ArgumentError, /billing/)
    expect(metrics.connection_observer(transport: "search")).not_to equal(first)
  end
end
