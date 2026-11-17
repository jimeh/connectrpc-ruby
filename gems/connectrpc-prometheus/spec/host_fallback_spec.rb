# frozen_string_literal: true

RSpec.describe "Prometheus exposition through the Async Host fallback" do
  # The README's native exporter.
  def exporter
    lambda do |request|
      next Protocol::HTTP::Response[404, {}, nil] unless request.path == "/metrics"

      Protocol::HTTP::Response[200, { "content-type" => Prometheus::Client::Formats::Text::CONTENT_TYPE },
                               [Prometheus::Client::Formats::Text.marshal(registry)]]
    end
  end

  it "serves /metrics over h2c beside RPC traffic on one Host" do
    reactor do
      host = ConnectRPC::Async::Host.new(server: server(instrumentation: bus), fallback: exporter).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(
        origin: origin, connection_observer: metrics.connection_observer(transport: "local")
      )
      expect(client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "scraped")).message.name)
        .to eq("scraped")
      raw = IO::Endpoint.tcp("127.0.0.1", host.port).connect
      connection = Async::HTTP::Protocol::HTTP2.client(raw)
      response = connection.call(Protocol::HTTP::Request["GET", "/metrics", scheme: "http",
                                                                            authority: "127.0.0.1:#{host.port}"])
      expect(response.status).to eq(200)
      expect(response.headers["content-type"]).to eq(Prometheus::Client::Formats::Text::CONTENT_TYPE)
      lines = response.read.lines.map(&:chomp)
      labels = MetricsHelpers::ECHO_LABELS.map { |name, value| %(#{name}="#{value}") }.join(",")
      expect(lines).to include(%(grpc_server_handled_total{#{labels},grpc_code="OK"} 1.0))
      expect(lines).to include('grpc_client_connections{transport="local",state="active"} 1.0')
      expect(lines).to include('grpc_client_pending_calls{transport="local"} 0.0')
      # The scrape itself is not an RPC.
      expect(value(:grpc_server_started_total, MetricsHelpers::ECHO_LABELS)).to eq(1)
      expect(total(:grpc_server_rejected_total)).to eq(0)
    ensure
      response&.close
      connection&.close
      AsyncHelpers::INTERNAL.raw_close(raw) if raw
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end
end
