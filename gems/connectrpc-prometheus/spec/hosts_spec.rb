# frozen_string_literal: true

require "rack/mock_request"

RSpec.describe "Host completion classification" do
  let(:echo) { AsyncHelpers::ECHO::ECHO }
  let(:message) { AsyncHelpers::MESSAGE.new(name: "hello") }

  def rack_env(**)
    Rack::MockRequest.env_for(echo.path, method: "POST", input: message.to_proto,
                                         "CONTENT_TYPE" => "application/proto", **)
  end

  it "counts a held Rack body in flight until the host closes it" do
    app = ConnectRPC::Rack.new(server: server(instrumentation: bus))
    status, _headers, body = app.call(rack_env)
    expect(status).to eq(200)
    expect(total(:grpc_server_started_total)).to eq(1)
    expect(in_flight).to eq(1)
    body.each { |_| nil }
    body.close
    expect(value(:grpc_server_handled_total, MetricsHelpers::ECHO_LABELS.merge(grpc_code: "OK"))).to eq(1)
    expect(in_flight).to eq(0)
    expect(total(:grpc_server_delivery_aborted_total)).to eq(0)
  end

  it "counts a Rack response the host reports as aborted" do
    app = ConnectRPC::Rack.new(server: server(instrumentation: bus))
    env = rack_env("rack.response_finished" => [])
    status, headers, = app.call(env)
    expect(in_flight).to eq(1)
    env.fetch("rack.response_finished").each { |callback| callback.call(env, status, headers, IOError.new("reset")) }
    expect(value(:grpc_server_handled_total, MetricsHelpers::ECHO_LABELS.merge(grpc_code: "OK"))).to eq(1)
    expect(value(:grpc_server_delivery_aborted_total, MetricsHelpers::ECHO_LABELS)).to eq(1)
    expect(in_flight).to eq(0)
  end

  it "records an Async HTTP/2 admission rejection without a handled status" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      target = server(instrumentation: bus, echo: lambda { |request, _|
        if request.name == "admitted"
          entered.enqueue(true)
          release.dequeue
        end
        request
      })
      host = ConnectRPC::Async::Host.new(server: target, max_requests: 1).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      # The host refuses the stream with REFUSED_STREAM; without the default
      # resend, one refused stream is one admission rejection.
      second_transport = ConnectRPC::Async::Transport.new(
        origin: origin, connection_policy: ConnectRPC::Async::ConnectionPolicy.new(retry_refused: false)
      )
      second = client(second_transport, origin)
      second.echo(AsyncHelpers::MESSAGE.new(name: "warmup"))
      active = task.async { client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "admitted"), timeout: nil) }
      entered.dequeue
      expect { second.echo(AsyncHelpers::MESSAGE.new(name: "rejected"), timeout: nil) }
        .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("unavailable") }
      wait_until(task, "admission rejection completion") { total(:grpc_server_rejected_total).positive? }
      expect(value(:grpc_server_rejected_total, MetricsHelpers::ECHO_LABELS.merge(reason: "admission_rejected")))
        .to eq(1)
      expect(in_flight).to eq(1)
      release.enqueue(true)
      expect(active.wait.message.name).to eq("admitted")
      wait_until(task, "admitted completion") { in_flight.zero? }
      expect(value(:grpc_server_handled_total, MetricsHelpers::ECHO_LABELS.merge(grpc_code: "OK"))).to eq(2)
    ensure
      release&.enqueue(true)
      second_transport&.close(grace: 0)
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  it "records a native gRPC preflight rejection as unprocessed" do
    reactor do |task|
      host = ConnectRPC::Async::Host.new(server: server(instrumentation: bus, protocol: %i[connect grpc],
                                                        status_codec: ConnectRPC::Bindings::GoogleProtobuf.new)).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      # gRPC intent with an unsupported codec is rejected before processing begins.
      response = transport.call(request("#{origin}#{echo.path}",
                                        headers: { "content-type" => ["application/grpc+json"] }))
      expect(response.status).to eq(415)
      wait_until(task, "preflight completion") { total(:grpc_server_rejected_total).positive? }
      labels = MetricsHelpers::ECHO_LABELS.merge(rpc_protocol: "grpc")
      expect(value(:grpc_server_rejected_total, labels.merge(reason: "unprocessed"))).to eq(1)
      expect(total(:grpc_server_handled_total)).to eq(0)
      expect(in_flight).to eq(0)
    ensure
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  it "records a Falcon HTTP/1 listener rejection by decision" do
    reactor do |task|
      falcon(server: server(instrumentation: bus)) do |_host, origin|
        status, = h2(origin, echo.path, protocol: "websocket")
        expect(status).to eq(501)
        wait_until(task, "host rejection completion") { total(:grpc_server_rejected_total).positive? }
        expect(value(:grpc_server_rejected_total, MetricsHelpers::ECHO_LABELS.merge(reason: "host_rejected")))
          .to eq(1)
        expect(in_flight).to eq(0)
      end
    end
  end
end
