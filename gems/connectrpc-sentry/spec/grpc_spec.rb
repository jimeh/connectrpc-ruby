# frozen_string_literal: true

RSpec.describe "gRPC and gRPC-Web conventions" do
  it "records gRPC-Web codes in uppercase with the Web protocol marker" do
    deliberate = ->(*) { raise ConnectRPC::Error.new(:not_found, "missing") }
    target = echo_server(instrumentation: bus, fail: deliberate, protocol: :grpc_web,
                         status_codec: ConnectRPC::Bindings::GoogleProtobuf.new)
    bytes = MESSAGE.new(name: "web").to_proto
    response = Rack::MockRequest.new(ConnectRPC::Rack.new(server: target))
                                .post(ECHO::FAIL.path, "CONTENT_TYPE" => "application/grpc-web+proto",
                                                       input: [0, bytes.bytesize].pack("CN") + bytes)
    expect(response.status).to eq(200)
    trace = trace_of(rpc_transactions.first)
    expect(trace).to include(status: "not_found")
    expect(trace[:data]).to include("rpc.system.name" => "grpc", "connectrpc.protocol" => "grpc_web",
                                    "rpc.method" => "example.v1.EchoService/Fail",
                                    "rpc.status_code" => "NOT_FOUND")
    expect(error_events).to be_empty
  end

  it "records native gRPC codes on both sides and captures a handler bug once" do
    sentry(isolation: :fiber)
    instrumentation = bus
    reactor do |task|
      handler = lambda do |message, _|
        raise "grpc bug" if message.name == "bug"
        raise ConnectRPC::Error.new(:canceled, "client went away") if message.name == "cancel"

        message
      end
      host = ConnectRPC::Async::Host.new(server: echo_server(instrumentation: instrumentation, echo: handler,
                                                             protocol: :grpc)).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      core = ConnectRPC::Client.new(base_url: origin, transport: transport, protocol: :grpc,
                                    binding: ConnectRPC::Bindings::GoogleProtobuf.new, instrumentation: instrumentation)
      typed = ECHO::Client.new(client: core)
      caller = start_caller
      expect(typed.echo(MESSAGE.new(name: "ok")).message.name).to eq("ok")
      %w[bug cancel].each { |name| expect { typed.echo(MESSAGE.new(name: name)) }.to raise_error(ConnectRPC::Error) }
      caller.finish
      wait_until(task, "three server transactions") { rpc_transactions("rpc.server").size == 3 }
      clients = transactions.find { |event| event.transaction == "caller" }.spans
      servers = rpc_transactions("rpc.server").map { |event| trace_of(event) }
      [clients, servers].each do |spans|
        expect(spans.map { |span| span[:data]["rpc.status_code"] }.sort).to eq(%w[CANCELLED INTERNAL OK])
        expect(spans.map { |span| span[:status] }.sort).to eq(%w[cancelled internal_error ok])
        expect(spans.map { |span| span[:data]["rpc.system.name"] }.uniq).to eq(["grpc"])
        spans.each { |span| expect(span[:data]).not_to have_key("rpc.response.status_code") }
      end
      # Servers record the request authority, as clients record their endpoint.
      (clients + servers).each do |span|
        expect(span[:data]).to include("server.address" => "127.0.0.1", "server.port" => host.port)
      end
      expect(error_events.size).to eq(1)
      bug = servers.find { |span| span[:status] == "internal_error" }
      expect(trace_of(error_events.first)).to include(span_id: bug[:span_id])
    ensure
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end
end
