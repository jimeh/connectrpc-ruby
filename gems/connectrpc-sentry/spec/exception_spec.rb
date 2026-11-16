# frozen_string_literal: true

RSpec.describe "Unexpected handler exception capture" do
  def expect_code(code)
    raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq(code) }
  end

  it "captures a handler bug once, unhandled and linked to the owned RPC transaction" do
    typed = in_process(echo_server(instrumentation: bus, fail: ->(*) { raise "secret bug" }))
    expect { typed.fail(MESSAGE.new(name: "private")) }.to expect_code("internal")
    expect(error_events.size).to eq(1)
    error = error_events.first
    owned = rpc_transactions("rpc.server").first
    expect(trace_of(error)).to include(trace_id: trace_of(owned)[:trace_id], span_id: trace_of(owned)[:span_id])
    expect(mechanism(error)).to include(type: "connectrpc", handled: false)
    expect(error.tags).to include("rpc.system.name" => "connectrpc", "rpc.method" => "example.v1.EchoService/Fail")
    expect(error.transaction).to eq("example.v1.EchoService/Fail")
    expect(trace_of(owned)).to include(status: "internal_error")
    expect(trace_of(owned)[:data]).to include("rpc.response.status_code" => "internal")
    expect(owned.to_h.to_s).not_to include("secret bug", "private")
  end

  it "leaves an exception the application already captured to the application" do
    handler = lambda do |*|
      error = RuntimeError.new("reported by the app")
      Sentry.capture_exception(error)
      raise error
    end
    typed = in_process(echo_server(instrumentation: bus, fail: handler))
    expect { typed.fail(MESSAGE.new) }.to expect_code("internal")
    expect(error_events.size).to eq(1)
    expect(mechanism(error_events.first)).to include(type: "generic")
  end

  it "skips SDK excluded_exceptions" do
    sentry(excluded_exceptions: ["ArgumentError"])
    typed = in_process(echo_server(instrumentation: bus, fail: ->(*) { raise ArgumentError, "excluded" }))
    expect { typed.fail(MESSAGE.new) }.to expect_code("internal")
    expect(error_events).to be_empty
    expect(rpc_transactions.size).to eq(1)
  end

  it "does not capture codec input failures" do
    app = ConnectRPC::Rack.new(server: echo_server(instrumentation: bus))
    response = Rack::MockRequest.new(app).post(ECHO::ECHO.path, "CONTENT_TYPE" => "application/proto",
                                                                input: "\xFF\xFF\xFF".b)
    expect(response.status).to eq(400)
    expect(error_events).to be_empty
    expect(trace_of(rpc_transactions.first)).to include(status: "invalid_argument")
  end

  it "does not capture deliberate ConnectRPC errors" do
    typed = in_process(echo_server(instrumentation: bus))
    expect { typed.fail(MESSAGE.new) }.to expect_code("invalid_argument")
    expect(error_events).to be_empty
  end

  it "does not capture a failure that cancellation arbitrates to canceled" do
    source = ConnectRPC::CancellationSource.new
    handler = lambda do |*|
      source.cancel
      raise IOError, "stream closed by the cancellation"
    end
    instrumentation = bus
    typed = in_process(echo_server(instrumentation: instrumentation, echo: handler), instrumentation: instrumentation)
    caller = start_caller
    expect { typed.echo(MESSAGE.new, timeout: nil, cancellation: source.token) }.to expect_code("canceled")
    caller.finish
    expect(error_events).to be_empty
    statuses = transactions.first.spans.to_h { |span| [span[:op], span[:status]] }
    expect(statuses).to eq("rpc.client" => "cancelled", "rpc.server" => "cancelled")
  end

  it "does not capture a failure that the deadline arbitrates to deadline_exceeded" do
    handler = lambda do |_, context|
      # The RPC deadline bounds this wait on the observable expiry.
      Thread.pass until context.deadline.expired?
      raise "database timed out"
    end
    typed = in_process(echo_server(instrumentation: bus, echo: handler))
    expect { typed.echo(MESSAGE.new, timeout: 0.05) }.to expect_code("deadline_exceeded")
    expect(error_events).to be_empty
    expect(trace_of(rpc_transactions.first)).to include(status: "deadline_exceeded")
  end

  it "never captures client failures" do
    transport = Object.new
    transport.define_singleton_method(:call) { |_request| raise "client transport bug" }
    core = ConnectRPC::Client.new(base_url: "https://example.invalid", transport: transport, instrumentation: bus,
                                  binding: ConnectRPC::Bindings::GoogleProtobuf.new)
    caller = start_caller
    expect { ECHO::Client.new(client: core).echo(MESSAGE.new) }.to raise_error(RuntimeError, "client transport bug")
    caller.finish
    expect(error_events).to be_empty
    expect(transactions.first.spans.first).to include(op: "rpc.client", status: "unknown_error")
  end

  it "records but does not capture handler bugs when capture is disabled" do
    subscriber = ConnectRPC::Sentry::Subscriber.new(capture_exceptions: false)
    typed = in_process(echo_server(instrumentation: bus(subscriber), fail: ->(*) { raise "bug" }))
    expect { typed.fail(MESSAGE.new) }.to expect_code("internal")
    expect(error_events).to be_empty
    expect(trace_of(rpc_transactions.first)).to include(status: "internal_error")
  end

  it "rejects a non-boolean capture_exceptions" do
    expect { ConnectRPC::Sentry::Subscriber.new(capture_exceptions: "yes") }
      .to raise_error(ArgumentError, "capture_exceptions must be true or false")
  end

  it "starts no observer before the SDK is initialized" do
    Sentry.close
    typed = in_process(echo_server(instrumentation: bus, fail: ->(*) { raise "bug" }))
    expect { typed.fail(MESSAGE.new) }.to expect_code("internal")
    expect(Sentry.initialized?).to be(false)
  end
end
