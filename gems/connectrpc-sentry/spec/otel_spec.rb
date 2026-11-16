# frozen_string_literal: true

require "sentry-opentelemetry"

RSpec.describe "Inert tracing when OpenTelemetry feeds Sentry" do
  def exercise
    seen = {}
    instrumentation = bus
    handler = lambda do |_, context|
      seen[:trace] = context.request_metadata["sentry-trace"]
      seen[:scope] = scope_span
      raise "bug under otel"
    end
    typed = in_process(echo_server(instrumentation: instrumentation, fail: handler), instrumentation: instrumentation)
    caller = Sentry.get_current_scope
    caller.set_tags(caller: "scope")
    expect { typed.fail(MESSAGE.new) }.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("internal") }
    seen
  end

  it "records and propagates nothing with instrumenter :otel but still captures handler bugs" do
    sentry(instrumenter: :otel)
    seen = exercise
    expect(seen).to eq(trace: nil, scope: nil)
    expect(transactions).to be_empty
    expect(error_events.size).to eq(1)
    expect(error_events.first.tags).to include(:caller => "scope", "rpc.method" => "example.v1.EchoService/Fail")
    expect(mechanism(error_events.first)).to include(type: "connectrpc", handled: false)
  end

  it "records and propagates nothing with OTLP enabled but still captures handler bugs" do
    skip "sentry-opentelemetry #{Sentry::VERSION} has no OTLP configuration" unless
      Sentry::Configuration.method_defined?(:otlp)

    sentry { |config| config.otlp.enabled = true }
    expect(Sentry.configuration.instrumenter).to eq(:sentry)
    seen = exercise
    expect(seen).to eq(trace: nil, scope: nil)
    expect(transactions).to be_empty
    expect(error_events.size).to eq(1)
  end
end
