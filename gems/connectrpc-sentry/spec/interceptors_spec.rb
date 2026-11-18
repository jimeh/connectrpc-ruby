# frozen_string_literal: true

RSpec.describe "Sentry capture with unary interceptors" do
  def translating(code)
    Object.new.tap do |interceptor|
      interceptor.define_singleton_method(:server_unary) do |_call, &continuation|
        continuation.call
      rescue RuntimeError
        raise ConnectRPC::Error.new(code, "Translated")
      end
    end
  end

  def call_failing(code)
    target = echo_server(instrumentation: bus, fail: ->(*) { raise "secret handler bug" },
                         interceptors: [translating(code)])
    expect { in_process(target).fail(MESSAGE.new) }
      .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq(code.to_s) }
  end

  it "captures a handler bug that an interceptor translates to internal" do
    call_failing(:internal)
    expect(error_events.size).to eq(1)
    expect(error_events.first.to_h.dig(:exception, :values).last[:value]).to include("secret handler bug")
    expect(mechanism(error_events.first)).to include(type: "connectrpc", handled: false)
  end

  it "does not capture a handler bug that an interceptor translates to invalid_argument" do
    call_failing(:invalid_argument)
    expect(error_events).to be_empty
    expect(trace_of(rpc_transactions("rpc.server").first)[:data])
      .to include("rpc.status_code" => "invalid_argument")
  end
end
