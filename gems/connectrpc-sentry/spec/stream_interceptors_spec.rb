# frozen_string_literal: true

RSpec.describe "Sentry capture with stream interceptors over the Async host" do
  def watch
    @watch ||= ConnectRPC::ServerStreamMethod.new(service: "trace.Streams", name: "Watch", request_type: MESSAGE,
                                                  response_type: MESSAGE)
  end

  def translating(code)
    Object.new.tap do |interceptor|
      interceptor.define_singleton_method(:server_stream) do |_call, &continuation|
        continuation.call
      rescue RuntimeError
        raise ConnectRPC::Error.new(code, "Translated")
      end
    end
  end

  def watch_failing(code)
    sentry(isolation: :fiber)
    instrumentation = bus
    reactor do |task|
      target = ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: "trace.Streams", methods: [watch])],
                                      handlers: { watch.path => ->(*) { raise "secret stream handler bug" } },
                                      instrumentation: instrumentation, interceptors: [translating(code)],
                                      binding: ConnectRPC::Bindings::GoogleProtobuf.new)
      host = ConnectRPC::Async::Host.new(server: target).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      core = ConnectRPC::Client.new(base_url: origin, transport: transport,
                                    binding: ConnectRPC::Bindings::GoogleProtobuf.new)
      expect { core.server_stream(watch, MESSAGE.new(name: "bug")) { |reader| reader.each { nil } } }
        .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq(code.to_s) }
      wait_until(task, "the server transaction") { rpc_transactions("rpc.server").size == 1 }
    ensure
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  it "captures a stream handler bug that an interceptor translates to internal" do
    watch_failing(:internal)
    expect(error_events.size).to eq(1)
    expect(error_events.first.to_h.dig(:exception, :values).last[:value]).to include("secret stream handler bug")
    expect(mechanism(error_events.first)).to include(type: "connectrpc", handled: false)
  end

  it "does not capture a stream handler bug that an interceptor translates to invalid_argument" do
    watch_failing(:invalid_argument)
    expect(error_events).to be_empty
    expect(trace_of(rpc_transactions("rpc.server").first)[:data])
      .to include("rpc.response.status_code" => "invalid_argument")
  end
end
