# frozen_string_literal: true

RSpec.describe "ThreadedTransport from ordinary threads" do
  def protobuf = ConnectRPC::Bindings::GoogleProtobuf.new

  def stream_method
    @stream_method ||= ConnectRPC::ServerStreamMethod.new(service: "trace.Streams", name: "Watch",
                                                          request_type: MESSAGE, response_type: MESSAGE)
  end

  def gated_server(instrumentation, entered, release)
    echo = lambda do |message, _|
      entered << message.name
      release.pop
      message
    end
    watch = lambda do |message, writer, _|
      2.times { |index| writer.send_message(MESSAGE.new(name: "#{message.name}#{index}")) }
    end
    services = [ECHO::SERVICE, ConnectRPC::Service.new(name: "trace.Streams", methods: [stream_method])]
    handlers = ECHO.handlers(echo: echo, fail: echo).merge(stream_method.path => watch)
    ConnectRPC::Server.new(services: services, handlers: handlers, binding: protobuf, instrumentation: instrumentation)
  end

  # Two callers enter the unary handler together, then each streams.
  def exercise(instrumentation)
    entered = Thread::Queue.new
    release = Thread::Queue.new
    peer = threaded_host(server: gated_server(instrumentation, entered, release))
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    core = ConnectRPC::Client.new(base_url: origin, transport: transport, binding: protobuf,
                                  instrumentation: instrumentation)
    typed = ECHO::Client.new(client: core)
    callers = {}
    threads = %w[a b].map do |name|
      Thread.new do
        caller = callers[name] = start_caller(name)
        unary = typed.echo(MESSAGE.new(name: name)).message.name
        streamed = []
        core.server_stream(stream_method, MESSAGE.new(name: name)) do |reader|
          reader.each { |message| streamed << message.name }
          streamed << scope_span.op
        end
        restored = scope_span.equal?(caller)
        caller.finish
        [unary, streamed, restored]
      end
    end
    expect([pop_within(entered, "first caller"), pop_within(entered, "second caller")].sort).to eq(%w[a b])
    2.times { release << true }
    results = threads.map { |thread| value_within(thread, "a caller thread") }
    [results, callers]
  ensure
    2.times { release&.<<(true) }
    transport&.close(grace: 0)
    peer&.stop
  end

  def client_spans(name)
    transactions.find { |event| event.transaction == name }.spans.select { |span| span[:op] == "rpc.client" }
  end

  it "records each caller's unary and streaming client spans and restores its scope under fiber isolation" do
    sentry(isolation: :fiber)
    results, callers = exercise(bus)
    expect(results.sort).to eq([["a", %w[a0 a1 rpc.client], true], ["b", %w[b0 b1 rpc.client], true]])
    eventually("four owned server transactions") { rpc_transactions("rpc.server").size == 4 }
    servers = rpc_transactions("rpc.server")
    callers.each do |name, caller|
      spans = client_spans(name)
      expect(spans.map { |span| span[:description] }.sort).to eq(%w[example.v1.EchoService/Echo trace.Streams/Watch])
      expect(spans.map { |span| span[:parent_span_id] }.uniq).to eq([caller.span_id])
      expect(spans.map { |span| span[:status] }.uniq).to eq(["ok"])
      spans.each do |span|
        server = servers.find { |event| trace_of(event)[:parent_span_id] == span[:span_id] }
        expect(trace_of(server)).to include(trace_id: caller.trace_id, status: "ok")
      end
    end
    expect(sentry_log).not_to include("hub_isolation_level")
  end

  it "keeps caller spans but skips reactor activation with thread isolation, warning once" do
    results, = exercise(bus)
    expect(results.map(&:last)).to eq([true, true])
    %w[a b].each { |name| expect(client_spans(name).size).to eq(2) }
    expect(rpc_transactions).to be_empty
    expect(sentry_log.scan("hub_isolation_level = :fiber").size).to eq(1)
  end
end
