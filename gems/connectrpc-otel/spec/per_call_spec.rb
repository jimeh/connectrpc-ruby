# frozen_string_literal: true

require_relative "../../connectrpc-async/spec/support/threaded_helpers"

RSpec.describe "PerCallTransport tracing" do
  include ThreadedHelpers

  let(:telemetry) { tracing }
  let(:provider) { telemetry[0] }
  let(:exporter) { telemetry[1] }
  let(:bus) { telemetry[2] }

  # A traced host on its own reactor thread whose "held" handler waits on the
  # current run's release queue; every handler records its active span.
  def traced_host(observed, entered, gates)
    threaded_host(server: server(instrumentation: bus, echo: lambda { |message, _context|
      observed[message.name] = current_span.context
      if message.name == "held"
        release = gates.fetch(:release)
        entered << true
        release.pop
      end
      message
    }))
  end

  # Runs a successful and a cancelled unary call under one parent span each,
  # and returns the exported client and server spans per call, comparable
  # across transports.
  def traced_calls(transport, origin, entered, release)
    source = ConnectRPC::CancellationSource.new
    canceller = Thread.new do
      entered.pop
      source.cancel
    end
    typed = client(transport, origin, instrumentation: bus)
    parents = %w[ok held].map { |name| provider.tracer("test").start_span("parent-#{name}") }
    results = parents.zip(%w[ok held]).map do |parent, name|
      OpenTelemetry::Trace.with_span(parent) do
        error = capture_error do
          typed.echo(AsyncHelpers::MESSAGE.new(name: name), cancellation: (source.token if name == "held"))
        end
        raise "parent span lost" unless current_span.equal?(parent)

        error&.code || "ok"
      end
    ensure
      parent.finish
    end
    canceller.join(10)
    # A handler on a connection that stays open outlives its reset stream.
    release << true
    [results, parents]
  end

  def summarize(parents, observed)
    spans = rpc_spans(exporter)
    parents.zip(%w[ok held]).map do |parent, name|
      pair = spans.select { |span| span.trace_id == parent.context.trace_id }
      caller = pair.find { |span| span.kind == :client }
      check_parents(parent, caller, pair.find { |span| span.kind == :server }, observed.fetch(name))
      attributes = caller.attributes.reject { |key, _| key.end_with?(".duration") || key == "server.port" }
      [caller.name, attributes, caller.status.code]
    end
  end

  # The client span is the parent's child and the server span, active in the
  # handler, is the client span's child.
  def check_parents(parent, caller, receiver, handler)
    raise "client span parent" unless caller.parent_span_id == parent.context.span_id
    raise "server span parent" unless receiver.parent_span_id == caller.span_id
    raise "server context" unless receiver.span_id == handler.span_id
  end

  %i[threaded inline].each do |path|
    it "exports the same propagated client spans as the #{path == :inline ? 'H2 Transport' : 'ThreadedTransport'} " \
       "on the #{path} path" do
      observed = {}
      entered = Thread::Queue.new
      gates = {}
      peer = traced_host(observed, entered, gates)
      origin = "http://127.0.0.1:#{peer.peer.port}"
      run = lambda do |transport|
        release = gates[:release] = Thread::Queue.new
        exporter.reset
        results, parents = if path == :inline
                             reactor { traced_calls(transport, origin, entered, release) }
                           else
                             value_within(Thread.new { traced_calls(transport, origin, entered, release) },
                                          "traced calls")
                           end
        expect(results).to eq(%w[ok canceled])
        eventually("four exported RPC spans") { rpc_spans(exporter).size == 4 }
        summarize(parents, observed)
      end
      per_call = run.call(ConnectRPC::Async::PerCallTransport.new)
      reference = if path == :inline
                    reactor do
                      transport = ConnectRPC::Async::Transport.new(origin: origin)
                      run.call(transport)
                    ensure
                      transport&.close(grace: 0)
                    end
                  else
                    transport = ThreadedHelpers::THREADED.new(origin: origin)
                    begin
                      run.call(transport)
                    ensure
                      transport.close(grace: 0)
                    end
                  end
      expect(per_call).to eq(reference)
      expect(per_call.map(&:last)).to eq([OpenTelemetry::Trace::Status::UNSET, OpenTelemetry::Trace::Status::ERROR])
      expect(per_call.last[1]).to include("rpc.status_code" => "canceled")
    ensure
      gates&.fetch(:release, nil)&.<<(true)
      peer&.stop
    end
  end
end
