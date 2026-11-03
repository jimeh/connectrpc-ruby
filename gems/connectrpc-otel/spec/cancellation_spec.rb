# frozen_string_literal: true

RSpec.describe "Cancellation tracing" do
  it "exports delivered server canceled separately from client error and restores ambient context exactly once" do
    provider, exporter, bus = tracing
    source = ConnectRPC::CancellationSource.new
    target = server(instrumentation: bus, echo: lambda { |message, context|
      source.cancel
      context.check!
      message
    })
    typed = client(ConnectRPC::Testing::Transport.new(server: target), "http://in-process.invalid",
                   instrumentation: bus)
    parent = provider.tracer("test").start_span("parent")
    original = OpenTelemetry::Context.current
    OpenTelemetry::Trace.with_span(parent) do
      expect do
        typed.echo(AsyncHelpers::MESSAGE.new(name: "private"), timeout: nil, cancellation: source.token)
      end.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("canceled") }
      expect(current_span).to equal(parent)
    end
    expect(OpenTelemetry::Context.current).to equal(original)
    spans = rpc_spans(exporter)
    expect(spans.size).to eq(2)
    spans.each do |span|
      expect(span.attributes).to include("rpc.response.status_code" => "canceled",
                                         "connectrpc.processing.state" => "finished",
                                         "connectrpc.delivery" => "unobserved")
      expect(span.status.code).to eq(span.kind == :client ? OpenTelemetry::Trace::Status::ERROR : OpenTelemetry::Trace::Status::UNSET)
      expect(Array(span.events)).to be_empty
      expect(span.attributes.to_s).not_to include("private")
    end
  ensure
    parent&.finish
  end

  %i[async falcon].each do |backend|
    it "finishes #{backend} cancellation on its executing fibers after a foreign thread only signals" do
      provider, exporter, = tracing
      reactor do |task|
        entered = Async::Queue.new
        release = Async::Queue.new
        source = ConnectRPC::CancellationSource.new
        context = nil
        callbacks = []
        observer = Object.new
        observer.define_singleton_method(:detach) { |_| callbacks << [Thread.current, Fiber.current] }
        observer.define_singleton_method(:processing_finished) { |outcome| callbacks << outcome }
        observer.define_singleton_method(:finished) { |completion| callbacks << completion }
        subscriber = Object.new
        subscriber.define_singleton_method(:start) { |_| observer }
        tracing_subscriber = ConnectRPC::OTel::Subscriber.new(tracer_provider: provider)
        observed_bus = ConnectRPC::Instrumentation.new(subscribers: [tracing_subscriber, subscriber])
        target = server(instrumentation: observed_bus, echo: lambda { |message, current|
          context = current
          entered.enqueue(true)
          release.dequeue
          current.check!
          message
        })
        runner = lambda do |_host, origin|
          transport = ConnectRPC::Async::Transport.new(origin: origin)
          typed = client(transport, origin, instrumentation: observed_bus)
          parent = provider.tracer("test").start_span("parent")
          execution_thread = Thread.current
          call = task.async(finished: false) do
            OpenTelemetry::Trace.with_span(parent) do
              typed.echo(AsyncHelpers::MESSAGE.new(name: "cancel"), timeout: nil, cancellation: source.token)
            ensure
              expect(current_span).to equal(parent)
            end
          end
          entered.dequeue
          worker = Thread.new { source.cancel }
          worker.join
          expect { call.wait }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("canceled") }
          wait_until(task, "native Context cancellation") { context.cancelled? }
          release.enqueue(true)
          wait_until(task, "two exported canceled spans") { rpc_spans(exporter).size == 2 }
          spans = rpc_spans(exporter)
          expect(spans.map { |span| span.attributes["rpc.response.status_code"] }).to eq(%w[canceled canceled])
          receiver = spans.find { |span| span.kind == :server }
          expect(receiver.attributes["connectrpc.delivery"]).to eq("aborted")
          expect(receiver.status.code).to eq(OpenTelemetry::Trace::Status::ERROR)
          expect(callbacks.grep(ConnectRPC::Instrumentation::Outcome).size).to eq(2)
          expect(callbacks.grep(ConnectRPC::Instrumentation::Completion).size).to eq(2)
          expect(callbacks.grep(Array).map(&:first)).to all(equal(execution_thread))
          expect(current_span.context).not_to be_valid
          expect(spans.flat_map { |span| Array(span.events) }).to be_empty
        ensure
          release.enqueue(true)
          transport&.close(grace: 0)
          parent&.finish
        end
        if backend == :async
          host = ConnectRPC::Async::Host.new(server: target).start
          runner.call(host, "http://127.0.0.1:#{host.port}")
        else
          falcon(server: target, &runner)
        end
      ensure
        host&.close(grace: 0)
      end
    end
  end
end
