# frozen_string_literal: true

RSpec.describe "Propagator selection" do
  # Serializes the span context under its own header, unlike any W3C field.
  let(:header_propagator) do
    Class.new do
      def initialize(name) = @name = name

      def fields = [@name]

      def inject(carrier, context: OpenTelemetry::Context.current, setter: nil) # rubocop:disable Lint/UnusedMethodArgument
        span_context = OpenTelemetry::Trace.current_span(context).context
        carrier[@name] = "#{span_context.hex_trace_id}-#{span_context.hex_span_id}" if span_context.valid?
      end

      def extract(carrier, context: OpenTelemetry::Context.current, getter: nil)
        trace_id, span_id = getter.get(carrier, @name)&.split("-")
        return context unless trace_id && span_id

        parent = OpenTelemetry::Trace::SpanContext.new(trace_id: [trace_id].pack("H*"), span_id: [span_id].pack("H*"),
                                                       trace_flags: OpenTelemetry::Trace::TraceFlags::SAMPLED,
                                                       remote: true)
        OpenTelemetry::Trace.context_with_span(OpenTelemetry::Trace.non_recording_span(parent), parent_context: context)
      end
    end
  end
  let(:message) { AsyncHelpers::MESSAGE.new(name: "propagated") }

  def subscriber(provider, **) = ConnectRPC::OTel::Subscriber.new(tracer_provider: provider, **)

  # One in-process round trip; returns the request metadata the server received.
  def round_trip(provider, subscriber)
    bus = ConnectRPC::Instrumentation.new(subscribers: [subscriber])
    seen = nil
    target = server(instrumentation: bus, echo: lambda { |request, context|
      seen = context.request_metadata.to_h
      request
    })
    typed = client(ConnectRPC::Testing::Transport.new(server: target), "https://in-process.invalid",
                   instrumentation: bus)
    parent = provider.tracer("test").start_span("parent")
    OpenTelemetry::Trace.with_span(parent) { typed.echo(message) }
    seen
  ensure
    parent&.finish
  end

  def linked?(exporter)
    caller = rpc_spans(exporter).find { |span| span.kind == :client }
    receiver = rpc_spans(exporter).find { |span| span.kind == :server }
    receiver.parent_span_id == caller.span_id && receiver.trace_id == caller.trace_id
  end

  it "injects and extracts with the global propagator by default" do
    provider, exporter, = tracing
    OpenTelemetry.propagation = header_propagator.new("x-trace")
    seen = round_trip(provider, subscriber(provider))
    expect(seen).to have_key("x-trace")
    expect(seen).not_to include("traceparent", "baggage")
    expect(linked?(exporter)).to be(true)
  end

  it "uses a global propagator the application installs after constructing the subscriber" do
    provider, exporter, = tracing
    default = subscriber(provider)
    OpenTelemetry.propagation = header_propagator.new("x-late")
    seen = round_trip(provider, default)
    expect(seen).to have_key("x-late")
    expect(seen).not_to have_key("traceparent")
    expect(linked?(exporter)).to be(true)
  end

  it "propagates nothing through a no-op global propagator" do
    provider, exporter, = tracing
    OpenTelemetry.propagation = OpenTelemetry::Context::Propagation::NoopTextMapPropagator.new
    seen = round_trip(provider, subscriber(provider))
    expect(seen).not_to include("traceparent", "tracestate", "baggage")
    receiver = rpc_spans(exporter).find { |span| span.kind == :server }
    expect(receiver.parent_span_id).to eq(OpenTelemetry::Trace::INVALID_SPAN_ID)
    expect(linked?(exporter)).to be(false)
  end

  it "prefers an explicit propagator over the global one" do
    provider, exporter, = tracing
    OpenTelemetry.propagation = header_propagator.new("x-global")
    seen = round_trip(provider, subscriber(provider, propagator: header_propagator.new("x-explicit")))
    expect(seen).to have_key("x-explicit")
    expect(seen).not_to include("x-global", "traceparent")
    expect(linked?(exporter)).to be(true)
  end
end
