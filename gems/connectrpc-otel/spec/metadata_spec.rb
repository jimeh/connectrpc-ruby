# frozen_string_literal: true

require "stringio"

RSpec.describe "Error types and propagated metadata" do
  let(:binding) do
    Object.new.tap do |codec|
      codec.define_singleton_method(:encode) { |message, **| message.b }
      codec.define_singleton_method(:decode) { |bytes, **| bytes }
    end
  end
  let(:rpc) { ConnectRPC::Method.new(service: "trace.Meta", name: "Call", request_type: :request, response_type: :response) }

  def server(bus, protocol:, **)
    ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: rpc.service, methods: [rpc])], binding: binding,
                           handlers: { rpc.path => ->(message, _) { message } }, protocol: protocol,
                           status_codec: ConnectRPC::Bindings::GoogleProtobuf.new, instrumentation: bus, **)
  end

  def span_attributes(exporter)
    spans = rpc_spans(exporter)
    expect(spans.size).to eq(1)
    spans.first.attributes
  end

  describe "error.type casing" do
    def native_grpc(target, path: rpc.path)
      output = Object.new
      output.define_singleton_method(:write) { |_chunk, **| nil }
      output.define_singleton_method(:finish) { |**| nil }
      input = StringIO.new("#{[0, 5].pack('CN')}hello")
      input.define_singleton_method(:read) { |length, **| super(length) }
      target.call_grpc(path: path, http_method: "POST", headers: { "content-type" => ["application/grpc"] },
                       input: input, output: output, publish: ->(_) {}, http2: true)
    end

    it "keeps a native gRPC operation failure lowercase beside an OK status" do
      _, exporter, bus = tracing
      invoker = lambda do |_method, request, context|
        context.record_failure(RuntimeError.new("recorded"))
        request
      end
      native_grpc(server(bus, protocol: :grpc, invoker: invoker))
      attributes = span_attributes(exporter)
      expect(attributes["error.type"]).to eq("operation_failed")
      expect(attributes["rpc.response.status_code"]).to eq("OK")
    end

    it "uppercases a native gRPC status code" do
      _, exporter, bus = tracing
      native_grpc(server(bus, protocol: :grpc), path: "/trace.Meta/Missing")
      attributes = span_attributes(exporter)
      expect(attributes["error.type"]).to eq("UNIMPLEMENTED")
      expect(attributes["rpc.response.status_code"]).to eq("UNIMPLEMENTED")
    end

    it "keeps a portable gRPC preflight rejection lowercase" do
      _, exporter, bus = tracing
      response = server(bus, protocol: :grpc).call(path: rpc.path, http_method: "POST", input: StringIO.new(""),
                                                   headers: { "content-type" => ["application/grpc"] })
      expect(response.status).to eq(415)
      attributes = span_attributes(exporter)
      expect(attributes["rpc.system.name"]).to eq("grpc")
      expect(attributes["error.type"]).to eq("preflight_rejected")
      expect(attributes).not_to have_key("rpc.response.status_code")
    end

    it "keeps a gRPC-Web preflight rejection lowercase" do
      _, exporter, bus = tracing
      response = server(bus, protocol: :grpc_web).call(path: rpc.path, http_method: "GET", input: StringIO.new(""),
                                                       headers: { "content-type" => ["application/grpc-web"] })
      expect(response.status).to eq(405)
      attributes = span_attributes(exporter)
      expect(attributes["connectrpc.protocol"]).to eq("grpc_web")
      expect(attributes["error.type"]).to eq("preflight_rejected")
    end
  end

  describe "client baggage" do
    let(:otel_baggage) { OpenTelemetry::Baggage.set_value("tenant", "acme") }

    # Stages baggage before the OTel subscriber, as an earlier propagation subscriber would.
    let(:stager) do
      Object.new.tap do |subscriber|
        subscriber.define_singleton_method(:start) do |call|
          call.carrier["baggage"] = ["staged=1"]
          nil
        end
      end
    end

    def wire_headers(bus, metadata: {})
      headers = nil
      transport = lambda do |request|
        headers = request.headers
        ConnectRPC::TransportResponse.new(status: 200, headers: { "content-type" => ["application/proto"] },
                                          body: "ok")
      end
      client = ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding, transport: transport,
                                      instrumentation: bus)
      OpenTelemetry::Context.with_current(otel_baggage) { client.call(rpc, "hi", metadata: metadata) }
      headers
    end

    def otel_bus(*before, propagator: nil)
      subscriber = ConnectRPC::OTel::Subscriber.new(tracer_provider: OpenTelemetry::SDK::Trace::TracerProvider.new,
                                                    propagator: propagator)
      errors = []
      bus = ConnectRPC::Instrumentation.new(subscribers: [*before, subscriber], on_error: ->(error) { errors << error })
      [bus, errors]
    end

    it "keeps caller baggage ahead of injected OpenTelemetry baggage" do
      bus, errors = otel_bus
      headers = wire_headers(bus, metadata: { "baggage" => ["caller=1"] })
      expect(headers["baggage"]).to eq(["caller=1", "tenant=acme"])
      expect(headers["traceparent"].size).to eq(1)
      expect(errors).to be_empty
    end

    it "keeps baggage staged by an earlier subscriber ahead of its own" do
      bus, errors = otel_bus(stager)
      expect(wire_headers(bus)["baggage"]).to eq(["staged=1", "tenant=acme"])
      expect(errors).to be_empty
    end

    it "injects through a custom carrier-and-context propagator" do
      propagator = Object.new
      propagator.define_singleton_method(:extract) { |_carrier, context:, **| context }
      propagator.define_singleton_method(:inject) do |carrier, context:|
        raise ArgumentError, "missing context" unless context.is_a?(OpenTelemetry::Context)

        carrier["x-custom"] = "1"
        carrier["baggage"] = "custom=1"
      end
      bus, errors = otel_bus(stager, propagator: propagator)
      headers = wire_headers(bus, metadata: { "baggage" => ["caller=1"] })
      expect(headers["x-custom"]).to eq(["1"])
      expect(headers["baggage"]).to eq(["caller=1", "staged=1", "custom=1"])
      expect(errors).to be_empty
    end

    {
      "appends to it" => [->(carrier) { carrier["baggage"] = [*carrier["baggage"], "custom=1"] },
                          ["staged=1", "custom=1"]],
      "prepends to it" => [->(carrier) { carrier["baggage"] = ["custom=1", *carrier["baggage"]] },
                           ["staged=1", "custom=1"]],
      "replaces it in place" => [->(carrier) { carrier["baggage"].replace(["custom=1"]) },
                                 ["staged=1", "custom=1"]],
      "appends a copy of a staged entry" => [->(carrier) { carrier["baggage"] = [*carrier["baggage"], "staged=1"] },
                                             ["staged=1", "staged=1"]],
      "joins a member onto it" => [->(carrier) { carrier["baggage"] = [*carrier["baggage"], "custom=1"].join(",") },
                                   ["staged=1", "custom=1"]],
      "clears it" => [->(carrier) { carrier["baggage"].clear }, ["staged=1"]],
      "deletes it" => [->(carrier) { carrier.delete("baggage") }, ["staged=1"]]
    }.each do |label, (edit, expected)|
      it "keeps staged baggage exactly once when a custom propagator #{label}" do
        propagator = Object.new
        propagator.define_singleton_method(:extract) { |_carrier, context:, **| context }
        propagator.define_singleton_method(:inject) { |carrier, context:| edit.call(carrier) } # rubocop:disable Lint/UnusedBlockArgument
        bus, errors = otel_bus(stager, propagator: propagator)
        expect(wire_headers(bus)["baggage"]).to eq(expected)
        expect(errors).to be_empty
      end
    end

    it "leaves invalid propagator baggage for core to reject the whole injection" do
      propagator = Object.new
      propagator.define_singleton_method(:extract) { |_carrier, context:, **| context }
      propagator.define_singleton_method(:inject) do |carrier, context:| # rubocop:disable Lint/UnusedBlockArgument
        carrier["x-custom"] = "1"
        carrier["baggage"] = "custom=1\n"
      end
      bus, errors = otel_bus(stager, propagator: propagator)
      headers = wire_headers(bus, metadata: { "baggage" => ["caller=1"] })
      expect(headers["baggage"]).to eq(["caller=1"])
      expect(headers).not_to have_key("x-custom")
      expect(errors.size).to eq(1)
    end

    it "keeps staged baggage when a custom propagator mutates a staged string in place" do
      mutable_stager = Object.new
      mutable_stager.define_singleton_method(:start) do |call|
        call.carrier["baggage"] = [String.new("staged=1")]
        nil
      end
      propagator = Object.new
      propagator.define_singleton_method(:extract) { |_carrier, context:, **| context }
      propagator.define_singleton_method(:inject) { |carrier, context:| carrier["baggage"].first.replace("custom=1") } # rubocop:disable Lint/UnusedBlockArgument
      bus, errors = otel_bus(mutable_stager, propagator: propagator)
      expect(wire_headers(bus)["baggage"]).to eq(["staged=1", "custom=1"])
      expect(errors).to be_empty
    end
  end
end
