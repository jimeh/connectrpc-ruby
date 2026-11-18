# frozen_string_literal: true

require "stringio"

RSpec.describe "Lifecycle request metadata" do
  let(:binding) do
    Object.new.tap do |codec|
      codec.define_singleton_method(:encode) { |message, **| message.b }
      codec.define_singleton_method(:decode) { |bytes, **| bytes }
    end
  end
  let(:status_codec) do
    require "connectrpc/bindings/google_protobuf"
    ConnectRPC::Bindings::GoogleProtobuf.new
  end
  let(:outcomes) { [] }
  let(:errors) { [] }
  let(:bus) do
    log = outcomes
    recording = Object.new
    recording.define_singleton_method(:start) do |_|
      Object.new.tap { |observer| observer.define_singleton_method(:processing_finished) { |outcome| log << outcome } }
    end
    ConnectRPC::Instrumentation.new(subscribers: [recording], on_error: ->(error) { errors << error })
  end
  let(:kept) { { "x-keep" => ["kept"] } }

  def declaration(kind = ConnectRPC::Method, **)
    kind.new(service: "metadata.Test", name: "Call", request_type: :request, response_type: :response, **)
  end

  def server(rpc, handler, instrumentation: bus, **)
    ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: rpc.service, rpcs: [rpc])], binding: binding,
                           handlers: { rpc.path => handler }, instrumentation: instrumentation,
                           status_codec: status_codec, **)
  end

  def frame(bytes, flag = 0) = [flag, bytes.bytesize].pack("CN") + bytes

  def sink
    Object.new.tap do |output|
      output.define_singleton_method(:write) { |_chunk, **| nil }
      output.define_singleton_method(:finish) { |**| nil }
      output.define_singleton_method(:abort) { nil }
    end
  end

  # Native inputs take a timeout keyword.
  def input(bytes)
    StringIO.new(bytes).tap { |source| source.define_singleton_method(:read) { |length, **| super(length) } }
  end

  def native_grpc(target, path, headers, body: frame("hello"))
    headers = { "content-type" => ["application/grpc"] }.merge(headers)
    target.call_grpc(path: path, http_method: "POST", headers: headers, input: input(body), output: sink,
                     publish: ->(_) {}, http2: true)
  end

  def web(target, path, headers, body: frame("hello"))
    target.call(path: path, http_method: "POST",
                headers: { "content-type" => ["application/grpc-web+proto"] }.merge(headers), input: StringIO.new(body))
  end

  describe "recorded when processing begins" do
    {
      "an invalid timeout" => { "grpc-timeout" => ["bogus"] },
      "an unsupported encoding" => { "grpc-encoding" => ["br"] }
    }.each do |label, fields|
      it "keeps native gRPC request metadata after #{label}" do
        rpc = declaration
        native_grpc(server(rpc, ->(*) { raise "invoked" }, protocol: :grpc), rpc.path, kept.merge(fields))
        expect(outcomes.size).to eq(1)
        expect(outcomes.first.code).not_to eq("OK")
        expect(outcomes.first.request_metadata).to eq(kept)
      end
    end

    it "keeps native gRPC request metadata for an unknown method" do
      rpc = declaration
      native_grpc(server(rpc, ->(*) { raise "invoked" }, protocol: :grpc), "/metadata.Test/Missing", kept)
      expect(outcomes.first.code).to eq("unimplemented")
      expect(outcomes.first.request_metadata).to eq(kept)
    end

    it "keeps gRPC-Web request metadata after an early failure" do
      rpc = declaration
      response = web(server(rpc, ->(*) { raise "invoked" }, protocol: :grpc_web), rpc.path,
                     kept.merge("grpc-timeout" => ["bogus"]))
      expect(response.status).to eq(200)
      expect(outcomes.first.code).not_to eq("OK")
      expect(outcomes.first.request_metadata).to eq(kept)
    end

    it "keeps Connect unary request metadata when header validation fails" do
      rpc = declaration
      response = server(rpc, ->(*) { raise "invoked" }).call(
        path: rpc.path, http_method: "POST", input: StringIO.new("hello"),
        headers: kept.merge("content-type" => ["application/proto"], "connect-protocol-version" => ["2"])
      )
      expect(response.status).to eq(400)
      expect(outcomes.first.request_metadata).to eq(kept)
    end

    describe "Connect GET" do
      let(:rpc) { declaration(idempotency: :no_side_effects) }
      # Mixed-case names normalize; an invalid field is omitted without losing its neighbour.
      let(:headers) { { "X-Keep" => "kept", "x-bad" => ["line\nbreak"] } }

      def get(query)
        server(rpc, ->(message, _) { message }).call(path: rpc.path, http_method: "GET", headers: headers,
                                                     input: StringIO.new(""), query: query)
      end

      it "records normalized valid metadata for a successful call" do
        expect(get("encoding=proto&message=aGk").status).to eq(200)
        expect(outcomes.first.request_metadata).to eq(kept)
      end

      it "records metadata when the query is rejected before negotiation completes" do
        expect(get("encoding=xml&message=aGk").status).to eq(415)
        expect(outcomes.first.request_metadata).to eq(kept)
      end
    end
  end

  describe "server Context request metadata" do
    it "is writable in a native gRPC unary handler" do
      rpc = declaration
      written = nil
      handler = lambda do |message, context|
        context.request_metadata["x-added"] = ["new"]
        written = context.request_metadata.to_h
        message
      end
      native_grpc(server(rpc, handler, protocol: :grpc), rpc.path, kept)
      expect(written).to eq(kept.merge("x-added" => ["new"]))
      expect(outcomes.first.code).to eq("OK")
      expect(outcomes.first.request_metadata).to eq(kept)
    end

    it "is writable in a gRPC-Web unary handler" do
      rpc = declaration
      written = nil
      handler = lambda do |message, context|
        context.request_metadata["x-added"] = ["new"]
        written = context.request_metadata.to_h
        message
      end
      expect(web(server(rpc, handler, protocol: :grpc_web), rpc.path, kept).status).to eq(200)
      expect(written).to eq(kept.merge("x-added" => ["new"]))
      expect(outcomes.first.code).to eq("OK")
      expect(outcomes.first.request_metadata).to eq(kept)
    end

    it "stays writable after a bidi handler reads the frozen session headers" do
      rpc = declaration(ConnectRPC::BidiStreamMethod)
      captured = nil
      written = nil
      handler = lambda do |duplex, context|
        duplex.receive
        captured = duplex.headers
        context.request_metadata["x-keep"] = ["changed"]
        written = context.request_metadata["x-keep"]
      end
      server(rpc, handler).call_stream(
        path: rpc.path, http_method: "POST", headers: kept.merge("content-type" => ["application/connect+proto"]),
        input: input(frame("one") + frame("{}", 2)), output: sink, publish: ->(_) {}, http2: true
      )
      expect(written).to eq(["changed"])
      expect(captured).to be_frozen
      expect(captured.to_h).to eq(kept)
      expect(outcomes.first.code).to eq("OK")
      expect(outcomes.first.request_metadata).to eq(kept)
    end
  end

  describe "client baggage injection" do
    let(:injector) do
      Object.new.tap do |subscriber|
        subscriber.define_singleton_method(:start) do |call|
          call.carrier["baggage"] = ["injected=1"]
          call.carrier["x-replace"] = "injected"
          nil
        end
      end
    end
    let(:injecting) { ConnectRPC::Instrumentation.new(subscribers: [injector], on_error: ->(e) { errors << e }) }
    let(:metadata) { { "baggage" => ["caller=1"], "x-replace" => ["caller"] }.freeze }

    def expect_merged(headers)
      expect(headers["baggage"]).to eq(["caller=1", "injected=1"])
      expect(headers["x-replace"]).to eq(["injected"])
      expect(metadata).to eq("baggage" => ["caller=1"], "x-replace" => ["caller"])
      expect(errors).to be_empty
    end

    # Captures the opened request's headers, then fails the stream before any exchange work.
    def capturing_transport(protocol, seen)
      open = lambda do |request, &|
        seen << request.headers
        raise ConnectRPC::Error.new(:unavailable, "captured")
      end
      Object.new.tap do |transport|
        if protocol == :grpc
          transport.define_singleton_method(:grpc_capabilities) { %i[unary server_stream client_stream bidi_stream] }
          transport.define_singleton_method(:open_grpc_stream, &open)
        else
          transport.define_singleton_method(:streaming_capabilities) { %i[server_stream client_stream bidi_stream] }
          transport.define_singleton_method(:open_stream, &open)
        end
      end
    end

    def stream_client(protocol, seen)
      ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding, protocol: protocol,
                             status_codec: (status_codec if protocol == :grpc),
                             transport: capturing_transport(protocol, seen), instrumentation: injecting)
    end

    it "appends injected baggage to caller baggage on a Connect unary call" do
      headers = nil
      transport = lambda do |request|
        headers = request.headers
        ConnectRPC::TransportResponse.new(status: 200, headers: { "content-type" => ["application/proto"] },
                                          body: "ok")
      end
      client = ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding, transport: transport,
                                      instrumentation: injecting)
      expect(client.call(declaration, "hi", metadata: metadata).message).to eq("ok")
      expect_merged(headers)
    end

    it "appends injected baggage to caller baggage on a gRPC unary call" do
      seen = []
      expect { stream_client(:grpc, seen).call(declaration, "hi", metadata: metadata) }
        .to raise_error(ConnectRPC::Error, "captured")
      expect_merged(seen.first)
    end

    %i[connect grpc].each do |protocol|
      it "appends injected baggage to caller baggage on a #{protocol} server stream" do
        seen = []
        expect do
          stream_client(protocol, seen).server_stream(declaration(ConnectRPC::ServerStreamMethod), "hi",
                                                      metadata: metadata, &:receive)
        end.to raise_error(ConnectRPC::Error, "captured")
        expect_merged(seen.first)
      end
    end
  end
end
