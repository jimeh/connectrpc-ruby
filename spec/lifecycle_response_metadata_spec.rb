# frozen_string_literal: true

require "stringio"

RSpec.describe "Lifecycle response headers, trailers and server authority" do
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
  let(:calls) { [] }
  let(:outcomes) { [] }
  let(:bus) do
    started = calls
    log = outcomes
    recording = Object.new
    recording.define_singleton_method(:start) do |call|
      started << call
      Object.new.tap { |observer| observer.define_singleton_method(:processing_finished) { |outcome| log << outcome } }
    end
    ConnectRPC::Instrumentation.new(subscribers: [recording])
  end

  def declaration(kind = ConnectRPC::Method)
    kind.new(service: "metadata.Test", name: "Call", request_type: :request, response_type: :response)
  end

  def server(rpc, handler, **)
    ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: rpc.service, rpcs: [rpc])], binding: binding,
                           handlers: { rpc.path => handler }, instrumentation: bus, status_codec: status_codec, **)
  end

  def frame(bytes, flag = 0) = [flag, bytes.bytesize].pack("CN") + bytes

  def input(bytes)
    StringIO.new(bytes).tap { |source| source.define_singleton_method(:read) { |length, **| super(length) } }
  end

  # Records stream writes and native gRPC terminal fields.
  def sink(written, finished)
    Object.new.tap do |output|
      output.define_singleton_method(:write) { |chunk, **| written << chunk }
      output.define_singleton_method(:finish) { |headers: nil, **| finished << headers }
      output.define_singleton_method(:abort) { nil }
    end
  end

  describe "Outcome construction" do
    let(:facts) do
      { code: "OK", http_status: 200, error: nil, normalized_error: nil, duration: 0.5, request_bytes: 1,
        response_bytes: 2, request_metadata: {}, response_metadata: { "x-head" => ["h"] } }
    end

    it "defaults trailers to a frozen empty snapshot for keyword and positional callers" do
      keyword = ConnectRPC::Instrumentation::Outcome.new(**facts)
      positional = ConnectRPC::Instrumentation::Outcome.new(*facts.values)
      [keyword, positional].each do |outcome|
        expect(outcome.response_trailers).to eq({})
        expect(outcome.response_trailers).to be_frozen
        expect(outcome.response_metadata).to eq("x-head" => ["h"])
      end
    end

    it "keeps supplied trailers through Data#with" do
      trailers = { "x-tail" => ["t"].freeze }.freeze
      outcome = ConnectRPC::Instrumentation::Outcome.new(**facts, response_trailers: trailers).with(duration: 1.0)
      expect(outcome.response_trailers).to equal(trailers)
      expect(outcome.duration).to eq(1.0)
    end
  end

  describe "Connect unary" do
    it "splits trailer-prefixed headers into server trailers on success and error" do
      rpc = declaration
      [nil, ConnectRPC::Error.new(:invalid_argument, "no", trailers: { "x-err" => "e" })].each do |error|
        handler = lambda do |message, context|
          context.response_metadata["x-head"] = ["h"]
          context.response_trailers["x-tail"] = ["t"]
          raise error if error

          message
        end
        response = server(rpc, handler).call(path: rpc.path, http_method: "POST", input: StringIO.new("hi"),
                                             headers: { "content-type" => ["application/proto"] })
        expect(response.headers).to include("trailer-x-tail" => ["t"])
        outcome = outcomes.last
        expect(outcome.response_metadata).to include("x-head" => ["h"])
        expect(outcome.response_metadata.keys).not_to include(a_string_starting_with("trailer-"))
        expect(outcome.response_trailers).to eq(error ? { "x-tail" => ["t"], "x-err" => ["e"] } : { "x-tail" => ["t"] })
        expect(outcome.response_trailers).to be_frozen
      end
    end

    it "neither writes nor observes a value-less trailer on success and error" do
      rpc = declaration
      [nil, ConnectRPC::Error.new(:invalid_argument, "no", trailers: { "x-err-empty" => [] })].each do |error|
        handler = lambda do |message, context|
          context.response_trailers["x-empty-list"] = []
          context.response_trailers["x-blank"] = [""]
          raise error if error

          message
        end
        response = server(rpc, handler).call(path: rpc.path, http_method: "POST", input: StringIO.new("hi"),
                                             headers: { "content-type" => ["application/proto"] })
        # Some Rack servers write an empty array as a blank field; the response never carries one.
        expect(response.headers.keys).not_to include("trailer-x-empty-list", "trailer-x-err-empty")
        expect(response.headers).to include("trailer-x-blank" => [""])
        expect(outcomes.last.response_trailers).to eq("x-blank" => [""])
      end
    end

    it "decodes mixed-case and repeated client trailers, dropping reserved and invalid names" do
      rpc = declaration
      headers = { "Content-Type" => ["application/json"], "X-Head" => ["h"], "Trailer-X-Tail" => ["a"],
                  "trailer-x-tail" => ["b"], "Trailer-Grpc-Status" => ["0"], "trailer-bad name" => ["x"] }
      body = JSON.generate("code" => "invalid_argument", "message" => "no")
      transport = ->(_request) { ConnectRPC::TransportResponse.new(status: 400, headers: headers, body: body) }
      client = ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding, transport: transport,
                                      instrumentation: bus)
      expect { client.call(rpc, "hi") }.to raise_error(ConnectRPC::Error) { |error|
        expect(error.trailers.to_h).to eq("x-tail" => %w[a b])
      }
      outcome = outcomes.last
      expect(outcome.code).to eq("invalid_argument")
      expect(outcome.response_metadata).to eq("content-type" => ["application/json"], "x-head" => ["h"])
      expect(outcome.response_trailers).to eq("x-tail" => %w[a b])
    end
  end

  describe "server stream terminals" do
    # x-big pushes the terminal over its byte limit, so a replacement carries no trailers.
    def handler(oversized)
      lambda do |context|
        context.response_metadata["x-head"] = ["h"]
        context.response_trailers["x-tail"] = ["kept"]
        context.response_trailers["x-big"] = ["a" * 300] if oversized
      end
    end

    [false, true].each do |oversized|
      it "observes the trailers the emitted Connect terminal carries (oversized: #{oversized})" do
        rpc = declaration(ConnectRPC::BidiStreamMethod)
        written = []
        apply = handler(oversized)
        target = server(rpc, ->(_duplex, context) { apply.call(context) }, terminal_limit: 200)
        target.call_stream(path: rpc.path, http_method: "POST",
                           headers: { "content-type" => ["application/connect+proto"] }, input: input(""),
                           output: sink(written, []), publish: ->(_) {}, http2: true)
        terminal = JSON.parse(written.join.byteslice(5..))
        outcome = outcomes.last
        expect(outcome.response_metadata).to eq("x-head" => ["h"])
        if oversized
          expect(terminal).to eq("error" => { "code" => "resource_exhausted" })
          expect(outcome.code).to eq("resource_exhausted")
          expect(outcome.response_trailers).to eq({})
        else
          expect(terminal).to eq("metadata" => { "x-tail" => ["kept"] })
          expect(outcome.response_trailers).to eq("x-tail" => ["kept"])
        end
      end

      it "observes the trailers the emitted gRPC status carries (oversized: #{oversized})" do
        rpc = declaration
        finished = []
        apply = handler(oversized)
        target = server(rpc, lambda { |message, context|
          apply.call(context)
          message
        }, protocol: :grpc, grpc_status_wire_limit: 200)
        target.call_grpc(path: rpc.path, http_method: "POST", headers: { "content-type" => ["application/grpc"] },
                         input: input(frame("hi")), output: sink([], finished), publish: ->(_) {}, http2: true)
        outcome = outcomes.last
        expect(outcome.response_metadata).to eq("x-head" => ["h"])
        if oversized
          expect(finished.last).to eq("grpc-status" => ["8"])
          expect(outcome.response_trailers).to eq({})
        else
          expect(finished.last).to include("x-tail" => ["kept"], "grpc-status" => ["0"])
          expect(outcome.response_trailers).to eq("x-tail" => ["kept"])
        end
      end

      it "observes the trailers the emitted gRPC-Web terminal carries (oversized: #{oversized})" do
        rpc = declaration
        apply = handler(oversized)
        target = server(rpc, lambda { |message, context|
          apply.call(context)
          message
        }, protocol: :grpc_web, grpc_status_wire_limit: 200)
        response = target.call(path: rpc.path, http_method: "POST", input: StringIO.new(frame("hi")),
                               headers: { "content-type" => ["application/grpc-web+proto"] })
        outcome = outcomes.last
        expect(outcome.response_metadata).to eq("x-head" => ["h"])
        expect(response.body.include?("x-tail: kept\r\n")).to be(!oversized)
        expect(response.body).to include(oversized ? "grpc-status: 8\r\n" : "grpc-status: 0\r\n")
        expect(outcome.response_trailers).to eq(oversized ? {} : { "x-tail" => ["kept"] })
      end
    end

    describe "value-less trailers" do
      let(:rpc) { declaration }
      let(:handler) do
        lambda do |message, context|
          context.response_trailers["x-empty-list"] = []
          context.response_trailers["x-blank"] = [""]
          message
        end
      end

      it "observes only the gRPC trailer fields that carry a value" do
        finished = []
        server(rpc, handler, protocol: :grpc)
          .call_grpc(path: rpc.path, http_method: "POST", headers: { "content-type" => ["application/grpc"] },
                     input: input(frame("hi")), output: sink([], finished), publish: ->(_) {}, http2: true)
        # A native host writes one field line per value.
        wire = finished.last.reject { |key, values| values.empty? || key.start_with?("grpc-") }
        expect(wire).to eq("x-blank" => [""])
        expect(outcomes.last.response_trailers).to eq(wire)
      end

      it "observes only the gRPC-Web terminal lines that carry a value" do
        response = server(rpc, handler, protocol: :grpc_web)
                   .call(path: rpc.path, http_method: "POST", input: StringIO.new(frame("hi")),
                         headers: { "content-type" => ["application/grpc-web+proto"] })
        terminal = response.body.byteslice((frame("hi").bytesize + 5)..)
        wire = terminal.split("\r\n").map { |line| line.split(": ", 2) }.reject { |key, _| key.start_with?("grpc-") }
        expect(wire).to eq([["x-blank", ""]])
        expect(outcomes.last.response_trailers).to eq("x-blank" => [""])
      end

      it "keeps a value-less trailer the Connect terminal JSON carries" do
        stream = declaration(ConnectRPC::BidiStreamMethod)
        written = []
        apply = handler
        server(stream, ->(_duplex, context) { apply.call(nil, context) })
          .call_stream(path: stream.path, http_method: "POST",
                       headers: { "content-type" => ["application/connect+proto"] }, input: input(""),
                       output: sink(written, []), publish: ->(_) {}, http2: true)
        terminal = JSON.parse(written.join.byteslice(5..))
        expect(terminal).to eq("metadata" => { "x-empty-list" => [], "x-blank" => [""] })
        expect(outcomes.last.response_trailers).to eq("x-empty-list" => [], "x-blank" => [""])
      end
    end

    it "observes gRPC error trailers and error metadata after a committed head" do
      rpc = declaration(ConnectRPC::ServerStreamMethod)
      handler = lambda do |_request, stream, context|
        context.response_trailers["x-tail"] = ["kept"]
        stream.send_message("one")
        raise ConnectRPC::Error.new(:not_found, "gone", metadata: { "x-meta" => "m" }, trailers: { "x-err" => "e" })
      end
      server(rpc, handler, protocol: :grpc)
        .call_grpc(path: rpc.path, http_method: "POST", headers: { "content-type" => ["application/grpc"] },
                   input: input(frame("hi")), output: sink([], []), publish: ->(_) {}, http2: true)
      expect(outcomes.last.code).to eq("not_found")
      expect(outcomes.last.response_trailers).to eq("x-tail" => ["kept"], "x-meta" => ["m"], "x-err" => ["e"])
    end
  end

  describe "client streams" do
    def connect_client(head, body)
      reader = StringIO.new(body)
      exchange = Object.new
      exchange.define_singleton_method(:response_head) { ConnectRPC::StreamHead.new(status: 200, headers: head) }
      exchange.define_singleton_method(:read) { |length, **| reader.read(length) }
      exchange.define_singleton_method(:write) { |_| nil }
      exchange.define_singleton_method(:close_send) { nil }
      exchange.define_singleton_method(:finish) { |**| nil }
      exchange.define_singleton_method(:abort) { nil }
      transport = Object.new
      transport.define_singleton_method(:streaming_capabilities) { %i[server_stream client_stream bidi_stream] }
      transport.define_singleton_method(:open_stream) { |_, &work| work.call(exchange) }
      ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding, transport: transport,
                             instrumentation: bus)
    end

    def grpc_client(head_fields:, trailers:, body: "", trailers_only: false)
      head = ConnectRPC::GRPCHead.new(status: 200, end_stream: trailers_only,
                                      headers: { "content-type" => ["application/grpc"] }
                                        .merge(head_fields, trailers_only ? trailers : {}))
      source = StringIO.new(body)
      exchange = Object.new
      exchange.define_singleton_method(:response_head) { |**| head }
      exchange.define_singleton_method(:response_trailers) { trailers }
      exchange.define_singleton_method(:read) { |length, **| source.read(length) }
      exchange.define_singleton_method(:write) { |_| nil }
      exchange.define_singleton_method(:close_send) { nil }
      exchange.define_singleton_method(:finish) { |**| nil }
      exchange.define_singleton_method(:abort) { nil }
      transport = Object.new
      transport.define_singleton_method(:grpc_capabilities) { %i[unary server_stream client_stream bidi_stream] }
      transport.define_singleton_method(:open_grpc_stream) { |_, &work| work.call(exchange) }
      ConnectRPC::Client.new(base_url: "https://example.invalid", protocol: :grpc, binding: binding,
                             status_codec: status_codec, transport: transport, instrumentation: bus)
    end

    let(:head) { { "content-type" => ["application/connect+proto"], "x-head" => ["h"] } }
    let(:terminal) { frame(JSON.generate("metadata" => { "x-tail" => ["t"] }), 2) }

    it "records the received Connect head and terminal trailers" do
      replies = []
      connect_client(head, frame("one") + terminal)
        .server_stream(declaration(ConnectRPC::ServerStreamMethod), "hi") do |stream|
        while (message = stream.receive)
          replies << message
        end
      end
      expect(replies).to eq(["one"])
      expect(outcomes.last.code).to eq("OK")
      expect(outcomes.last.response_metadata).to eq("x-head" => ["h"])
      expect(outcomes.last.response_trailers).to eq("x-tail" => ["t"])
    end

    it "keeps parsed Connect trailers when a later terminal check fails" do
      expect do
        connect_client(head, terminal).client_stream(declaration(ConnectRPC::ClientStreamMethod)) { nil }
      end.to raise_error(ConnectRPC::Error, "Expected exactly one response")
      expect(outcomes.last.code).to eq("unimplemented")
      expect(outcomes.last.response_metadata).to eq("x-head" => ["h"])
      expect(outcomes.last.response_trailers).to eq("x-tail" => ["t"])
    end

    it "records no headers for a rejected Connect head" do
      rejected = head.merge("content-type" => ["text/plain"])
      expect do
        connect_client(rejected, terminal).server_stream(declaration(ConnectRPC::ServerStreamMethod), "hi", &:receive)
      end.to raise_error(ConnectRPC::Error, "Unexpected streaming response content type")
      expect(outcomes.last.response_metadata).to eq({})
      expect(outcomes.last.response_trailers).to eq({})
    end

    it "records gRPC unary initial metadata and trailing metadata" do
      reply = grpc_client(head_fields: { "x-head" => ["h"] }, trailers: { "grpc-status" => ["0"], "x-tail" => ["t"] },
                          body: frame("one")).call(declaration, "hi")
      expect(reply.message).to eq("one")
      expect(outcomes.last.response_metadata).to eq("x-head" => ["h"])
      expect(outcomes.last.response_trailers).to eq("x-tail" => ["t"])
    end

    it "records a gRPC trailers-only response as trailers with empty headers" do
      client = grpc_client(head_fields: {}, trailers: { "grpc-status" => ["5"], "x-tail" => ["t"] },
                           trailers_only: true)
      expect { client.call(declaration, "hi") }.to raise_error(ConnectRPC::Error) { |error|
        expect(error.code).to eq("not_found")
      }
      expect(outcomes.last.response_metadata).to eq({})
      expect(outcomes.last.response_trailers).to eq("x-tail" => ["t"])
    end
  end

  describe "server authority" do
    let(:rpc) { declaration }

    def facts(*values, key: "host")
      headers = values.empty? ? {} : { key => values }
      server(rpc, ->(message, _) { message }).start_call(path: rpc.path, headers: headers)
      [calls.last.server_address, calls.last.server_port]
    end

    {
      "example.com" => ["example.com", nil], "Example.com:8443" => ["Example.com", 8443],
      "10.1.2.80:1" => ["10.1.2.80", 1], "[::1]" => ["::1", nil], "[2001:db8::1]:65535" => ["2001:db8::1", 65_535],
      "[::ffff:10.1.2.80]:80" => ["::ffff:10.1.2.80", 80], "valid.example:000443" => ["valid.example", 443],
      "example.com." => ["example.com.", nil], "my_host-1.example" => ["my_host-1.example", nil],
      "a" * 255 => ["a" * 255, nil]
    }.each do |authority, expected|
      it "records #{authority[0, 40]} as #{expected.map { |fact| fact.to_s[0, 40] }.inspect}" do
        expect(facts(authority)).to eq(expected)
      end
    end

    ["", "a" * 256, "example.com:", "example.com:0", "example.com:65536", "example.com:123456", "exa mple.com",
     "user@example.com", "example.com:80:90", "::1", "[::1", "[example.com]", "[:]", "[1:2:3]", "[:::::]",
     "[:::]:443", "[12345::1]", "[fe80::1%eth0]", "[::1/64]", "[10.1.2.80]", "-", "..", ".", "a..b", "-a.example",
     "a-.example", "example.com:0000", "example.com:#{'0' * 10}65536", "exämple.com", "bad\xff.example",
     "bad\xff.example".b].each do |authority|
      it "omits both facts for #{authority[0, 40].inspect} (#{authority.encoding})" do
        expect(facts(authority)).to eq([nil, nil])
      end
    end

    it "omits both facts for repeated or absent host values and reads a mixed-case name" do
      expect(facts("a.example", "b.example")).to eq([nil, nil])
      expect(facts).to eq([nil, nil])
      expect(facts("example.com:80", key: "Host")).to eq(["example.com", 80])
    end

    it "keeps an RPC with an invalid-encoding host successful while instrumented" do
      response = nil
      expect do
        response = server(rpc, ->(message, _) { message })
                   .call(path: rpc.path, http_method: "POST", input: StringIO.new("hi"),
                         headers: { "content-type" => ["application/proto"], "host" => ["bad\xff.example:443"] })
      end.not_to raise_error
      expect([response.status, response.body]).to eq([200, "hi"])
      expect(outcomes.last.code).to eq("OK")
      expect([calls.last.server_address, calls.last.server_port]).to eq([nil, nil])
    end
  end
end
