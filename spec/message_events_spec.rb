# frozen_string_literal: true

require "stringio"

RSpec.describe "Per-message lifecycle callbacks" do
  let(:binding) do
    Object.new.tap do |codec|
      codec.define_singleton_method(:encode) { |message, **| message.b }
      codec.define_singleton_method(:decode) do |bytes, **|
        raise ConnectRPC::CodecError, "undecodable" if bytes.start_with?("bad")

        bytes
      end
    end
  end
  let(:status_codec) do
    require "connectrpc/bindings/google_protobuf"
    ConnectRPC::Bindings::GoogleProtobuf.new
  end
  let(:events) { [] }
  let(:errors) { [] }
  let(:subscriber) do
    log = events
    Object.new.tap do |recording|
      recording.define_singleton_method(:start) do |call|
        Object.new.tap do |observer|
          observer.define_singleton_method(:message_received) { |bytes| log << [call.role, :received, bytes] }
          observer.define_singleton_method(:message_sent) { |bytes| log << [call.role, :sent, bytes] }
          observer.define_singleton_method(:finished) { |_| log << [call.role, :finished] }
        end
      end
    end
  end
  let(:bus) { ConnectRPC::Instrumentation.new(subscribers: [subscriber], on_error: ->(error) { errors << error }) }
  let(:payload) { "abc" * 100 }

  def declaration(kind = ConnectRPC::Method, **)
    kind.new(service: "events.Test", name: "Call", request_type: :request, response_type: :response, **)
  end

  def frame(bytes, flag = 0) = [flag, bytes.bytesize].pack("CN") + bytes
  def gzip(bytes) = ConnectRPC::Compression.encode(bytes, limit: 1 << 20)

  def messages(role, kind) = events.filter_map { |entry| entry.last if entry.first(2) == [role, kind] }

  def server(rpc, handler, **)
    ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: rpc.service, methods: [rpc])], binding: binding,
                           handlers: { rpc.path => handler }, instrumentation: bus, status_codec: status_codec, **)
  end

  # Records the request wire body; the server sees exactly what the client sent.
  def loopback(target, wire)
    transport = Object.new
    transport.define_singleton_method(:call) do |request|
      wire << request.body
      target.call(path: request.url.path, http_method: request.http_method, headers: request.headers,
                  input: StringIO.new(request.body), query: request.url.query || "")
    end
    transport
  end

  def output
    bytes = +"".b
    Object.new.tap do |sink|
      sink.define_singleton_method(:write) { |chunk, **| bytes << chunk }
      sink.define_singleton_method(:finish) { |**| nil }
      sink.define_singleton_method(:abort) { nil }
      sink.define_singleton_method(:bytes) { bytes }
    end
  end

  # Native inputs take a timeout keyword.
  def input(bytes)
    StringIO.new(bytes).tap { |source| source.define_singleton_method(:read) { |length, **| super(length) } }
  end

  def envelopes(bytes)
    result = []
    offset = 0
    while offset < bytes.bytesize
      flags, length = bytes.byteslice(offset, 5).unpack("CN")
      result << [flags, length]
      offset += 5 + length
    end
    result
  end

  describe "Connect unary" do
    [[:identity, false], [:gzip, false], [:identity, true], [:gzip, true]].each do |encoding, get|
      it "reports encoded #{encoding} #{get ? 'GET' : 'POST'} payload sizes on both roles" do
        rpc = declaration(idempotency: get ? :no_side_effects : :unknown)
        response = nil
        target = server(rpc, lambda { |message, _|
          response = "#{message}!" * 2
        }, encoding: encoding)
        wire = []
        client = ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding, use_get: get,
                                        transport: loopback(target, wire), encoding: encoding, instrumentation: bus)
        expect(client.call(rpc, payload).message).to eq(response)
        request_size = encoding == :gzip ? gzip(payload).bytesize : payload.bytesize
        response_size = encoding == :gzip ? gzip(response).bytesize : response.bytesize
        sent_body = encoding == :gzip ? gzip(payload) : payload
        expect(wire.first).to eq(get ? "" : sent_body)
        expect(messages(:client, :sent)).to eq([request_size])
        expect(messages(:server, :received)).to eq([request_size])
        expect(messages(:server, :sent)).to eq([response_size])
        expect(messages(:client, :received)).to eq([response_size])
        expect(request_size).to be < payload.bytesize if encoding == :gzip
      end
    end

    it "fires nothing for a request the server cannot decode or an error response" do
      rpc = declaration
      target = server(rpc, ->(*) { raise "unreached" })
      client = ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding,
                                      transport: loopback(target, []), instrumentation: bus)
      expect { client.call(rpc, "bad request") }.to raise_error(ConnectRPC::Error) { |error|
        expect(error.code).to eq("invalid_argument")
      }
      expect(messages(:client, :sent)).to eq([11])
      expect(messages(:server, :received)).to be_empty
      expect(messages(:server, :sent)).to be_empty
      expect(messages(:client, :received)).to be_empty
    end
  end

  describe "Connect streaming server" do
    it "reports gzip request envelopes as received and emitted envelopes as sent" do
      rpc = declaration(ConnectRPC::BidiStreamMethod)
      target = server(rpc, lambda { |duplex, _|
        while (message = duplex.receive)
          duplex.send_message(message * 3)
        end
      }, encoding: :gzip)
      sink = output
      body = frame(gzip(payload), 1) + frame(gzip("x"), 1)
      headers = { "content-type" => ["application/connect+proto"], "connect-content-encoding" => ["gzip"],
                  "connect-accept-encoding" => ["gzip"] }
      target.call_stream(path: rpc.path, http_method: "POST", headers: headers, input: input(body),
                         output: sink, publish: ->(_) {}, http2: true)
      expect(messages(:server, :received)).to eq([gzip(payload).bytesize, gzip("x").bytesize])
      data = envelopes(sink.bytes).reject { |flags, _| flags.anybits?(2) }
      expect(data.map(&:first)).to all(eq(1))
      expect(messages(:server, :sent)).to eq(data.map(&:last))
      expect(messages(:server, :sent)).to eq([gzip(payload * 3).bytesize, gzip("xxx").bytesize])
    end
  end

  describe "Connect streaming client" do
    def stream_client(body, write: ->(_) {}, kind: ConnectRPC::ServerStreamMethod)
      reader = StringIO.new(body)
      exchange = Object.new
      exchange.define_singleton_method(:response_head) do |**|
        ConnectRPC::StreamHead.new(status: 200, headers: { "content-type" => ["application/connect+proto"] })
      end
      exchange.define_singleton_method(:read) { |length, **| reader.read(length) }
      exchange.define_singleton_method(:write, &write)
      exchange.define_singleton_method(:close_send) { nil }
      exchange.define_singleton_method(:finish) { |**| nil }
      exchange.define_singleton_method(:abort) { nil }
      transport = Object.new
      transport.define_singleton_method(:streaming_capabilities) { %i[server_stream client_stream bidi_stream] }
      transport.define_singleton_method(:open_stream) { |_, &work| work.call(exchange) }
      [ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding, transport: transport,
                              instrumentation: bus), declaration(kind)]
    end

    it "reports each server-stream message live, before the stream finishes" do
      client, rpc = stream_client(frame("one") + frame("three") + frame("{}", 2))
      seen = []
      client.server_stream(rpc, "hi") do |reader|
        expect(reader.receive).to eq("one")
        seen << events.dup
        expect(reader.receive).to eq("three")
        expect(reader.receive).to be_nil
      end
      expect(seen.first).to eq([[:client, :sent, 2], [:client, :received, 3]])
      expect(messages(:client, :received)).to eq([3, 5])
      expect(events.last).to eq(%i[client finished])
    end

    it "reports client-stream sends and fires nothing for recovery discards or failed writes" do
      wrote = 0
      failing = lambda do |_|
        wrote += 1
        raise ConnectRPC::Error.new(:unavailable, "incidental") if wrote > 5
      end
      body = frame("discard") + frame('{"error":{"code":"not_found"}}', 2)
      client, rpc = stream_client(body, write: failing, kind: ConnectRPC::BidiStreamMethod)
      expect do
        client.bidi_stream(rpc) do |duplex|
          duplex.send_message("first")
          duplex.send_message("second")
          duplex.send_message("third")
        end
      end.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("not_found") }
      expect(messages(:client, :sent)).to eq([5, 6])
      expect(messages(:client, :received)).to be_empty
    end

    it "fires no receive when the client cannot decode a response message" do
      client, rpc = stream_client(frame("bad") + frame("{}", 2))
      expect { client.server_stream(rpc, "hi", &:receive) }.to raise_error(ConnectRPC::Error)
      expect(messages(:client, :received)).to be_empty
    end
  end

  describe "gRPC" do
    it "reports gzip request and response envelopes on a native server" do
      rpc = declaration
      target = server(rpc, ->(message, _) { message * 2 }, protocol: :grpc, encoding: :gzip)
      sink = output
      headers = { "content-type" => ["application/grpc"], "grpc-encoding" => ["gzip"],
                  "grpc-accept-encoding" => ["gzip"] }
      target.call_grpc(path: rpc.path, http_method: "POST", headers: headers, input: input(frame(gzip(payload), 1)),
                       output: sink,
                       publish: ->(_) {}, http2: true)
      expect(messages(:server, :received)).to eq([gzip(payload).bytesize])
      expect(envelopes(sink.bytes)).to eq([[1, gzip(payload * 2).bytesize]])
      expect(messages(:server, :sent)).to eq([gzip(payload * 2).bytesize])
    end

    it "reports server-stream responses on a native server" do
      rpc = declaration(ConnectRPC::ServerStreamMethod)
      target = server(rpc, lambda { |request, writer, _|
        writer.send_message(request)
        writer.send_message("#{request}!")
      }, protocol: :grpc)
      sink = output
      target.call_grpc(path: rpc.path, http_method: "POST", headers: { "content-type" => ["application/grpc"] },
                       input: input(frame("hello")), output: sink, publish: ->(_) {}, http2: true)
      expect(messages(:server, :received)).to eq([5])
      expect(messages(:server, :sent)).to eq([5, 6])
      expect(envelopes(sink.bytes)).to eq([[0, 5], [0, 6]])
    end

    it "reports unary and streaming client messages through the session" do
      reader = nil
      exchange = Object.new
      exchange.define_singleton_method(:response_head) do |**|
        ConnectRPC::GRPCHead.new(status: 200, end_stream: false, headers: { "content-type" => ["application/grpc"] })
      end
      exchange.define_singleton_method(:response_trailers) { { "grpc-status" => ["0"] } }
      exchange.define_singleton_method(:read) { |length, **| reader.read(length) }
      exchange.define_singleton_method(:write) { |_| nil }
      exchange.define_singleton_method(:close_send) { nil }
      exchange.define_singleton_method(:finish) { |**| nil }
      exchange.define_singleton_method(:abort) { nil }
      transport = Object.new
      transport.define_singleton_method(:grpc_capabilities) { %i[unary server_stream client_stream bidi_stream] }
      transport.define_singleton_method(:open_grpc_stream) { |_, &work| work.call(exchange) }
      client = ConnectRPC::Client.new(base_url: "https://example.invalid", protocol: :grpc, binding: binding,
                                      status_codec: status_codec, transport: transport, instrumentation: bus)
      reader = StringIO.new(frame("reply"))
      expect(client.call(declaration, "question").message).to eq("reply")
      reader = StringIO.new(frame("a") + frame("bc"))
      client.server_stream(declaration(ConnectRPC::ServerStreamMethod), "q") do |stream|
        stream.receive
        stream.receive
        expect(stream.receive).to be_nil
      end
      expect(messages(:client, :sent)).to eq([8, 1])
      expect(messages(:client, :received)).to eq([5, 1, 2])
    end
  end

  describe "gRPC-Web" do
    %w[application/grpc-web application/grpc-web-text].each do |media|
      it "reports binary payload sizes for #{media}" do
        rpc = declaration
        target = server(rpc, ->(message, _) { message * 2 }, protocol: :grpc_web)
        body = frame(payload)
        body = [body].pack("m0") if media.end_with?("text")
        response = target.call(path: rpc.path, http_method: "POST", headers: { "content-type" => [media] },
                               input: StringIO.new(body))
        expect(response.status).to eq(200)
        expect(messages(:server, :received)).to eq([payload.bytesize])
        expect(messages(:server, :sent)).to eq([payload.bytesize * 2])
      end
    end
  end

  describe "observer isolation" do
    it "keeps RPCs and sibling observers unchanged when a message callback raises" do
      failing = Object.new
      failing.define_singleton_method(:start) do |_|
        Object.new.tap do |observer|
          observer.define_singleton_method(:message_received) { |_| raise "received boom" }
          observer.define_singleton_method(:message_sent) { |_| raise "sent boom" }
        end
      end
      plain = Object.new
      completions = []
      plain.define_singleton_method(:start) do |_|
        Object.new.tap { |observer| observer.define_singleton_method(:finished) { |done| completions << done } }
      end
      bus = ConnectRPC::Instrumentation.new(subscribers: [failing, subscriber, plain],
                                            on_error: ->(error) { errors << error })
      rpc = declaration
      target = ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: rpc.service, methods: [rpc])],
                                      binding: binding, handlers: { rpc.path => ->(message, _) { message } },
                                      instrumentation: bus)
      client = ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding,
                                      transport: loopback(target, []), instrumentation: bus)
      expect(client.call(rpc, "hello").message).to eq("hello")
      expect(errors.map(&:message)).to eq(["sent boom", "received boom", "sent boom", "received boom"])
      expect(events.count { |entry| entry[1] != :finished }).to eq(4)
      expect(completions.map(&:outcome).map(&:code)).to eq(%w[OK OK])
    end
  end
end
