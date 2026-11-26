# frozen_string_literal: true

require "stringio"

RSpec.describe "Connect streaming framing and scope" do
  let(:engine) { ConnectRPC.const_get(:Streaming) }
  let(:binding) do
    Object.new.tap do |codec|
      codec.define_singleton_method(:encode) { |message, **| message.b }
      codec.define_singleton_method(:decode) { |bytes, **| bytes }
    end
  end
  let(:method_options) { { service: "test.Streams", name: "Run", request_type: String, response_type: String } }

  def frame(bytes, flags = 0)
    [flags, bytes.bytesize].pack("CN") + bytes
  end

  def declaration(type = ConnectRPC::ServerStreamMethod)
    type.new(**method_options)
  end

  def input(bytes, fragment: nil)
    reader = StringIO.new(bytes)
    reader.define_singleton_method(:read) do |length, **|
      super(fragment ? [length, fragment].min : length)
    end
    reader
  end

  def framing(bytes, request: false, encoding: :identity, **)
    engine::Framing.new(input: input(bytes, fragment: 1), request: request, wire_limit: 1024, limit: 1024,
                        encoding: encoding, **)
  end

  def transport(bytes, content_type: "application/connect+proto", encoding: nil)
    reader = input(bytes, fragment: 1)
    exchange = Object.new
    writes = []
    head = { "content-type" => [content_type], "x-head" => ["a"] }
    head["connect-content-encoding"] = [encoding] if encoding
    exchange.define_singleton_method(:response_head) { ConnectRPC::StreamHead.new(status: 200, headers: head) }
    exchange.define_singleton_method(:read) { |length, **| reader.read(length) }
    exchange.define_singleton_method(:write) { |part| writes << part }
    exchange.define_singleton_method(:close_send) { nil }
    exchange.define_singleton_method(:finish) { |**| nil }
    exchange.define_singleton_method(:abort) { nil }
    custom = Object.new
    custom.define_singleton_method(:streaming_capabilities) { %i[server_stream client_stream bidi_stream] }
    custom.define_singleton_method(:open_stream) do |request, &block|
      writes << request
      block.call(exchange)
    end
    [custom, exchange, writes]
  end

  def client(custom, **)
    ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding, transport: custom, **)
  end

  def expect_code(code, &)
    expect(&).to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq(code.to_s) }
  end

  it "keeps all three streaming declarations outside unary Method and accepts a mixed service" do
    unary = ConnectRPC::Method.new(**method_options, name: "Unary")
    methods = [unary, *[ConnectRPC::ServerStreamMethod, ConnectRPC::ClientStreamMethod, ConnectRPC::BidiStreamMethod]
      .each_with_index.map { |type, index| type.new(**method_options, name: "Run#{index}") }]
    expect(methods.drop(1)).to all(satisfy { |method| !method.is_a?(ConnectRPC::Method) })
    expect(ConnectRPC::Service.new(name: unary.service, rpcs: methods).rpcs).to eq(methods)
  end

  it "rejects all streaming declarations at the shared unary server seam before binding or input" do
    expect(binding).not_to receive(:encode)
    expect(binding).not_to receive(:decode)
    incoming = StringIO.new("must not read")
    expect(incoming).not_to receive(:read)
    [ConnectRPC::ServerStreamMethod, ConnectRPC::ClientStreamMethod, ConnectRPC::BidiStreamMethod].each do |type|
      rpc = declaration(type)
      target = ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: rpc.service, rpcs: [rpc])],
                                      binding: binding, handlers: { rpc.path => ->(*) { raise "handler invoked" } })
      { "application/connect+proto" => 415, "application/proto" => 501 }.each do |media, status|
        response = target.call(path: rpc.path, http_method: "POST", headers: { "content-type" => [media] },
                               input: incoming)
        expect(response.status).to eq(status)
        expect(JSON.parse(response.body)["code"]).to eq("unimplemented") if status == 501
      end
      expect(target.call(path: rpc.path, http_method: "GET", headers: {}, input: incoming).status).to eq(405)
    end
  end

  it "reads fragmented prefixes and payloads, adjacent envelopes, and a default empty message" do
    reader = framing(frame("") + frame("next") + frame("{}", 2))
    expect(reader.read_frame).to eq([0, "", 0])
    expect(reader.read_frame).to eq([0, "next", 4])
    expect(reader.read_frame).to eq([2, "{}", 2])
    expect(reader.read_frame).to be_nil
  end

  [0, 1, 2, 3, 4, 5, 128, 255].each do |flags|
    [true, false].each do |request|
      it "validates flags #{flags} for #{request ? 'request' : 'response'} envelopes" do
        bytes = flags.odd? ? ConnectRPC::Compression.encode("{}", limit: 1024) : "{}"
        reader = framing(frame(bytes, flags), request: request, encoding: :gzip)
        if flags < 4 && (!request || flags < 2)
          expect(reader.read_frame).to eq([flags, "{}", bytes.bytesize])
        else
          expect_code(request ? :invalid_argument : :internal) { reader.read_frame }
        end
      end
    end
  end

  it "rejects uint32 over-limit lengths before attempting to read their payload" do
    source = Object.new
    calls = []
    source.define_singleton_method(:read) do |length, **|
      calls << length
      raise "payload read" if calls.size > 1

      [0, 0xffff_ffff].pack("CN")
    end
    reader = engine::Framing.new(input: source, request: true, wire_limit: 10, limit: 10)
    expect_code(:resource_exhausted) { reader.read_frame }
    expect(calls).to eq([5])
  end

  ["\0", "\0\0\0\0", "#{[0, 4].pack('CN')}abc"].each do |bytes|
    it "rejects truncated #{bytes.bytesize}-byte envelopes in each direction" do
      expect_code(:invalid_argument) { framing(bytes, request: true).read_frame }
      expect_code(:internal) { framing(bytes).read_frame }
    end
  end

  it "rejects a compressed envelope without negotiation and an empty gzip member" do
    expect_code(:internal) { framing(frame("", 1), encoding: :gzip).read_frame }
    expect_code(:internal) { framing(frame("gzip", 1)).read_frame }
  end

  ["identity", nil].each do |coding|
    it "maps unnegotiated compressed request DATA to internal for #{coding.inspect} coding" do
      headers = coding ? { "connect-content-encoding" => [coding] } : {}
      negotiated = ConnectRPC::Protocol.content_encoding(engine.coding_headers(headers))
      expect(negotiated).to eq(:identity)
      expect do
        framing(frame("untrusted", 1), request: true, encoding: negotiated).read_frame
      end.to raise_error(ConnectRPC::Error) { |error|
        expect(error.code).to eq("internal")
        expect(error.message).to eq("Unnegotiated compression")
        expect(error.cause).to be_nil
      }
    end
  end

  ["", "malformed"].each do |payload|
    it "keeps negotiated gzip request #{payload.inspect} invalid_argument" do
      expect_code(:invalid_argument) do
        framing(frame(payload, 1), request: true, encoding: :gzip).read_frame
      end
    end
  end

  [2, 3, 5, 7, 129].each do |flags|
    it "validates request flags #{flags} before the unnegotiated compression exception" do
      expect_code(:invalid_argument) { framing(frame("", flags), request: true).read_frame }
    end
  end

  it "enforces terminal wire and decoded limits independently" do
    expect_code(:resource_exhausted) { framing(frame("{}", 2), terminal_wire_limit: 1).read_frame }
    gzip = ConnectRPC::Compression.encode("{}", limit: 1024)
    expect_code(:resource_exhausted) do
      framing(frame(gzip, 3), encoding: :gzip, terminal_limit: 1).read_frame
    end
  end

  ["{}", '{"error":null}'].each do |bytes|
    it "accepts bounded tolerant success input #{bytes}" do
      expect(engine.parse_terminal(bytes).first).to be_nil
    end
  end

  ["{}", '{"code":null}', '{"code":"future"}'].each do |error|
    it "normalizes absent/null/unknown string codes in #{error}" do
      result, = engine.parse_terminal("{\"error\":#{error}}")
      expect(result.code).to eq("unknown")
    end
  end

  ["[]", "null", '{"error":3}', '{"error":{"code":5}}', '{"error":{"details":{}}}',
   '{"error":{"details":[{"type":"valid.Name","value":"!"}]}}', '{"metadata":[]}'].each do |bytes|
    it "rejects malformed terminal #{bytes}" do
      expect_code(:internal) { engine.parse_terminal(bytes) }
    end
  end

  it "reads a null terminal message as empty and null details as none" do
    error, = engine.parse_terminal('{"error":{"code":"not_found","message":null,"details":null}}')
    expect([error.code, error.message, error.details]).to eq(["not_found", "", []])
  end

  ['{"type":"test.Detail"}', '{"type":"test.Detail","value":null}',
   '{"type":"type.googleapis.com/test.Detail","value":""}'].each do |detail|
    it "normalizes the terminal error detail #{detail} like connect-go" do
      error, = engine.parse_terminal("{\"error\":{\"code\":\"not_found\",\"details\":[#{detail}]}}")
      expect(error.details.map { |item| [item.type, item.value] }).to eq([["test.Detail", ""]])
    end
  end

  # Deliberately stricter than connect-go: ErrorDetail requires a valid type name.
  ['{"value":""}', '{"type":null}', '{"type":""}', '{"type":"type.googleapis.com/"}', '{"type":"not a name"}',
   '{"type":"test.Detail","value":5}', "null"].each do |detail|
    it "rejects the terminal with error detail #{detail}" do
      expect_code(:internal) { engine.parse_terminal("{\"error\":{\"code\":\"not_found\",\"details\":[#{detail}]}}") }
    end
  end

  it "omits invalid individual metadata while deeply freezing valid snapshots" do
    _, metadata = engine.parse_terminal('{"metadata":{"x-good":["a"],"x-bad":[4]}}')
    expect(metadata.to_h).to eq("x-good" => ["a"])
    expect { metadata["x-good"] = ["changed"] }.to raise_error(FrozenError)
    expect { metadata.to_h["x-good"].first.replace("changed") }.to raise_error(FrozenError)
  end

  it "replaces an oversized terminal within both limits and fails when the minimal error cannot fit" do
    bytes, error = engine.terminal_bytes(ConnectRPC::Error.new(:internal, "x" * 1000), ConnectRPC::Metadata.new,
                                         wire_limit: 64, limit: 64)
    expect(bytes.bytesize).to be <= 64
    expect(engine.parse_terminal(bytes).first.code).to eq("resource_exhausted")
    expect(error.code).to eq("resource_exhausted")
    expect_code(:resource_exhausted) do
      engine.terminal_bytes(nil, ConnectRPC::Metadata.new("x" => ["x" * 100]), wire_limit: 1, limit: 1)
    end
  end

  it "consumes an SS scope incrementally, preserves empty messages and forbids escaped I/O" do
    custom, = transport(frame("") + frame("two") + frame('{"metadata":{"x-tail":["b"]}}', 2))
    escaped = nil
    values = []
    expect(client(custom).server_stream(declaration, "request", timeout: nil) do |stream|
      escaped = stream
      expect(stream.headers["x-head"]).to eq(["a"])
      expect(stream.trailers).to be_nil
      expect(stream.each { |message| values << message }).to be_nil
      expect(stream.receive).to be_nil
      :ignored
    end).to be_nil
    expect(values).to eq(["", "two"])
    expect(escaped.headers["x-head"]).to eq(["a"])
    expect(escaped.trailers["x-tail"]).to eq(["b"])
    expect { escaped.receive }.to raise_error(ConnectRPC::StreamStateError)
  end

  it "automatically half-closes CS and returns exactly one typed response after terminal verification" do
    custom, = transport(frame("reply") + frame("{}", 2))
    result = client(custom).client_stream(declaration(ConnectRPC::ClientStreamMethod)) do |writer|
      writer.send_message("one")
      writer.send_message("two")
      :ignored
    end
    expect(result.message).to eq("reply")
    expect(result.trailers).to be_a(ConnectRPC::Metadata)
  end

  %i[transport_finish transport_scope_cleanup].each do |phase|
    it "rejects late success when a shared token changes during #{phase}" do
      source = ConnectRPC::CancellationSource.new
      custom, exchange, = transport(frame("reply") + frame("{}", 2))
      if phase == :transport_finish
        exchange.define_singleton_method(:finish) { |**| source.cancel }
      else
        original = custom.method(:open_stream)
        custom.define_singleton_method(:open_stream) do |request, &work|
          original.call(request, &work)
          source.cancel
        end
      end
      expect_code(:canceled) do
        client(custom).server_stream(declaration, "q", timeout: nil, cancellation: source.token) do |reader|
          reader.each { |_| nil }
        end
      end
      expect(source.token.reason).to eq(:canceled)
      expect(custom).not_to receive(:open_stream)
      expect_code(:canceled) do
        client(custom).server_stream(declaration, "q", cancellation: source.token) { |reader| reader.each { |_| nil } }
      end
    end
  end

  [0, 2].each do |count|
    it "rejects #{count} CS response messages with unimplemented cardinality" do
      custom, = transport((frame("reply") * count) + frame("{}", 2))
      expect_code(:unimplemented) { client(custom).client_stream(declaration(ConnectRPC::ClientStreamMethod)) { nil } }
    end
  end

  ["", "reply"].each do |bytes|
    it "rejects missing/nonfinal/duplicate terminals and extra bytes after #{bytes.inspect}" do
      [frame(bytes), "#{frame(bytes)}#{frame('{}', 2)}x", frame(bytes) + frame("{}", 2) + frame("{}", 2)].each do |body|
        custom, = transport(body)
        expect_code(:internal) { client(custom).server_stream(declaration, "q") { |reader| reader.each { |_| nil } } }
      end
    end
  end

  it "retains remote error metadata but accepts it only after verified EOF" do
    terminal = '{"error":{"code":"not_found","message":"missing"},"metadata":{"x-tail":["b"]}}'
    custom, = transport(frame(terminal, 2))
    expect_code(:not_found) { client(custom).server_stream(declaration, "q", &:receive) }
    custom, = transport("#{frame(terminal, 2)}x")
    expect_code(:internal) { client(custom).server_stream(declaration, "q", &:receive) }
  end

  it "rejects ordinary unconsumed exit but permits explicit SS/bidi abandonment" do
    [ConnectRPC::ServerStreamMethod, ConnectRPC::BidiStreamMethod].each do |type|
      custom, = transport(frame("a") + frame("{}", 2))
      rpc = declaration(type)
      args = type == ConnectRPC::ServerStreamMethod ? [rpc, "q"] : [rpc]
      operation = rpc.cardinality
      expect { client(custom).public_send(operation, *args) { nil } }.to raise_error(ConnectRPC::StreamNotConsumedError)
      expect(client(custom).public_send(operation, *args, &:abandon)).to be_nil
    end
  end

  it "preserves nonlocal break and user exception identity despite simultaneous source cancellation" do
    custom, = transport(frame("a") + frame("{}", 2))
    expect(client(custom).server_stream(declaration, "q") { break :value }).to eq(:value)
    source = ConnectRPC::CancellationSource.new
    failure = RuntimeError.new("application")
    backtrace = ["app.rb:7"]
    failure.set_backtrace(backtrace)
    expect do
      client(custom).server_stream(declaration, "q", cancellation: source.token) do
        source.cancel
        raise failure
      end
    end.to(raise_error do |error|
             expect(error).to equal(failure)
             expect(error.backtrace).to eq(backtrace)
           end)
    expect(source.token.cancelled?).to be(true)
  end

  it "rejects nil streaming messages and post-close sends before invoking the binding" do
    custom, = transport(frame("reply") + frame("{}", 2))
    client(custom).client_stream(declaration(ConnectRPC::ClientStreamMethod)) do |writer|
      expect_code(:invalid_argument) { writer.send_message(nil) }
      writer.close_send
      writer.close_send
      expect { writer.send_message("late") }.to raise_error(ConnectRPC::StreamStateError)
    end
  end

  {
    "server stream" => [ConnectRPC::ServerStreamMethod, ->(reader) { reader.each { nil } }],
    "client stream" => [ConnectRPC::ClientStreamMethod, ->(writer) { writer.send_message("q") }],
    "client stream with explicit close_send" => [ConnectRPC::ClientStreamMethod, lambda { |writer|
      writer.send_message("q")
      writer.close_send
    }],
    "bidi with explicit close_send" => [ConnectRPC::BidiStreamMethod, lambda { |duplex|
      duplex.send_message("q")
      duplex.close_send
      duplex.each { nil }
    }]
  }.each do |label, (type, work)|
    it "calls the exchange close_send once for a #{label}" do
      custom, exchange, = transport(frame("reply") + frame("{}", 2))
      calls = 0
      exchange.define_singleton_method(:close_send) { calls += 1 }
      rpc = declaration(type)
      arguments = type == ConnectRPC::ServerStreamMethod ? [rpc, "q"] : [rpc]
      client(custom).public_send(rpc.cardinality, *arguments, &work)
      expect(calls).to eq(1)
    end
  end

  it "does not retry a failed explicit close_send at completion" do
    custom, exchange, = transport(frame("reply") + frame("{}", 2))
    calls = 0
    exchange.define_singleton_method(:close_send) do
      calls += 1
      raise IOError, "close failed" if calls == 1
    end
    received = []
    client(custom).bidi_stream(declaration(ConnectRPC::BidiStreamMethod)) do |duplex|
      expect { duplex.close_send }.to raise_error(IOError, "close failed")
      duplex.each { |message| received << message }
    end
    expect(received).to eq(["reply"])
    expect(calls).to eq(1)
  end

  it "rejects wrong declaration, missing block, unsupported capability and unary misuse before binding/I/O" do
    custom = Object.new
    codec = Object.new
    core = ConnectRPC::Client.new(base_url: "https://example.invalid", binding: codec, transport: custom)
    expect { core.server_stream(declaration, "q") }.to raise_error(ArgumentError)
    expect { core.server_stream(declaration, "q") { nil } }.to raise_error(ArgumentError)
    expect { core.client_stream(declaration) { nil } }.to raise_error(ArgumentError)
    expect { core.bidi_stream(declaration) { nil } }.to raise_error(ArgumentError)
    expect { core.call(declaration, "q") }.to raise_error(ArgumentError)
  end

  [RuntimeError, Interrupt].each do |type|
    it "preserves an independent #{type} identity/backtrace over transport cleanup failure" do
      custom, exchange, = transport(frame("{}", 2))
      custom.define_singleton_method(:open_stream) do |_, &work|
        work.call(exchange)
      ensure
        raise IOError, "cleanup noise"
      end
      original = type.new("application")
      original.set_backtrace(["application.rb:42"])
      expect do
        client(custom).bidi_stream(declaration(ConnectRPC::BidiStreamMethod)) { raise original }
      end.to(raise_error do |actual|
        expect(actual).to equal(original)
        expect(actual.backtrace).to eq(["application.rb:42"])
        expect(actual.cause).to be_nil
      end)
    end
  end

  it "rejects overlapping writes before another codec call and permits one read alongside a writer" do
    custom, = transport(frame("reply") + frame("{}", 2))
    encoded = []
    binding.define_singleton_method(:encode) do |message, **|
      encoded << message
      Fiber.yield(:encoding) if message == "first"
      message
    end
    core = client(custom)
    core.bidi_stream(declaration(ConnectRPC::BidiStreamMethod)) do |duplex|
      writing = Fiber.new { duplex.send_message("first") }
      expect(writing.resume).to eq(:encoding)
      expect { duplex.send_message("overlap") }.to raise_error(ConnectRPC::StreamStateError)
      expect(duplex.receive).to eq("reply")
      expect(writing.resume).to be_nil
      expect(encoded).to eq(["first"])
      duplex.close_send
      expect(duplex.receive).to be_nil
    end
  end

  it "rejects header publication from a foreign thread and after the owning processing scope closes" do
    publications = []
    context = ConnectRPC::StreamingContext.new(publish: ->(metadata) { publications << metadata })
    foreign = Thread.new do
      context.send_headers
    rescue ConnectRPC::StreamStateError => e
      e
    end.value
    expect(foreign).to be_a(ConnectRPC::StreamStateError)
    expect(publications).to be_empty
    context.close
    expect { context.send_headers }.to raise_error(ConnectRPC::StreamStateError)
    expect(publications).to be_empty
  end
end

RSpec.describe "Streaming send-failure arbitration" do
  def bytes(payload, flag = 0)
    [flag, payload.bytesize].pack("CN") + payload
  end

  def stream_client(body, kind: ConnectRPC::BidiStreamMethod, **)
    reader = StringIO.new(body)
    exchange = Object.new
    exchange.define_singleton_method(:response_head) do |**|
      ConnectRPC::StreamHead.new(status: 200, headers: { "content-type" => ["application/connect+proto"] })
    end
    exchange.define_singleton_method(:read) { |length, **| reader.read(length) }
    exchange.define_singleton_method(:write) { |_| raise ConnectRPC::Error.new(:unavailable, "incidental") }
    exchange.define_singleton_method(:close_send) { nil }
    exchange.define_singleton_method(:finish) { |**| nil }
    exchange.define_singleton_method(:abort) { nil }
    transport = Object.new
    transport.define_singleton_method(:streaming_capabilities) { %i[server_stream client_stream bidi_stream] }
    transport.define_singleton_method(:open_stream) { |_, &work| work.call(exchange) }
    binding = Object.new
    binding.define_singleton_method(:encode) { |message, **| message }
    binding.define_singleton_method(:decode) { |message, **| message }
    rpc = kind.new(service: "recovery.Test", name: "Bidi", request_type: String, response_type: String)
    [ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding, transport: transport, **),
     rpc, binding, exchange, transport]
  end

  # Core sends a server stream's one request itself; grpc-go and connect-go
  # then let the response decide the call's outcome.
  describe "while core sends a server stream's request" do
    let(:outcomes) { [] }
    let(:bus) do
      outcomes = self.outcomes
      subscriber = Object.new
      subscriber.define_singleton_method(:start) do |_call|
        Object.new.tap do |observer|
          observer.define_singleton_method(:processing_finished) { |outcome| outcomes << outcome }
        end
      end
      ConnectRPC::Instrumentation.new(subscribers: [subscriber])
    end

    def server_stream_client(body, write: ->(_) { raise IOError, "incidental send I/O" }, **)
      core, rpc, binding, exchange, transport = stream_client(body, kind: ConnectRPC::ServerStreamMethod, **)
      exchange.define_singleton_method(:write, &write)
      [core, rpc, binding, exchange, transport]
    end

    [IOError.new("incidental send I/O"), ConnectRPC::Error.new(:unavailable, "incidental")].each do |failure|
      it "returns an OK response's messages after a #{failure.class} send failure" do
        core, rpc, = server_stream_client(bytes("one") + bytes("two") + bytes("{}", 2),
                                          write: ->(_) { raise failure }, instrumentation: bus)
        received = []
        expect(core.server_stream(rpc, "q") { |reader| reader.each { |message| received << message } }).to be_nil
        expect(received).to eq(%w[one two])
        expect(outcomes.map { |outcome| [outcome.code, outcome.error, outcome.normalized_error] })
          .to eq([["OK", nil, nil]])
      end
    end

    # connectrpc-hyper and connectrpc-async record their raw write failure
    # before raising unavailable with it as the cause.
    it "keeps a transport's recorded write failure out of an OK outcome" do
      core, rpc, _, exchange, transport = server_stream_client(bytes("one") + bytes("{}", 2), instrumentation: bus)
      transport.define_singleton_method(:open_stream) do |request, &work|
        exchange.define_singleton_method(:write) do |_|
          native = RuntimeError.new("native write failure")
          request.lifecycle.record_failure(native)
          raise ConnectRPC::Error.new(:unavailable, "HTTP transport failed: stream closed"), cause: native
        end
        work.call(exchange)
      end
      expect(core.server_stream(rpc, "q") { |reader| reader.each { nil } }).to be_nil
      expect(outcomes.map { |outcome| [outcome.code, outcome.error] }).to eq([["OK", nil]])
    end

    # connectrpc-async's HTTP/1.1 writer records its failure before core's
    # send starts; the first write then surfaces it as unavailable.
    it "drops a transport failure recorded before the send that the forgiven write surfaced" do
      core, rpc, _, exchange, transport = server_stream_client(bytes("one") + bytes("{}", 2), instrumentation: bus)
      transport.define_singleton_method(:open_stream) do |request, &work|
        raw = IOError.new("request writer failed")
        request.lifecycle.record_failure(raw)
        exchange.define_singleton_method(:write) do |_|
          raise ConnectRPC::Error.new(:unavailable, "HTTP/1 transport failed"), cause: raw
        end
        work.call(exchange)
      end
      expect(core.server_stream(rpc, "q") { |reader| reader.each { nil } }).to be_nil
      expect(outcomes.map { |outcome| [outcome.code, outcome.error] }).to eq([["OK", nil]])
    end

    it "keeps an independent failure recorded while the request was sent" do
      independent = IOError.new("sibling read failed")
      core, rpc, _, exchange, transport = server_stream_client(bytes("one") + bytes("{}", 2), instrumentation: bus)
      transport.define_singleton_method(:open_stream) do |request, &work|
        exchange.define_singleton_method(:write) do |_|
          request.lifecycle.record_failure(independent)
          raise IOError, "incidental send I/O"
        end
        work.call(exchange)
      end
      expect(core.server_stream(rpc, "q") { |reader| reader.each { nil } }).to be_nil
      expect(outcomes.map { |outcome| [outcome.code, outcome.error] }).to eq([["OK", independent]])
    end

    # The call holds its first eight failures; later ones cannot displace an
    # earlier independent failure that forgiveness must keep.
    it "keeps an independent failure recorded before the forgiven send's ninth failure" do
      independent = IOError.new("sibling read failed")
      core, rpc, _, exchange, transport = server_stream_client(bytes("one") + bytes("{}", 2), instrumentation: bus)
      transport.define_singleton_method(:open_stream) do |request, &work|
        exchange.define_singleton_method(:write) do |_|
          causes = []
          7.times do |index|
            raise IOError, "write cause #{index}", cause: causes.last
          rescue IOError => e
            request.lifecycle.record_failure(e)
            causes << e
          end
          request.lifecycle.record_failure(independent)
          raise ConnectRPC::Error.new(:unavailable, "HTTP transport failed: stream closed"), cause: causes.last
        end
        work.call(exchange)
      end
      expect(core.server_stream(rpc, "q") { |reader| reader.each { nil } }).to be_nil
      expect(outcomes.map { |outcome| [outcome.code, outcome.error] }).to eq([["OK", independent]])
    end

    it "closes the send side after a forgiven send before reading the response" do
      core, rpc, _, exchange = server_stream_client(bytes("one") + bytes("{}", 2))
      closed = false
      original = exchange.method(:read)
      exchange.define_singleton_method(:close_send) { closed = true }
      exchange.define_singleton_method(:read) do |length, **|
        raise ConnectRPC::Error.new(:deadline_exceeded, "request never closed") unless closed

        original.call(length)
      end
      received = []
      expect(core.server_stream(rpc, "q") { |reader| reader.each { |message| received << message } }).to be_nil
      expect(received).to eq(["one"])
    end

    {
      "the write's failure" => ->(failure) { raise failure },
      "an IOError caused by the write's failure" => ->(failure) { raise IOError, "close failed", cause: failure },
      "an unavailable error caused by the write's failure" => lambda { |failure|
        raise ConnectRPC::Error.new(:unavailable, "close failed"), cause: failure
      }
    }.each do |label, close_failure|
      it "forgives a close_send that raises #{label} after a forgiven send" do
        failure = IOError.new("incidental send I/O")
        core, rpc, _, exchange = server_stream_client(bytes("one") + bytes("{}", 2),
                                                      write: ->(_) { raise failure }, instrumentation: bus)
        exchange.define_singleton_method(:close_send) { close_failure.call(failure) }
        received = []
        expect(core.server_stream(rpc, "q") { |reader| reader.each { |message| received << message } }).to be_nil
        expect(received).to eq(["one"])
        expect(outcomes.map { |outcome| [outcome.code, outcome.error] }).to eq([["OK", nil]])
      end
    end

    it "still ends the call when close_send fails otherwise after a forgiven send" do
      core, rpc, _, exchange = server_stream_client(bytes("one") + bytes("{}", 2))
      exchange.define_singleton_method(:close_send) { raise ConnectRPC::Error.new(:deadline_exceeded, "closing") }
      expect { core.server_stream(rpc, "q") { |reader| reader.each { nil } } }
        .to raise_error(ConnectRPC::Error) { |error| expect([error.code, error.cause]).to eq(["deadline_exceeded", nil]) }
    end

    # Closing may have failed before it committed the half-close, so a fresh
    # failure there ends the call instead of leaving the read to the deadline.
    it "ends the call when close_send raises an unavailable error unrelated to the forgiven send" do
      closing = ConnectRPC::Error.new(:unavailable, "close failed")
      core, rpc, _, exchange = server_stream_client(bytes("one") + bytes("{}", 2), instrumentation: bus)
      exchange.define_singleton_method(:close_send) { raise closing }
      expect { core.server_stream(rpc, "q") { |reader| reader.each { nil } } }
        .to raise_error(ConnectRPC::Error) { |error| expect(error).to be(closing) }
      expect(outcomes.map { |outcome| [outcome.code, outcome.error] }).to eq([["unavailable", closing]])
    end

    it "raises the response's remote error" do
      core, rpc, = server_stream_client(bytes("one") + bytes('{"error":{"code":"not_found"}}', 2),
                                        instrumentation: bus)
      received = []
      raised = nil
      expect { core.server_stream(rpc, "q") { |reader| reader.each { |message| received << message } } }
        .to raise_error(ConnectRPC::Error) { |error|
              raised = error
              expect(error.code).to eq("not_found")
            }
      expect(received).to eq(["one"])
      expect(outcomes.map { |outcome| [outcome.code, outcome.error] }).to eq([["not_found", raised]])
    end

    it "raises the read's own failure when the response never completes" do
      core, rpc, _, exchange = server_stream_client(bytes("one"))
      exchange.define_singleton_method(:read) { |_, **| raise ConnectRPC::Error.new(:unavailable, "read lost") }
      expect { core.server_stream(rpc, "q") { |reader| reader.each { nil } } }
        .to raise_error(ConnectRPC::Error) { |error|
              expect([error.code, error.message]).to eq(["unavailable", "read lost"])
            }
    end

    it "still fails the call on a request error that is not a send failure" do
      core, rpc, = server_stream_client(bytes("{}", 2), write: ->(_) {}, request_limit: 3)
      expect { core.server_stream(rpc, "long") { |reader| reader.each { nil } } }
        .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("resource_exhausted") }
      core, rpc, = server_stream_client(bytes("{}", 2))
      expect { core.server_stream(rpc, nil) { |reader| reader.each { nil } } }
        .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("invalid_argument") }
    end

    it "lets cancellation during the send win" do
      source = ConnectRPC::CancellationSource.new
      core, rpc, = server_stream_client(bytes("one") + bytes("{}", 2), write: lambda { |_|
        source.cancel
        raise IOError, "incidental send I/O"
      })
      expect { core.server_stream(rpc, "q", cancellation: source.token) { |reader| reader.each { nil } } }
        .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("canceled") }
    end
  end

  it "recovers a remote error only after skipping data without binding decode and verifying the tail" do
    core, rpc, binding = stream_client(bytes("discard") + bytes('{"error":{"code":"not_found"}}', 2))
    expect(binding).not_to receive(:decode)
    expect { core.bidi_stream(rpc) { |duplex| duplex.send_message("q") } }
      .to raise_error(ConnectRPC::Error) { |error|
            expect(error.code).to eq("not_found")
            expect(error.cause).to be_nil
          }
  end

  %w[success extra_tail malformed].each do |kind|
    it "preserves an unhandled send failure for #{kind}" do
      body = case kind
             when "success" then bytes("{}", 2)
             when "extra_tail" then "#{bytes('{"error":{"code":"not_found"}}', 2)}x"
             else bytes("invalid", 2)
             end
      core, rpc, = stream_client(body)
      expect { core.bidi_stream(rpc) { |duplex| duplex.send_message("q") } }
        .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("unavailable") }
    end
  end

  # The application drives a client stream's sends, so an unhandled send
  # failure still wins over an OK response.
  it "preserves an unhandled client-streaming send failure over an OK response" do
    core, rpc, = stream_client(bytes("reply") + bytes("{}", 2), kind: ConnectRPC::ClientStreamMethod)
    expect { core.client_stream(rpc) { |writer| writer.send_message("q") } }
      .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("unavailable") }
  end

  it "allows an explicitly handled send failure to complete the normal response path" do
    core, rpc, = stream_client(bytes("reply") + bytes("{}", 2))
    expect(core.bidi_stream(rpc) do |duplex|
      expect { duplex.send_message("q") }.to raise_error(ConnectRPC::Error)
      expect(duplex.receive).to eq("reply")
      expect(duplex.receive).to be_nil
    end).to be_nil
  end

  it "does not erase an independent user Connect error with concurrent cancellation" do
    core, rpc, = stream_client(bytes("{}", 2))
    source = ConnectRPC::CancellationSource.new
    error = ConnectRPC::Error.new(:unavailable, "application-owned")
    expect do
      core.bidi_stream(rpc, cancellation: source.token) do
        source.cancel
        raise error
      end
    end.to(raise_error { |actual| expect(actual).to equal(error) })
  end

  it "caps recovery wire bytes including envelope prefixes without decoding skipped payloads" do
    core, rpc, binding, exchange = stream_client(bytes("x" * 270_000) + bytes('{"error":{"code":"not_found"}}', 2))
    original = exchange.method(:read)
    reads = []
    exchange.define_singleton_method(:read) do |length, **options|
      original.call(length, **options).tap { |chunk| reads << chunk.bytesize if chunk }
    end
    expect(binding).not_to receive(:decode)
    expect { core.bidi_stream(rpc) { |duplex| duplex.send_message("q") } }
      .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("unavailable") }
    expect(reads.first).to eq(5)
    expect(reads.max).to be <= 16_384
    expect(reads.sum).to be <= 262_144
    expect(reads.sum).to be > 245_000
  end

  it "does not renew the recovery allowance as skipped payload fragments make progress" do
    core, rpc, _, exchange = stream_client(bytes("x" * 100_000) + bytes('{"error":{"code":"not_found"}}', 2))
    clock = 0.0
    allow_any_instance_of(ConnectRPC.const_get(:Streaming)::Session).to receive(:now) { clock }
    original = exchange.method(:read)
    allowances = []
    exchange.define_singleton_method(:read) do |length, timeout: nil|
      allowances << timeout
      clock += 0.03
      original.call(length)
    end
    expect { core.bidi_stream(rpc, timeout: nil) { |duplex| duplex.send_message("q") } }
      .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("unavailable") }
    expect(allowances.size).to eq(4)
    expect(allowances.each_cons(2).all? { |left, right| right < left }).to be(true)
    expect(allowances.last).to be < 0.011
  end

  it "includes recovered terminal transport cleanup in its one absolute 100ms budget" do
    core, rpc, _, exchange = stream_client(bytes('{"error":{"code":"not_found"}}', 2))
    clock = 0.0
    allow_any_instance_of(ConnectRPC.const_get(:Streaming)::Session).to receive(:now) { clock }
    exchange.define_singleton_method(:finish) { |**| clock = 0.101 }
    expect { core.bidi_stream(rpc, timeout: nil) { |duplex| duplex.send_message("q") } }
      .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("unavailable") }
  end

  it "includes normal transport cleanup in the fixed one second tail policy with a nil RPC deadline" do
    core, rpc, _, exchange = stream_client(bytes("{}", 2))
    clock = 0.0
    allow_any_instance_of(ConnectRPC.const_get(:Streaming)::Session).to receive(:now) { clock }
    exchange.define_singleton_method(:finish) { |**| clock = 1.001 }
    expect { core.bidi_stream(rpc, timeout: nil, &:receive) }
      .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("unavailable") }
  end

  it "caps the response head wait by the same recovery budget before reading any terminal bytes" do
    core, rpc, _, exchange = stream_client(bytes('{"error":{"code":"not_found"}}', 2))
    waits = []
    head = exchange.response_head
    exchange.define_singleton_method(:response_head) do |timeout: nil|
      waits << timeout
      head
    end
    expect { core.bidi_stream(rpc, timeout: nil) { |duplex| duplex.send_message("q") } }
      .to raise_error(ConnectRPC::Error)
    expect(waits.size).to eq(1)
    expect(waits.first).to be > 0
    expect(waits.first).to be <= 0.1
  end

  it "does not steal a live normal reader for recovery or resume its escaped I/O after abandonment" do
    core, rpc, binding, exchange = stream_client(bytes("reply") + bytes('{"error":{"code":"not_found"}}', 2))
    original = exchange.method(:read)
    reads = 0
    exchange.define_singleton_method(:read) do |length, **options|
      reads += 1
      Fiber.yield(:reading) if reads == 1
      original.call(length, **options)
    end
    reading = nil
    expect(binding).not_to receive(:decode)
    expect do
      core.bidi_stream(rpc) do |duplex|
        reading = Fiber.new { duplex.receive }
        expect(reading.resume).to eq(:reading)
        duplex.send_message("q")
      end
    end.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("unavailable") }
    expect(reads).to eq(1)
    expect { reading.resume }.to raise_error(ConnectRPC::StreamStateError)
    expect(reads).to eq(1)
  end
end
