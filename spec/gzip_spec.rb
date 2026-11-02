# frozen_string_literal: true

require "stringio"
require "zlib"
require_relative "../examples/echo_schema"
require_relative "../examples/localhost"
require_relative "support/socket_peer"

require "connectrpc/transports/net_http"

RSpec.describe "bounded unary gzip" do
  include SocketPeer

  let(:rpc) { ConnectRPC::Method.new(service: "test.Gzip", name: "Echo", request_type: String, response_type: String) }
  let(:service) { ConnectRPC::Service.new(name: rpc.service, methods: [rpc]) }
  let(:binding) do
    Class.new do
      def encode(message, **) = message
      def decode(bytes, **) = bytes
    end.new
  end
  let(:invocations) { [] }
  let(:handler) do
    lambda do |message, context|
      invocations << message
      context.response_metadata["x-head"] = ["one"]
      context.response_trailers["x-tail"] = ["two"]
      message
    end
  end

  def server(**)
    ConnectRPC::Server.new(services: [service], binding: binding, handlers: { rpc.path => handler }, **)
  end

  def dispatch(body, headers = {}, **)
    server(**).call(path: rpc.path, http_method: "POST", input: StringIO.new(body),
                    headers: { "content-type" => ["application/proto"] }.merge(headers))
  end

  def client(transport:, **)
    ConnectRPC::Client.new(base_url: "http://example.invalid", binding: binding, transport: transport, **)
  end

  def error_code(response) = JSON.parse(response.body).fetch("code")

  it "compresses both custom seams and normalizes identity to the last preference" do
    endpoint = server(encoding: :gzip)
    transport = lambda do |request|
      expect(request.headers["accept-encoding"]).to eq(["gzip, identity"])
      expect(request.headers["content-encoding"]).to eq(["gzip"])
      expect(Zlib.gunzip(request.body)).to eq("hello")
      result = endpoint.call(path: rpc.path, http_method: "POST", headers: request.headers,
                             input: StringIO.new(request.body))
      expect(result.headers["content-encoding"]).to eq(["gzip"])
      result
    end
    result = client(transport: transport, encoding: :gzip, accept_encoding: %i[identity gzip]).call(rpc, "hello")
    expect(result.message).to eq("hello")
    expect(result.metadata["x-head"]).to eq(["one"])
    expect(result.trailers["x-tail"]).to eq(["two"])
  end

  [nil, %i[gzip], %i[gzip identity], %i[identity gzip], [:identity]].each do |preference|
    it "advertises actual response preference #{preference.inspect}" do
      transport = lambda do |request|
        expect(request.headers["accept-encoding"]).to eq([preference == [:identity] ? "identity" : "gzip, identity"])
        ConnectRPC::TransportResponse.new(status: 200, headers: { "content-type" => ["application/proto"] }, body: "ok")
      end
      expect(client(transport: transport, encoding: :gzip, accept_encoding: preference).call(rpc,
                                                                                             "hi").message).to eq("ok")
    end
  end

  [[], %i[gzip gzip], %i[identity identity], ["gzip"], [:br], "gzip", false].each do |preference|
    it "rejects invalid response preference #{preference.inspect}" do
      expect { client(transport: ->(_) {}, accept_encoding: preference) }.to raise_error(ArgumentError)
    end
  end

  { nil => "gzip", [] => nil, [""] => nil, ["identity, gzip"] => "gzip", ["br", "GZIP, identity"] => "gzip",
    ["gzip;q=1, br"] => nil, ["identity"] => nil }.each do |accept, selected|
    it "negotiates #{accept.inspect} with mandatory identity fallback" do
      headers = { "content-encoding" => ["GZip"] }
      headers["accept-encoding"] = accept unless accept.nil?
      response = dispatch(Zlib.gzip("hello"), headers, encoding: :gzip)
      expect(response.status).to eq(200)
      expect(response.headers["content-encoding"]).to eq(selected ? [selected] : nil)
      expect(selected ? Zlib.gunzip(response.body) : response.body).to eq("hello")
      expect(response.headers["accept-encoding"]).to eq(["gzip, identity"])
    end
  end

  [[""], ["br"], ["gzip, identity"], %w[gzip gzip]].each do |coding|
    it "rejects invalid request coding #{coding.inspect} before invocation" do
      response = dispatch("", { "content-encoding" => coding }, encoding: :gzip)
      expect(error_code(response)).to eq("unimplemented")
      expect(response.headers["accept-encoding"]).to eq(["gzip, identity"])
      expect(invocations).to be_empty
    end
  end

  it "preserves the identity-only default" do
    expect(error_code(dispatch(Zlib.gzip("hello"), { "content-encoding" => ["gzip"] }))).to eq("unimplemented")
  end

  it "compresses actual empty request and success serialization as validated nonempty gzip members" do
    endpoint = server(encoding: :gzip)
    transport = lambda do |request|
      expect(request.headers["content-encoding"]).to eq(["gzip"])
      expect(request.body.bytesize).to be_positive
      expect(Zlib.gunzip(request.body)).to eq("")
      response = endpoint.call(path: rpc.path, http_method: "POST", headers: request.headers,
                               input: StringIO.new(request.body))
      expect(response.headers["content-encoding"]).to eq(["gzip"])
      expect(response.body.bytesize).to be_positive
      expect(Zlib.gunzip(response.body)).to eq("")
      response
    end
    expect(client(transport: transport, encoding: :gzip).call(rpc, "").message).to eq("")
    expect(invocations).to eq([""])
  end

  it "bypasses inflation only for zero wire content after validating its header" do
    expect(ConnectRPC::Compression).not_to receive(:decode)
    response = dispatch("", { "content-encoding" => ["gzip"], "accept-encoding" => ["identity"] }, encoding: :gzip)
    expect(response.status).to eq(200)
    expect(response.body).to eq("")
    reply = ConnectRPC::TransportResponse.new(status: 200, headers: { "content-type" => ["application/proto"],
                                                                      "content-encoding" => ["gzip"] }, body: "")
    expect(client(transport: ->(_) { reply }, accept_encoding: [:gzip]).call(rpc, "").message).to eq("")
    reply.headers["content-encoding"] = [""]
    expect { client(transport: ->(_) { reply }, accept_encoding: [:gzip]).call(rpc, "") }
      .to raise_error(ConnectRPC::Error, "Unsupported response content encoding")
  end

  it "enforces gzip framing overhead on empty messages without relaxing message caps" do
    wire_size = Zlib.gzip("").bytesize
    transport = ->(_) { raise "Must not dispatch" }
    expect do
      client(transport: transport, encoding: :gzip, request_limit: 1, request_wire_limit: wire_size - 1).call(rpc, "")
    end
      .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("resource_exhausted") }
    headers = { "content-encoding" => ["gzip"] }
    failure = dispatch(Zlib.gzip(""), headers, encoding: :gzip, response_wire_limit: wire_size - 1)
    expect(failure.status).to eq(429)
    expect(failure.headers).not_to have_key("content-encoding")
    success = dispatch(Zlib.gzip(""), headers, encoding: :gzip, request_limit: 1, request_wire_limit: wire_size,
                                               response_limit: 1, response_wire_limit: wire_size)
    expect(success.status).to eq(200)
    expect(success.body.bytesize).to eq(wire_size)
    expect(Zlib.gunzip(success.body)).to eq("")
  end

  it "validates every concatenated member, including empty members, under one cap" do
    body = Zlib.gzip("alpha") + Zlib.gzip("") + Zlib.gzip("beta")
    headers = { "content-encoding" => ["gzip"], "accept-encoding" => ["identity"] }
    success = dispatch(body, headers, encoding: :gzip, request_limit: 9, request_wire_limit: body.bytesize)
    expect(success.body).to eq("alphabeta")
    failure = dispatch(body, headers, encoding: :gzip, request_limit: 8, request_wire_limit: body.bytesize)
    expect(error_code(failure)).to eq("resource_exhausted")
  end

  it "rejects corrupt headers, CRC, ISIZE, truncation, trailing data and bad next members before binding" do
    good = Zlib.gzip("alpha")
    bad_crc = good.dup
    bad_crc.setbyte(-8, bad_crc.getbyte(-8) ^ 1)
    bad_size = good.dup
    bad_size.setbyte(-1, bad_size.getbyte(-1) ^ 1)
    empty_crc = Zlib.gzip("")
    empty_crc.setbyte(-8, empty_crc.getbyte(-8) ^ 1)
    malformed = ["bad header", bad_crc, bad_size, good[0...-1], good[0...12], "#{good}x", good + Zlib.deflate("bad"),
                 good + Zlib.gzip("beta")[0...-1], good + bad_crc, good + empty_crc, good + Zlib.gzip("")[0...-1]]
    expect(binding).not_to receive(:decode)
    malformed.each do |body|
      response = dispatch(body, { "content-encoding" => ["gzip"] }, encoding: :gzip)
      expect(error_code(response)).to eq("invalid_argument")
      expect(response.body).not_to match(/zlib|CRC|buffer|stream/i)
    end
    expect(invocations).to be_empty
  end

  it "preserves HTTP fallback and metadata for bad compressed errors while keeping success corruption internal" do
    bad_crc = Zlib.gzip('{"code":"internal"}')
    bad_crc.setbyte(-8, bad_crc.getbyte(-8) ^ 1)
    [[Zlib.gzip("{"), false], [Zlib.gzip("error")[0...-2], true], [bad_crc, true]].each do |body, corrupt|
      (corrupt ? [200, 503] : [503]).each do |status|
        headers = { "content-type" => [status == 200 ? "application/proto" : "application/json"],
                    "content-encoding" => ["gzip"],
                    "x-head" => ["yes"], "trailer-x-tail" => ["end"] }
        response = ConnectRPC::TransportResponse.new(status: status, headers: headers, body: body)
        caller = client(transport: ->(_) { response }, accept_encoding: [:gzip])
        expect { caller.call(rpc, "hi") }.to raise_error(ConnectRPC::Error) do |error|
          expect(error.code).to eq(status == 200 ? "internal" : "unavailable")
          expect(error.message).to eq("Cannot decompress response") if status == 200
          if status == 503
            expect(error.message).to eq("HTTP 503")
            expect(error.metadata["x-head"]).to eq(["yes"])
            expect(error.trailers["x-tail"]).to eq(["end"])
          end
        end
      end
    end
  end

  %i[proto json].each do |codec|
    it "decodes gzip structured #{codec} peer errors with details and trailers" do
      body = JSON.generate(code: "out_of_range", message: "safe", details: [{ type: "test.Detail", value: "aGk" }])
      headers = { "content-type" => ["application/json"], "content-encoding" => ["GZIP"], "trailer-x-tail" => ["end"] }
      reply = ConnectRPC::TransportResponse.new(status: 422, headers: headers, body: Zlib.gzip(body))
      caller = client(transport: ->(_) { reply }, codec: codec, accept_encoding: [:gzip])
      expect { caller.call(rpc, "hi") }.to raise_error(ConnectRPC::Error) do |error|
        expect([error.code, error.message, error.details.first.value,
                error.trailers["x-tail"]]).to eq(["out_of_range", "safe", "hi", ["end"]])
      end
    end
  end

  it "retains wire caps by default and permits compression expansion only with explicit wire caps" do
    wire = Zlib.gzip("x")
    endpoint = server(encoding: :gzip, request_limit: 1, request_wire_limit: wire.bytesize, response_limit: 1,
                      response_wire_limit: wire.bytesize)
    transport = lambda do |request|
      expect(request.response_limit).to eq(wire.bytesize)
      endpoint.call(path: rpc.path, http_method: "POST", headers: request.headers, input: StringIO.new(request.body))
    end
    options = { encoding: :gzip, request_limit: 1, response_limit: 1 }
    expect { client(transport: transport, **options).call(rpc, "x") }.to raise_error(ConnectRPC::Error) do |error|
      expect(error.code).to eq("resource_exhausted")
    end
    expect(client(transport: transport, **options, request_wire_limit: wire.bytesize,
                  response_wire_limit: wire.bytesize).call(rpc, "x").message).to eq("x")
    expect(error_code(dispatch(wire, { "content-encoding" => ["gzip"] }, encoding: :gzip,
                                                                         request_limit: 1))).to eq("resource_exhausted")
  end

  it "checks outgoing serialized caps before compression even when wire caps are larger" do
    transport = double("unused transport")
    expect(transport).not_to receive(:call)
    expect { client(transport: transport, encoding: :gzip, request_limit: 1, request_wire_limit: 100).call(rpc, "xx") }
      .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("resource_exhausted") }
    allow(handler).to receive(:call).and_return("xx")
    response = dispatch("x", { "accept-encoding" => ["gzip"] }, encoding: :gzip,
                                                                response_limit: 1, response_wire_limit: 100)
    expect(response.status).to eq(429)
    expect(response.headers).not_to have_key("content-encoding")
  end

  it "checks identity against both bounds and rejects declared wire excess without reading" do
    expect(error_code(dispatch("xx", {}, request_limit: 1, request_wire_limit: 2))).to eq("resource_exhausted")
    expect(error_code(dispatch("xx", {}, request_limit: 2, request_wire_limit: 1))).to eq("resource_exhausted")
    input = double("unread input")
    expect(input).not_to receive(:read)
    headers = { "content-type" => ["application/proto"], "content-encoding" => ["gzip"], "content-length" => ["2"] }
    result = server(encoding: :gzip, request_wire_limit: 1).call(path: rpc.path, http_method: "POST", input: input,
                                                                 headers: headers)
    expect(error_code(result)).to eq("resource_exhausted")
  end

  it "retains local size, header and expiry classifications on compressed non-200 responses" do
    reply = ConnectRPC::TransportResponse.new(status: 503, headers: { "content-type" => ["application/json"],
                                                                      "content-encoding" => ["gzip"] },
                                              body: Zlib.gzip("x" * 10_000))
    expect do
      client(transport: ->(_) { reply }, accept_encoding: [:gzip], response_limit: 10,
             response_wire_limit: 1000).call(rpc, "hi")
    end
      .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("resource_exhausted") }
    expect { client(transport: ->(_) { reply }, accept_encoding: [:identity]).call(rpc, "hi") }
      .to raise_error(ConnectRPC::Error, "Unsupported response content encoding")
    reply.headers["content-encoding"] = [""]
    expect { client(transport: ->(_) { reply }, accept_encoding: [:gzip]).call(rpc, "hi") }
      .to raise_error(ConnectRPC::Error, "Unsupported response content encoding")
    reply.headers["content-encoding"] = ["gzip"]
    clock = 0.0
    allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { clock }
    allow(Zlib::Inflate).to receive(:new).and_wrap_original do |original, *arguments|
      clock = 2.0
      original.call(*arguments)
    end
    expect { client(transport: ->(_) { reply }, accept_encoding: [:gzip]).call(rpc, "hi", timeout: 1) }
      .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("deadline_exceeded") }
  end

  it "uses safe internal errors for unexpected native compression failures" do
    allow(Zlib::Deflate).to receive(:new).and_raise(Zlib::StreamError, "native secret")
    expect { client(transport: ->(_) { raise "Must not dispatch" }, encoding: :gzip).call(rpc, "hello") }
      .to raise_error(ConnectRPC::Error) do |error|
        expect([error.code, error.message]).to eq(["internal", "Cannot compress request"])
      end
    response = dispatch("hello", { "accept-encoding" => ["gzip"] }, encoding: :gzip)
    expect(error_code(response)).to eq("internal")
    expect(response.headers).not_to have_key("content-encoding")
    expect(response.body).not_to include("native secret")
    allow(Zlib::Inflate).to receive(:new).and_raise(Zlib::StreamError, "native secret")
    reply = ConnectRPC::TransportResponse.new(status: 503, headers: { "content-encoding" => ["gzip"] }, body: "bad")
    expect { client(transport: ->(_) { reply }, accept_encoding: [:gzip]).call(rpc, "hi") }
      .to raise_error(ConnectRPC::Error, "Cannot decompress response")
  end

  it "keeps error responses identity and safely bounds the replacement and empty fallback" do
    allow(handler).to receive(:call) { raise ConnectRPC::Error.new(:invalid_argument, "x" * 200) }
    [1, 80, 400].each do |limit|
      response = dispatch(Zlib.gzip("hello"), { "content-encoding" => ["gzip"] }, encoding: :gzip,
                                                                                  response_wire_limit: limit)
      expect(response.headers).not_to have_key("content-encoding")
      expect(response.body.bytesize).to be <= limit
      expect(response.status).to eq(limit == 400 ? 400 : 429)
    end
  end

  %i[request_limit response_limit request_wire_limit response_wire_limit].each do |option|
    it "validates positive integer #{option}" do
      [0, -1, 1.5, Float::INFINITY, nil].each do |value|
        expect { server(**{ option => value }) }.to raise_error(ArgumentError)
        expect { client(transport: ->(_) {}, **{ option => value }) }.to raise_error(ArgumentError)
      end
    end
  end

  it "preserves actual gzip bytes, Content-Encoding and Content-Length through NetHTTP" do
    compressed = Zlib.gzip("opaque" * 20)
    peer = lambda do |socket|
      request = read_request(socket)
      expect(request).to match(/Accept-Encoding: gzip, identity\r\n/i)
      socket.write("HTTP/1.1 200 OK\r\nContent-Type: application/proto\r\nContent-Encoding: gzip\r\n" \
                   "Content-Length: #{compressed.bytesize}\r\nConnection: close\r\n\r\n" + compressed)
    end
    with_peer(peer) do |url, _listener|
      request = transport_request(url)
      request.headers["accept-encoding"] = ["gzip, identity"]
      response = ConnectRPC::Transports::NetHTTP.new.call(request)
      expect(response.body).to eq(compressed)
      expect(response.headers["content-encoding"]).to eq(["gzip"])
      expect(response.headers["content-length"]).to eq([compressed.bytesize.to_s])
    end
  end

  %i[proto json].each do |codec|
    it "exchanges real Google #{codec} gzip with opaque HTTP and identity errors" do
      handler = ->(message, _context) { message }
      endpoint = ConnectRPC::Server.new(services: [EchoExample::SERVICE], binding: EchoExample::BINDING,
                                        encoding: :gzip,
                                        handlers: { EchoExample::ECHO.path => handler })
      host = EchoExample::Localhost.new(ConnectRPC::Rack.new(server: endpoint))
      peer = ConnectRPC::Client.new(base_url: host.url, binding: EchoExample::BINDING, codec: codec, encoding: :gzip)
      message = EchoExample::Message.new(name: "gzip" * 100, count: 42)
      expect(peer.call(EchoExample::ECHO, message, timeout: nil).message).to eq(message)
      expect { peer.call(EchoExample::FAIL, message) }.to raise_error(ConnectRPC::Error) do |error|
        expect(error.code).to eq("unimplemented")
      end
    ensure
      host&.close
    end
  end
end

RSpec.describe ConnectRPC::Compression do
  it "stops an inflation bomb before retaining its first 16 KiB output chunk" do
    wire = Zlib.gzip("x" * (8 * 1024 * 1024))
    stream = nil
    yielded = []
    allow(Zlib::Inflate).to receive(:new).and_wrap_original do |original, *arguments|
      stream = original.call(*arguments)
      allow(stream).to receive(:inflate).and_wrap_original do |inflate, input, &callback|
        inflate.call(input) do |chunk|
          yielded << chunk.bytesize
          callback.call(chunk)
        end
      end
      stream
    end
    expect { described_class.decode(wire, limit: 1) }.to raise_error(ConnectRPC::Error) do |error|
      expect(error.code).to eq("resource_exhausted")
    end
    expect(yielded).to eq([16_384])
    expect(stream).to be_closed
  end

  %i[encode decode].each do |operation|
    it "checks #{operation} chunk progress against the same deadline and closes on expiry" do
      now = 0.0
      deadline = ConnectRPC::Deadline.new(1, clock: -> { now })
      bytes = Random.new(42).bytes(40_000)
      bytes = Zlib.gzip(bytes) if operation == :decode
      klass = operation == :decode ? Zlib::Inflate : Zlib::Deflate
      native_method = operation == :decode ? :inflate : :deflate
      stream = nil
      allow(klass).to receive(:new).and_wrap_original do |original, *arguments|
        stream = original.call(*arguments)
        allow(stream).to receive(native_method).and_wrap_original do |native, chunk, &callback|
          native.call(chunk) do |output|
            now = 2.0
            callback.call(output)
          end
        end
        stream
      end
      expect { described_class.public_send(operation, bytes, limit: 100_000, deadline: deadline) }
        .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("deadline_exceeded") }
      expect(stream).to be_closed
    end
  end

  it "checks actual expiry after a native data error and closes the stream" do
    now = 0.0
    deadline = ConnectRPC::Deadline.new(1, clock: -> { now })
    stream = nil
    allow(Zlib::Inflate).to receive(:new).and_wrap_original do |original, *arguments|
      stream = original.call(*arguments)
      allow(stream).to receive(:inflate).and_wrap_original do |inflate, chunk, &callback|
        inflate.call(chunk, &callback)
      ensure
        now = 2.0
      end
      stream
    end
    expect { described_class.decode("bad native header", limit: 100, deadline: deadline) }
      .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("deadline_exceeded") }
    expect(stream).to be_closed
  end

  it "closes streams on success, corruption, wire overflow and footer overflow" do
    streams = []
    [Zlib::Deflate, Zlib::Inflate].each do |klass|
      allow(klass).to receive(:new).and_wrap_original do |original, *arguments|
        original.call(*arguments).tap { |stream| streams << stream }
      end
    end
    wire = described_class.encode("hello", limit: 100)
    expect(described_class.decode(wire, limit: 5)).to eq("hello")
    expect { described_class.decode(wire[0...-1], limit: 5) }.to raise_error(described_class::Malformed)
    [1, wire.bytesize - 1].each do |limit|
      expect { described_class.encode("hello", limit: limit) }.to raise_error(ConnectRPC::Error) do |error|
        expect(error.code).to eq("resource_exhausted")
      end
    end
    expect(streams).to all(be_closed)
  end
end
