# frozen_string_literal: true

RSpec.describe ConnectRPC::Testing::Transport do
  let(:binding) { ConnectRPC::Bindings::GoogleProtobuf.new }
  let(:service) { Example::V1::EchoService }
  let(:type) { Example::V1::EchoMessage }
  let(:calls) { [] }
  let(:handler) do
    lambda do |message, context|
      calls << [message, context]
      context.response_metadata["x-leading"] = ["one", "two,three"]
      context.response_metadata.add_binary("x-bytes-bin", "\x00\xff".b)
      context.response_trailers["x-trailing"] = %w[four five]
      message
    end
  end
  let(:server_options) { {} }
  let(:server) do
    fail_handler = lambda do |message, context|
      handler.call(message, context)
      detail = ConnectRPC::ErrorDetail.new(type: type.descriptor.name, value: type.encode(message))
      raise ConnectRPC::Error.new(:invalid_argument, "safe", details: [detail], metadata: { "x-error" => ["yes"] },
                                                             trailers: { "x-error-trailer" => ["done"] })
    end
    ConnectRPC::Server.new(services: [service::SERVICE], binding: binding, encoding: :gzip,
                           handlers: service.handlers(echo: handler, fail: fail_handler), **server_options)
  end
  let(:transport) { described_class.new(server: server) }

  def request(**changes)
    ConnectRPC::TransportRequest.new(url: URI("https://example.invalid#{service::ECHO.path}"), http_method: "POST",
                                     headers: { "content-type" => ["application/proto"] }, body: "".b,
                                     response_limit: 4096, deadline: nil, **changes)
  end

  def client(**)
    core = ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding, transport: transport,
                                  **)
    service::Client.new(client: core)
  end

  def error_code(code, &)
    expect(&).to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq(code) }
  end

  %i[proto json].product(%i[identity gzip]).each do |codec, encoding|
    it "dispatches generated typed #{codec}/#{encoding} success and error with metadata and observed bytes" do
      observed = []
      real = transport
      wrapper = lambda do |outgoing|
        response = real.call(outgoing)
        observed << [outgoing, response]
        response
      end
      core = ConnectRPC::Client.new(base_url: "https://test.invalid", binding: binding, transport: wrapper,
                                    codec: codec, encoding: encoding)
      generated = service::Client.new(client: core)
      message = type.new(name: "héllo 世界", count: 9, data: "\x00\xff".b)
      result = generated.echo(message, metadata: { "x-repeat" => ["a", "b,c"], "x-bin" => ["AA", "/w"] })
      expect(result.message).to eq(message)
      expect(calls.last.first).to be_a(type)
      expect(calls.last.last.request_metadata["x-repeat"]).to eq(["a", "b,c"])
      expect(calls.last.last.request_metadata.binary("x-bin")).to eq(["\0".b, "\xff".b])
      expect(result.metadata["x-leading"]).to eq(["one", "two,three"])
      expect(result.metadata.binary("x-bytes-bin")).to eq(["\0\xff".b])
      expect(result.trailers["x-trailing"]).to eq(%w[four five])
      outgoing, response = observed.last
      expected = binding.encode(message, type: type, codec: codec)
      if encoding == :gzip
        expect(outgoing.body.byteslice(0, 2)).to eq("\x1f\x8b".b)
        expect(response.headers["content-encoding"]).to eq(["gzip"])
        expect(Zlib.gunzip(outgoing.body)).to eq(expected.b)
        expect(Zlib.gunzip(response.body)).to eq(expected.b)
      else
        expect(outgoing.body.b).to eq(expected.b)
        expect(response.body.b).to eq(expected.b)
      end
      expect { generated.fail(message) }.to raise_error(ConnectRPC::Error) do |error|
        expect([error.code, error.message]).to eq(%w[invalid_argument safe])
        expect(error.details.first.decode(binding: binding, type: type)).to eq(message)
        expect(error.metadata["x-leading"]).to eq(["one", "two,three"])
        expect(error.metadata["x-error"]).to eq(["yes"])
        expect(error.trailers["x-trailing"]).to eq(%w[four five])
        expect(error.trailers["x-error-trailer"]).to eq(["done"])
      end
      expect(generated.echo(type.new, timeout: nil).message).to eq(type.new)
      expect(calls.last.last.deadline).to be_nil
    end
  end

  it "preserves frozen caller input and ignores query" do
    outgoing = request(url: URI("https://test.invalid#{service::ECHO.path}?anything=1").freeze,
                       headers: { "content-type" => ["application/proto"].freeze }.freeze, body: "".b.freeze).freeze
    expect(transport.call(outgoing).status).to eq(200)
    expect(outgoing.url.query).to eq("anything=1")
    expect(outgoing.headers).to eq("content-type" => ["application/proto"])
  end

  ["/unrelated/Call", "/prefix/example.v1.EchoService/Echo", "/example.v1.EchoService/Unknown",
   "/example.v1.EchoService/%45cho"].each do |path_value|
    it "keeps literal unknown path #{path_value}" do
      response = transport.call(request(url: URI("http://test.invalid#{path_value}")))
      expect(response.status).to eq(501)
      expect(JSON.parse(response.body).fetch("code")).to eq("unimplemented")
      expect(calls).to be_empty
    end
  end

  it "passes the actual verb and preserves core validation precedence" do
    response = transport.call(request(http_method: "GET", headers: {}, url: URI("http://test.invalid/unknown")))
    expect(response.to_h).to eq(status: 405, headers: { "allow" => ["POST"] }, body: "")
    expect(transport.call(request(headers: {}, url: URI("http://test.invalid/unknown"))).status).to eq(415)
    expect(calls).to be_empty
  end

  [
    [{ "connect-protocol-version" => %w[1 1] }, "", "invalid_argument"],
    [{ "connect-protocol-version" => ["2"] }, "", "invalid_argument"],
    [{ "connect-timeout-ms" => ["bad"] }, "", "invalid_argument"],
    [{ "connect-timeout-ms" => %w[1 2] }, "", "invalid_argument"],
    [{ "content-encoding" => %w[gzip identity] }, "", "unimplemented"],
    [{ "content-encoding" => ["br"] }, "", "unimplemented"],
    [{ "content-length" => %w[1 1] }, "", "invalid_argument"],
    [{ "content-length" => ["2"] }, "", "invalid_argument"],
    [{}, "\xff".b, "invalid_argument"],
    [{ "content-type" => ["application/json"] }, "{", "invalid_argument"]
  ].each_with_index do |(headers, body, code), index|
    it "preserves direct core malformed protocol response #{index}" do
      outgoing = request(headers: { "content-type" => ["application/proto"] }.merge(headers), body: body.freeze)
      require "stringio"
      direct = server.call(path: outgoing.url.path, http_method: outgoing.http_method, headers: outgoing.headers,
                           input: StringIO.new(body))
      expect(transport.call(outgoing).to_h).to eq(direct.to_h)
      expect(JSON.parse(direct.body).fetch("code")).to eq(code)
      expect(calls).to be_empty
    end
  end

  it "reads multibyte JSON across bounded byte chunks" do
    message = type.new(name: "世界" * 6000)
    expect(client(codec: :json).echo(message).message).to eq(message)
  end

  it "rejects non-server callables and accepts a Server subclass" do
    expect { described_class.new(server: -> {}) }.to raise_error(ArgumentError)
    subclass = Class.new(ConnectRPC::Server).new(services: [], binding: binding)
    expect(described_class.new(server: subclass).call(request).status).to eq(501)
  end

  it "rejects non-request objects" do
    expect { transport.call(request.to_h) }.to raise_error(ArgumentError)
    expect(calls).to be_empty
  end

  invalid = { url: ["http://test.invalid", URI("/relative"), URI("ftp://test.invalid/a"),
                    URI("http://u:p@test.invalid/a"), URI("http://test.invalid/a#f"), URI("http:/a")],
              http_method: [:POST, nil], body: [nil, Object.new],
              headers: [nil, [], { "Content-Type" => ["x"] }, { x: ["x"] }, { "x" => "x" }, { "x" => [1] }],
              response_limit: [nil, 0, -1, 1.5, "1"], deadline: [false, Object.new] }
  invalid.each do |field, values|
    values.each_with_index do |value, index|
      it "rejects structural #{field} variant #{index} before dispatch" do
        expect { transport.call(request(**{ field => value })) }.to raise_error(ArgumentError)
        expect(calls).to be_empty
      end
    end
  end

  %i[request_limit request_wire_limit].each do |bound|
    context "server #{bound}" do
      let(:server_options) { { bound => 1 } }
      it "rejects excess before invoking the handler" do
        response = transport.call(request(body: type.encode(type.new(name: "large"))))
        expect(JSON.parse(response.body).fetch("code")).to eq("resource_exhausted")
        expect(calls).to be_empty
      end
    end
  end

  it "enforces decoded gzip request limit before dispatch" do
    bounded = ConnectRPC::Server.new(services: [service::SERVICE], binding: binding, encoding: :gzip,
                                     request_limit: 4, request_wire_limit: 200,
                                     handlers: service.handlers(echo: handler, fail: handler))
    compressed = Zlib.gzip(type.encode(type.new(name: "a" * 100)))
    response = described_class.new(server: bounded).call(request(body: compressed,
                                                                 headers: { "content-type" => ["application/proto"],
                                                                            "content-encoding" => ["gzip"] }))
    expect(JSON.parse(response.body).fetch("code")).to eq("resource_exhausted")
    expect(calls).to be_empty
  end

  %i[request_limit request_wire_limit].each do |bound|
    it "enforces local client #{bound} before dispatch" do
      error_code("resource_exhausted") { client(**{ bound => 1 }).echo(type.new(name: "large")) }
      expect(calls).to be_empty
    end
  end

  it "enforces local decoded response limits independently of compressed wire limits" do
    error_code("resource_exhausted") do
      client(encoding: :gzip, response_limit: 1, response_wire_limit: 200).echo(type.new(name: "large"))
    end
    expect(calls.size).to eq(1)
  end

  %i[response_limit response_wire_limit].each do |bound|
    context "tiny server #{bound}" do
      let(:server_options) { { bound => 1 } }
      it "preserves empty 429 and client HTTP fallback" do
        outgoing = request(body: type.encode(type.new(name: "large")))
        expect(transport.call(outgoing).to_h).to include(status: 429, body: "")
        error_code("unavailable") { client.echo(type.new(name: "large")) }
      end
    end
  end

  %i[identity gzip error].each do |kind|
    it "directly enforces #{kind} wire cap at boundary and boundary plus one" do
      outgoing = request(body: type.encode(type.new(name: "a")),
                         url: URI("http://test.invalid#{kind == :error ? service::FAIL.path : service::ECHO.path}"),
                         headers: { "content-type" => ["application/proto"],
                                    "accept-encoding" => [kind == :gzip ? "gzip" : "identity"] })
      response = transport.call(outgoing)
      cap = response.body.bytesize
      expect(cap).to be > 1
      outgoing.response_limit = cap
      expect(transport.call(outgoing).to_h).to eq(response.to_h)
      outgoing.response_limit = cap - 1
      error_code("resource_exhausted") { transport.call(outgoing) }
      expect(Zlib.gunzip(response.body).bytesize).to be < cap - 1 if kind == :gzip
    end
  end

  it "leaves the client's omitted default at 30 seconds" do
    client.echo(type.new)
    expect(calls.last.last.deadline.remaining).to be_between(29, 30)
  end

  it "refreshes remaining deadline before dispatch and preserves frozen headers" do
    now = 0.0
    deadline = ConnectRPC::Deadline.new(10, clock: -> { now })
    now = 7.0
    headers = { "content-type" => ["application/proto"].freeze, "connect-timeout-ms" => ["10000"].freeze }.freeze
    outgoing = request(headers: headers, deadline: deadline).freeze
    expect(transport.call(outgoing).status).to eq(200)
    expect(calls.last.last.deadline.remaining).to be_between(2.9, 3.0)
    expect(headers["connect-timeout-ms"]).to eq(["10000"])
  end

  it "rejects an expired original deadline before dispatch" do
    now = 0
    deadline = ConnectRPC::Deadline.new(1, clock: -> { now })
    now = 1
    error_code("deadline_exceeded") { transport.call(request(deadline: deadline)) }
    expect(calls).to be_empty
  end

  it "preserves explicit low-level timeout with a nil deadline" do
    transport.call(request(headers: { "content-type" => ["application/proto"], "connect-timeout-ms" => ["4000"] }))
    expect(calls.last.last.deadline.remaining).to be_between(3.9, 4.0)
  end

  it "uses actual cooperative Context deadline checks without sleeps" do
    now = 100.0
    allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { now }
    allow(handler).to receive(:call) do |_message, context|
      calls << context
      now += 2
      expect(context.cancelled?).to be(true)
      context.check!
    end
    response = transport.call(request(headers: { "content-type" => ["application/proto"],
                                                 "connect-timeout-ms" => ["1000"] }))
    expect(JSON.parse(response.body).fetch("code")).to eq("deadline_exceeded")
    expect(calls.size).to eq(1)
  end

  %i[success error].each do |kind|
    it "checks original post-handler expiry before oversized #{kind} response" do
      now = 0
      deadline = ConnectRPC::Deadline.new(1, clock: -> { now })
      allow(handler).to receive(:call) do |message, _context|
        now = 1
        message
      end
      outgoing = request(deadline: deadline, response_limit: 1, body: type.encode(type.new(name: "large")),
                         url: URI("http://test.invalid#{kind == :error ? service::FAIL.path : service::ECHO.path}"))
      error_code("deadline_exceeded") { transport.call(outgoing) }
    end
  end
  it "preserves the exact Server response object, bytes and repeated fields" do
    returned = nil
    allow(server).to receive(:call).and_wrap_original do |original, **arguments|
      returned = original.call(**arguments)
    end
    response = transport.call(request(body: type.encode(type.new(name: "opaque"))))
    expect(response).to equal(returned)
    expect(response.headers["x-leading"]).to eq(["one", "two,three"])
  end

  it "preserves frozen malformed caller input on core failure" do
    headers = { "content-type" => ["application/proto"].freeze,
                "content-length" => ["2"].freeze }.freeze
    outgoing = request(url: URI("https://test.invalid#{service::ECHO.path}?query=1").freeze,
                       headers: headers, body: "".b.freeze).freeze
    expect(transport.call(outgoing).status).to eq(400)
    expect(outgoing.headers).to eq(headers)
    expect(outgoing.body).to eq("")
    expect(outgoing.url.query).to eq("query=1")
    expect(calls).to be_empty
  end

  it "enforces local client response wire cap below the Server bound" do
    error_code("resource_exhausted") { client(response_wire_limit: 1).echo(type.new(name: "large")) }
    expect(calls.size).to eq(1)
  end

  it "rejects gzip framing above Server response wire cap with a decodable error" do
    bounded = ConnectRPC::Server.new(services: [service::SERVICE], binding: binding, encoding: :gzip,
                                     response_limit: 1000, response_wire_limit: 100,
                                     handlers: service.handlers(echo: handler, fail: handler))
    message = type.new(data: (0..255).to_a.pack("C*"))
    outgoing = request(body: type.encode(message),
                       headers: { "content-type" => ["application/proto"], "accept-encoding" => ["gzip"] })
    response = described_class.new(server: bounded).call(outgoing)
    expect(response.status).to eq(429)
    expect(JSON.parse(response.body).fetch("code")).to eq("resource_exhausted")
    expect(calls.size).to eq(1)
  end

  it "enforces Server decoded response cap before compression with a decodable error" do
    bounded = ConnectRPC::Server.new(services: [service::SERVICE], binding: binding, encoding: :gzip,
                                     response_limit: 100, response_wire_limit: 1000,
                                     handlers: service.handlers(echo: handler, fail: handler))
    message = type.new(name: "a" * 200)
    outgoing = request(body: type.encode(message),
                       headers: { "content-type" => ["application/proto"], "accept-encoding" => ["gzip"] })
    response = described_class.new(server: bounded).call(outgoing)
    expect(response.status).to eq(429)
    expect(JSON.parse(response.body).fetch("code")).to eq("resource_exhausted")
    expect(calls.size).to eq(1)
  end
end
