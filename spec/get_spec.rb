# frozen_string_literal: true

require "stringio"

RSpec.describe "Connect unary GET" do
  let(:binding) do
    Class.new do
      def encode(message, **) = message
      def decode(bytes, **) = bytes
    end.new
  end
  let(:method) do
    ConnectRPC::Method.new(service: "get.Service", name: "Read", request_type: String, response_type: String,
                           idempotency: :no_side_effects)
  end
  let(:service) { ConnectRPC::Service.new(name: method.service, methods: [method]) }
  let(:server) do
    ConnectRPC::Server.new(services: [service], binding: binding, handlers: { method.path => ->(value, _) { value } })
  end

  def get(query, headers: {}, input: nil, target: server, **)
    target.call(path: method.path, http_method: "GET", query: query, headers: headers, input: input, **)
  end

  def roundtrip(codec: :proto, **)
    observed = []
    transport = lambda do |request|
      observed << request
      server.call(path: request.url.path, query: request.url.query || "", http_method: request.http_method,
                  headers: request.headers, input: StringIO.new(request.body))
    end
    client = ConnectRPC::Client.new(base_url: "http://example.invalid", binding: binding, transport: transport,
                                    codec: codec, use_get: true, **)
    [client, observed]
  end

  it "dispatches a safe binary GET with an empty body and canonical query" do
    client, observed = roundtrip
    expect(client.call(method, "\x00\xff".b).message).to eq("\x00\xff".b)
    expect(observed.one?).to be(true)
    request = observed.first
    expect(request.http_method).to eq("GET")
    expect(request.body).to eq("")
    expect(request.url.query).to eq("connect=v1&base64=1&encoding=proto&message=AP8")
    expect(request.headers.keys).not_to include("content-type", "content-encoding", "content-length",
                                                "connect-protocol-version")
  end

  it "decodes a bodyless JSON GET without requiring POST headers" do
    result = get("connect=v1&encoding=json&message=%7B%22x%22%3A%22hello+%2B+%E9%9B%AA%22%7D")
    expect(result.status).to eq(200)
    expect(result.body.force_encoding(Encoding::UTF_8)).to eq('{"x":"hello + 雪"}')
    expect(result.headers["vary"]).to eq(["Accept-Encoding"])
  end
  it "validates safety metadata and selects GET only for explicitly safe opt-in calls" do
    %i[unknown idempotent no_side_effects].each do |safety|
      declaration = ConnectRPC::Method.new(service: method.service, name: "Call", request_type: String,
                                           response_type: String, idempotency: safety)
      expect(declaration.idempotency).to eq(safety)
      expect(declaration).to be_frozen
      [false, true].each do |enabled|
        captured = nil
        transport = lambda do |request|
          captured = request
          ConnectRPC::TransportResponse.new(status: 200, headers: { "content-type" => ["application/proto"] }, body: "")
        end
        client = ConnectRPC::Client.new(base_url: "http://example.invalid", binding: binding, transport: transport,
                                        use_get: enabled)
        client.call(declaration, "")
        expect(captured.http_method).to eq(enabled && safety == :no_side_effects ? "GET" : "POST")
      end
    end
    expect(ConnectRPC::Method.new(service: method.service, name: "Default", request_type: String,
                                  response_type: String).idempotency).to eq(:unknown)
    expect do
      ConnectRPC::Method.new(service: method.service, name: "Bad", request_type: String, response_type: String,
                             idempotency: :safe)
    end.to raise_error(ArgumentError, /idempotency/)
    [{ use_get: nil }, { use_get: 1 }, { get_url_limit: 0 }, { get_url_limit: 1.5 }].each do |options|
      expect { roundtrip(**options) }.to raise_error(ArgumentError)
    end
    [false, 0, 1.5].each do |limit|
      expect { ConnectRPC::Server.new(services: [service], binding: binding, get_query_limit: limit) }
        .to raise_error(ArgumentError)
    end
    expect { ConnectRPC::Server.new(services: [service], binding: binding, request_wire_limit: "bad") }
      .to raise_error(ArgumentError)
  end

  it "uses form encoding once for JSON including plus, space, Unicode and punctuation" do
    client, observed = roundtrip(codec: :json)
    value = "雪 + &=%;/?#"
    expect(client.call(method, value).message.force_encoding(Encoding::UTF_8)).to eq(value)
    expect(observed.first.url.query).to eq("connect=v1&encoding=json&message=%E9%9B%AA+%2B+%26%3D%25%3B%2F%3F%23")
  end

  it "accepts arbitrary parameter order, binary unknowns, duplicate unknowns, empty fields and raw semicolons" do
    result = get("&x=%FF&encoding=proto&x=%FE&%2565ncoding=ignored&base64=1&connect=&compression=&message=YTs9PQ==&&")
    expect(result.status).to eq(200)
    expect(result.body).to eq("a;==")
    expect(get("encoding=json&message=a;b=c").body).to eq("a;b=c")
    expect(get("encoding=proto&message").status).to eq(200)
    expect(get("encoding=proto&message").body).to eq("")
  end

  ["YQ", "YQ==", "YQ%3D%3D"].each do |encoded|
    it "accepts URL-safe base64 #{encoded}" do
      expect(get("encoding=proto&base64=1&message=#{encoded}").body).to eq("a")
    end
  end

  ["Y", "YQ=", "YQ===", "Y=Q=", "Yh==", "%2Fw", "%2Bw", "a!", "YQ%FF"].each do |encoded|
    it "rejects invalid URL-safe base64 #{encoded}" do
      result = get("encoding=proto&base64=1&message=#{encoded}")
      expect(result.status).to eq(400)
      expect(JSON.parse(result.body)["code"]).to eq("invalid_argument")
    end
  end

  ["encoding=json&encod%69ng=json&message=", "message=&message=&encoding=json", "x=%", "x=%GG",
   "x=%2", "%FF=%0X", "encoding=json&message=%FF"].each do |query|
    it "rejects malformed/ambiguous queries #{query}" do
      expect(get(query).status).to eq(400)
    end
  end

  %w[connect base64 compression].each do |control|
    it "rejects repeated #{control} controls even when identical" do
      result = get("encoding=proto&message=&#{control}=&#{control}=")
      expect(result.status).to eq(400)
      expect(JSON.parse(result.body)["code"]).to eq("invalid_argument")
    end
  end

  it "validates text UTF-8 even when a custom binding accepts arbitrary bytes" do
    expect(binding).not_to receive(:decode)
    expect(get("encoding=proto&base64=other&message=%FF").status).to eq(400)
  end

  it "lets query controls override irrelevant POST protocol headers" do
    result = get("connect=v1&encoding=proto&message=hello", headers: {
                   "content-type" => ["application/grpc"], "content-encoding" => ["br"],
                   "connect-protocol-version" => ["wrong"]
                 })
    expect(result.status).to eq(200)
    expect(result.body).to eq("hello")
  end

  it "preserves codec, body, compression, version, message and timeout rejection precedence" do
    headers = { "connect-timeout-ms" => ["bad"] }
    expect(get("compression=br&connect=bad", headers: headers).status).to eq(415)
    expect(get("encoding=proto&compression=br&connect=bad", headers: headers,
                                                            input: StringIO.new("x")).status).to eq(415)
    expect(get("encoding=proto&compression=br&connect=bad", headers: headers).status).to eq(501)
    result = get("encoding=proto&connect=bad", headers: headers)
    expect(JSON.parse(result.body)["message"]).to include("version")
    expect(JSON.parse(get("encoding=proto", headers: headers).body)["message"]).to include("Missing")
    expect(JSON.parse(get("encoding=proto&message=", headers: headers).body)["message"]).to include("timeout")
    expect(get("encoding=PROTO&message=").status).to eq(415)
    expect(get("encoding=proto&connect=V1&message=").status).to eq(400)
    expect(get("encoding=proto&compression=IDENTITY&message=").status).to eq(200)
  end

  it "rejects unsafe GET before touching query, headers, body or decoder, and advertises safe verbs" do
    unsafe = ConnectRPC::Method.new(service: method.service, name: "Write", request_type: String, response_type: String)
    merely = ConnectRPC::Method.new(service: method.service, name: "Retry", request_type: String, response_type: String,
                                    idempotency: :idempotent)
    declarations = ConnectRPC::Service.new(name: method.service, methods: [method, unsafe, merely])
    target = ConnectRPC::Server.new(services: [declarations],
                                    binding: binding)
    expect(binding).not_to receive(:decode)
    [unsafe, merely].each do |declaration|
      result = target.call(path: declaration.path, http_method: "GET", query: "%", headers: nil,
                           input: double("unread"))
      expect(result).to have_attributes(status: 405, body: "")
      expect(result.headers).to eq("allow" => ["POST"], "vary" => ["Accept-Encoding"])
    end
    %w[HEAD PUT DELETE].each do |verb|
      result = target.call(path: method.path, http_method: verb, headers: {}, input: nil)
      expect(result.status).to eq(405)
      expect(result.headers["allow"]).to eq(["GET, POST"])
    end
    result = target.call(path: "/#{method.service}/Unknown", http_method: "GET", query: "encoding=proto&message=",
                         headers: {}, input: nil)
    expect(result.status).to eq(501)
  end

  it "validates Content-Length then reads at most one actual body byte" do
    %w[1 99999999].each do |length|
      input = double("unread")
      expect(input).not_to receive(:read)
      expect(get("encoding=proto&message=", headers: { "content-length" => [length] }, input: input).status).to eq(415)
    end
    ["bad", "-1", "1, 1"].each do |length|
      input = double("unread")
      expect(input).not_to receive(:read)
      expect(get("encoding=proto&message=", headers: { "content-length" => [length] }, input: input).status).to eq(400)
    end
    input = StringIO.new("unbounded arbitrary body")
    expect(input).to receive(:read).with(1).once.and_call_original
    expect(get("encoding=proto&message=", headers: { "content-length" => ["0"] }, input: input).status).to eq(415)
    expect(input.pos).to eq(1)
    expect(get("encoding=proto&message=", headers: { "content-length" => ["0"] }).status).to eq(200)
  end

  it "enforces complete absolute URL bytes before dispatch without POST fallback" do
    client, observed = roundtrip
    client.call(method, "a")
    limit = observed.first.url.to_s.bytesize
    expect(roundtrip(get_url_limit: limit).first.call(method, "a").message).to eq("a")
    limited, requests = roundtrip(get_url_limit: limit - 1)
    expect { limited.call(method, "a") }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("resource_exhausted") }
    expect(requests).to be_empty
    limited, requests = roundtrip(codec: :json, get_url_limit: limit)
    expect { limited.call(method, "雪" * 10) }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("resource_exhausted") }
    expect(requests).to be_empty
  end

  it "independently bounds serialized, decoded-wire, raw-query and inflated-message bytes" do
    limited = ConnectRPC::Server.new(services: [service], binding: binding, handlers: { method.path => ->(x, _) { x } },
                                     request_limit: 1, request_wire_limit: 1)
    query = "encoding=proto&base64=1&message=%59%51%3D%3D"
    derived = (3 * (4 * ((1 + 2) / 3))) + 1024
    query += "&x=#{'a' * (derived - query.bytesize - 3)}"
    expect(query.bytesize).to eq(derived)
    expect(get(query, target: limited).body).to eq("a")
    expect(get("#{query}a", target: limited).status).to eq(429)
    expect(get("encoding=proto&base64=1&message=YWI", target: limited).status).to eq(429)
    expect(get("encoding=proto&message=ab", target: limited).status).to eq(429)
    compressed = ConnectRPC::Compression.encode("ab", limit: 1000)
    encoded = [compressed].pack("m0").tr("+/", "-_").delete("=")
    compressed_query = "encoding=proto&compression=gzip&base64=1&message=#{encoded}"
    target = ConnectRPC::Server.new(services: [service], binding: binding, handlers: { method.path => ->(x, _) { x } },
                                    encoding: :gzip, request_limit: 2, request_wire_limit: compressed.bytesize)
    expect(get(compressed_query, target: target).status).to eq(200)
    too_small = ConnectRPC::Server.new(services: [service], binding: binding, encoding: :gzip, request_limit: 1,
                                       request_wire_limit: compressed.bytesize)
    expect(get(compressed_query, target: too_small).status).to eq(429)
    too_small = ConnectRPC::Server.new(services: [service], binding: binding, encoding: :gzip, request_limit: 2,
                                       request_wire_limit: compressed.bytesize - 1)
    expect(get(compressed_query, target: too_small).status).to eq(429)
    client, requests = roundtrip(request_limit: 1, request_wire_limit: 100, encoding: :gzip)
    expect { client.call(method, "ab") }.to raise_error(ConnectRPC::Error) { |e| expect(e.code).to eq("resource_exhausted") }
    expect(requests).to be_empty
  end

  it "bounds outgoing encoded-wire bytes independently for identity and gzip GET" do
    %i[identity gzip].each do |coding|
      wire = coding == :gzip ? ConnectRPC::Compression.encode("hello", limit: 1000) : "hello"
      captured = []
      transport = lambda do |request|
        captured << request
        ConnectRPC::TransportResponse.new(status: 200, headers: { "content-type" => ["application/proto"] },
                                          body: "hello")
      end
      [wire.bytesize, wire.bytesize - 1].each do |limit|
        core = ConnectRPC::Client.new(base_url: "http://example.invalid/prefix%20data/", binding: binding,
                                      transport: transport, use_get: true, encoding: coding,
                                      request_limit: 5, request_wire_limit: limit)
        if limit == wire.bytesize
          expect(core.call(method, "hello").message).to eq("hello")
          expect(captured.last.http_method).to eq("GET")
          expect(captured.last.url.path).to start_with("/prefix%20data/")
        else
          count = captured.size
          expect { core.call(method, "hello") }.to raise_error(ConnectRPC::Error) { |e|
            expect(e.code).to eq("resource_exhausted")
          }
          expect(captured.size).to eq(count)
        end
      end
    end
  end

  it "handles gzip, concatenation, malformed gzip and measured-empty gzip queries" do
    target = ConnectRPC::Server.new(services: [service], binding: binding, handlers: { method.path => ->(x, _) { x } },
                                    encoding: :gzip)
    compressed = ConnectRPC::Compression.encode("hello", limit: 1000)
    encoded = [compressed + compressed].pack("m0").tr("+/", "-_").delete("=")
    result = get("encoding=proto&base64=1&compression=GZIP&message=#{encoded}", target: target)
    expect(ConnectRPC::Compression.decode(result.body, limit: 1000)).to eq("hellohello")
    malformed = get("encoding=proto&base64=1&compression=gzip&message=bm90IGd6aXA", target: target)
    expect(malformed.status).to eq(400)
    expect(JSON.parse(malformed.body)).to include("code" => "invalid_argument",
                                                  "message" => "Cannot decompress request")
    expect(get("encoding=proto&base64=1&compression=gzip&message=", target: target).status).to eq(200)
    expect(get("encoding=proto&compression=gzip&message=").status).to eq(501)
  end

  it "keeps valid deadlines active during parser and body work" do
    now = 0.0
    allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { now }
    input = double("progress")
    allow(input).to receive(:read).with(1) {
      now = 0.1
      nil
    }
    expect(binding).not_to receive(:decode)
    result = get("encoding=proto&message=", headers: { "connect-timeout-ms" => ["1"] }, input: input)
    expect(result.status).to eq(504)
    now = 0.0
    queries = "encoding=proto&message=a&unknown=#{'a' * 5000}"
    reads = 0
    allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) {
      reads += 1
      reads > 5 ? 0.1 : 0.0
    }
    expect(get(queries, headers: { "connect-timeout-ms" => ["1"] }).status).to eq(504)
  end

  it "merges Vary after application/error metadata while preserving wildcard and cache policy" do
    [["Origin", "Accept-Encoding"], ["ORIGIN, accept-ENCODING"], ["*"]].each do |vary|
      handler = lambda do |_, context|
        context.response_metadata["vary"] = vary
        context.response_metadata["cache-control"] = ["private, max-age=5"]
        "hello"
      end
      target = ConnectRPC::Server.new(services: [service], binding: binding, handlers: { method.path => handler })
      result = get("encoding=proto&message=", target: target)
      expect(result.headers["vary"]).to eq(vary)
      expect(result.headers["cache-control"]).to eq(["private, max-age=5"])
    end
    handler = lambda do |_, context|
      context.response_metadata["vary"] = %w[Origin X-Other]
      ""
    end
    target = ConnectRPC::Server.new(services: [service], binding: binding, handlers: { method.path => handler })
    expect(get("encoding=proto&message=", target: target).headers["vary"]).to eq(%w[Origin X-Other Accept-Encoding])
    handler = ->(_, _) { raise ConnectRPC::Error.new(:permission_denied, "safe", metadata: { "vary" => "Origin" }) }
    target = ConnectRPC::Server.new(services: [service], binding: binding, handlers: { method.path => handler })
    expect(get("encoding=proto&message=", target: target).headers["vary"]).to eq(%w[Origin Accept-Encoding])
    expect(get("", target: target).headers).to include("vary" => ["Accept-Encoding"])
    expect(get("").headers).not_to have_key("cache-control")
  end
  it "preserves unknown POST byte measurement when header preparation expires" do
    now = 0.0
    allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { now }
    outcomes = []
    subscriber = Object.new
    observer = Object.new
    observer.define_singleton_method(:processing_finished) { |outcome| outcomes << outcome }
    subscriber.define_singleton_method(:start) { |_| observer }
    bus = ConnectRPC::Instrumentation.new(subscribers: [subscriber])
    deadline = ConnectRPC::Deadline.new(1)
    allow(ConnectRPC::Deadline).to receive(:new).and_return(deadline)
    allow(deadline).to receive(:timeout_ms) do
      now = 2.0
      deadline.check!
    end
    transport = double("undispatched")
    expect(transport).not_to receive(:call)
    core = ConnectRPC::Client.new(base_url: "http://example.invalid", binding: binding, transport: transport,
                                  instrumentation: bus)
    expect { core.call(method, "hello") }.to raise_error(ConnectRPC::Error) { |error|
      expect(error.code).to eq("deadline_exceeded")
    }
    expect(outcomes.one?).to be(true)
    expect(outcomes.first.request_bytes).to be_nil
  end

  it "retains context response metadata and trailers when a GET handler raises" do
    handler = lambda do |_, context|
      context.response_metadata["x-before"] = ["yes"]
      context.response_trailers["x-tail"] = ["before"]
      raise ConnectRPC::Error.new(:permission_denied, "safe", metadata: { "vary" => ["Origin"] })
    end
    target = ConnectRPC::Server.new(services: [service], binding: binding, handlers: { method.path => handler })
    result = get("encoding=proto&message=", target: target)
    expect(result.status).to eq(403)
    expect(result.headers).to include("x-before" => ["yes"], "trailer-x-tail" => ["before"],
                                      "vary" => %w[Origin Accept-Encoding])
  end

  it "measures query payload bytes before inflation and leaves rejected/unknown measurements nil" do
    records = []
    observer = Object.new
    observer.define_singleton_method(:processing_finished) { |outcome| records << outcome }
    subscriber = Object.new
    subscriber.define_singleton_method(:start) { |_call| observer }
    bus = ConnectRPC::Instrumentation.new(subscribers: [subscriber])
    target = ConnectRPC::Server.new(services: [service], binding: binding, handlers: { method.path => ->(x, _) { x } },
                                    encoding: :gzip, instrumentation: bus)
    compressed = ConnectRPC::Compression.encode("hello", limit: 1000)
    encoded = [compressed].pack("m0").tr("+/", "-_").delete("=")
    get("encoding=proto&compression=gzip&base64=1&message=#{encoded}", target: target)
    expect(records.last.request_bytes).to eq(compressed.bytesize)
    get("encoding=proto&message=", target: target)
    expect(records.last.request_bytes).to eq(0)
    get("encoding=proto&message=%FF", target: target)
    expect(records.last.request_bytes).to be_nil
    get("", target: target)
    expect(records.last.request_bytes).to be_nil
    records.clear
    transport = lambda do |request|
      target.call(path: request.url.path, query: request.url.query, http_method: request.http_method,
                  input: nil, headers: request.headers)
    end
    client = ConnectRPC::Client.new(base_url: "http://example.invalid", binding: binding, transport: transport,
                                    use_get: true, encoding: :gzip, instrumentation: bus)
    expect(client.call(method, "hello").message).to eq("hello")
    expect(records.size).to eq(2)
    expect(records.map(&:request_bytes)).to eq([compressed.bytesize, compressed.bytesize])
    expect(records.map(&:code)).to eq(%w[OK OK])
  end
end

RSpec.describe "GET serialization repeatability" do
  it "preserves official map serialization and repeatable gzip for the same object within this process" do
    require "connectrpc/bindings/google_protobuf"
    require "google/protobuf/struct_pb"
    binding = ConnectRPC::Bindings::GoogleProtobuf.new
    type = Google::Protobuf::Struct
    message = type.new(fields: { "z" => Google::Protobuf::Value.new(string_value: "last"),
                                 "a" => Google::Protobuf::Value.new(number_value: 1) })
    %i[proto json].each do |codec|
      encoded = 10.times.map { binding.encode(message, type: type, codec: codec) }
      expect(encoded.uniq.size).to eq(1)
      compressed = encoded.map { |bytes| ConnectRPC::Compression.encode(bytes, limit: 1000) }
      expect(compressed.uniq.size).to eq(1)
      expect(binding.decode(encoded.first, type: type, codec: codec)).to eq(message)
    end
  end
end
