# frozen_string_literal: true

require "stringio"

RSpec.describe "Native gRPC protocol" do
  let(:grpc) { ConnectRPC.const_get(:GRPCProtocol) }

  def expect_code(code, &)
    expect(&).to raise_error(ConnectRPC::Error) { |error|
      expect(error.code).to eq(code.to_s)
    }
  end

  it "parses numeric status with arbitrarily many bounded leading zeros without losing the uint32 boundary" do
    %w[0 00 0000].each { |value| expect(grpc.status_number(value)).to eq(0) }
    expect(grpc.status_number("#{'0' * 60_000}1")).to eq(1)
    expect(grpc.status_number("0004294967295")).to eq(4_294_967_295)
    ["4294967296", "9" * 60_000, "", "-1", "+1", " 1", "1 ", "1x"].each do |value|
      expect(grpc.status_number(value)).to be_nil
    end
  end

  it "preserves invalid percent escapes and scrubs invalid UTF8 while round tripping emitted messages" do
    message = "missing % / café\n"
    expect(grpc.decode_message(grpc.encode_message(message))).to eq(message)
    expect(grpc.decode_message("bad%xy%2%FF")).to eq("bad%xy%2�")
  end

  it "retains deeply immutable head fields without changing the legacy StreamHead record" do
    values = { "x" => [+"one"] }
    head = ConnectRPC::GRPCHead.new(status: 200, headers: values, end_stream: true)
    values["x"].first.replace("two")
    expect(head.headers).to eq("x" => ["one"])
    expect { head.headers["x"].first.replace("three") }.to raise_error(FrozenError)
    expect(ConnectRPC::StreamHead.members).to eq(%i[status headers])
  end

  it "counts every occurrence including custom fields in the complete terminal field block" do
    expect(grpc.field_size({ "x" => %w[a bc] })).to eq(69)
    expect_code(:resource_exhausted) { grpc.check_fields!({ "x" => %w[a bc] }, 68) }
  end

  it "rejects duplicate statuses independently of spelling and success details" do
    expect_code(:internal) { grpc.parse_status({ "grpc-status" => %w[0 00] }, codec: nil) }
    expect_code(:internal) do
      grpc.parse_status({ "grpc-status" => ["00"], "grpc-status-details-bin" => [""] }, codec: nil)
    end
  end

  it "uses status over HTTP fallback and preserves normal metadata on remote failure" do
    error, trailers = grpc.parse_status({ "grpc-status" => ["01"], "grpc-message" => ["canceled%21"],
                                          "x-tail" => %w[one two] }, codec: nil, http_status: 503)
    expect(error.code).to eq("canceled")
    expect(error.message).to eq("canceled!")
    expect(trailers["x-tail"]).to eq(%w[one two])
    expect(grpc.parse_status({ "grpc-status" => ["0000"] }, codec: nil, http_status: 503).first).to be_nil
    expect(grpc.parse_status({}, codec: nil, http_status: 503).first.code).to eq("unavailable")
  end

  it "accepts all six timeout units and zero through a private expired factory" do
    %w[H M S m u n].each do |unit|
      expect(grpc.request_deadline({ "grpc-timeout" => ["1#{unit}"] })).to be_a(ConnectRPC::Deadline)
    end
    expect(grpc.request_deadline({ "grpc-timeout" => ["0S"] })).to be_expired
    expect(grpc.request_deadline({})).to be_nil
    ["", "1s", "123456789S", "-1S"].each do |value|
      expect_code(:internal) { grpc.request_deadline({ "grpc-timeout" => [value] }) }
    end
    expect { ConnectRPC::Deadline.new(0) }.to raise_error(ArgumentError)
  end
end

RSpec.describe "gRPC public callers and status codec" do
  let(:grpc) { ConnectRPC.const_get(:GRPCProtocol) }
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

  def rpc(kind = ConnectRPC::Method)
    kind.new(service: "test.Tokens", name: "Call", request_type: :request_token, response_type: :response_token)
  end

  def client(body: "", status: "0", trailers_only: false, extra_fields: {}, http_status: 200, kind: ConnectRPC::Method)
    source = StringIO.new(body)
    fields = { "grpc-status" => [status] }.merge(extra_fields)
    head = ConnectRPC::GRPCHead.new(
      status: http_status, end_stream: trailers_only,
      headers: { "content-type" => ["application/grpc"] }.merge(trailers_only ? fields : {})
    )
    exchange = Object.new
    exchange.define_singleton_method(:response_head) { |**| head }
    exchange.define_singleton_method(:response_trailers) { fields }
    exchange.define_singleton_method(:read) { |length, **| source.read([length, 1].min) }
    exchange.define_singleton_method(:write) { |_| nil }
    exchange.define_singleton_method(:close_send) { nil }
    exchange.define_singleton_method(:finish) { |**| nil }
    exchange.define_singleton_method(:abort) { nil }
    transport = Object.new
    transport.define_singleton_method(:grpc_capabilities) { %i[unary server_stream client_stream bidi_stream] }
    transport.define_singleton_method(:open_grpc_stream) { |_, &work| work.call(exchange) }
    core = ConnectRPC::Client.new(base_url: "https://example.invalid", protocol: :grpc, binding: binding,
                                  status_codec: status_codec, transport: transport)
    [core, rpc(kind), exchange, transport]
  end

  def envelope(bytes, flags = 0) = [flags, bytes.bytesize].pack("CN") + bytes

  %w[0 00 0000].each do |status|
    [false, true].each do |trailers_only|
      it "validates #{status} with trailers-only=#{trailers_only} through a gRPC-only transport" do
        core, method, = client(body: trailers_only ? "" : envelope(""), status: status, trailers_only: trailers_only)
        if trailers_only
          expect { core.call(method, "q") }.to raise_error(ConnectRPC::Error) { |error|
            expect(error.code).to eq("unimplemented")
          }
        else
          expect(core.call(method, "q").message).to eq("")
        end
      end
    end
  end

  [false, true].each do |trailers_only|
    it "receives leading-zero remote error ahead of missing success cardinality for #{trailers_only}" do
      core, method, = client(status: "01", trailers_only: trailers_only)
      expect { core.call(method, "q") }.to raise_error(ConnectRPC::Error) { |error|
        expect(error.code).to eq("canceled")
      }
    end
  end

  [ConnectRPC::Method, ConnectRPC::ClientStreamMethod].each do |kind|
    [0, 2].each do |count|
      it "does not bypass #{kind} cardinality with zero status after #{count} messages" do
        core, method, = client(body: envelope("reply") * count, status: "0000", kind: kind)
        expect do
          kind == ConnectRPC::Method ? core.call(method, "q") : core.client_stream(method) { nil }
        end.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("unimplemented") }
      end
    end
  end

  it "preserves rich message and opaque Any bytes without global Status registration" do
    error = ConnectRPC::Error.new(:not_found, "rich % café", details: [
                                    ConnectRPC::ErrorDetail.new(type: "unknown.Opaque", value: "\x00\xFFunknown".b)
                                  ])
    bytes = status_codec.encode_status(error)
    decoded = status_codec.decode_status(bytes, code: 5)
    expect(decoded.message).to eq(error.message)
    expect(decoded.details.map(&:to_h)).to eq(error.details.map(&:to_h))
    expect(Google::Protobuf::DescriptorPool.generated_pool.lookup("google.rpc.Status")).to be_nil
    expect(Google.const_defined?(:Rpc, false)).to be(false)
    expect { status_codec.decode_status(bytes, code: 42) }.to raise_error(ConnectRPC::CodecError)
  end

  it "compares rich unknown status against the raw leading-zero numeric code before mapping unknown" do
    descriptor = Google::Protobuf::FileDescriptorSet.decode(File.binread("lib/connectrpc/bindings/descriptors/status.bin"))
    pool = Google::Protobuf::DescriptorPool.new
    descriptor.file.each { |file| pool.add_serialized_file(Google::Protobuf::FileDescriptorProto.encode(file)) }
    status_type = pool.lookup("google.rpc.Status").msgclass
    wire = status_type.encode(status_type.new(code: 42, message: "future rich code"))
    fields = { "grpc-status-details-bin" => [ConnectRPC::Metadata.encode_binary(wire)], "grpc-message" => ["other"] }
    core, method, = client(status: "00042", extra_fields: fields)
    expect { core.call(method, "q") }.to raise_error(ConnectRPC::Error) { |error|
      expect(error.code).to eq("unknown")
      expect(error.message).to eq("future rich code")
    }
    core, method, = client(status: "00043", extra_fields: fields)
    expect { core.call(method, "q") }.to raise_error(ConnectRPC::Error) { |error|
      expect(error.code).to eq("internal")
      expect(error.message).not_to include("future rich code")
    }
  end

  it "recovers non-OK terminal status after multiple skipped CS messages without binding decode" do
    core, method, exchange, = client(body: envelope("discard") * 2, status: "5", kind: ConnectRPC::ClientStreamMethod)
    exchange.define_singleton_method(:write) { |_| raise IOError, "incidental send I/O" }
    expect(binding).not_to receive(:decode)
    expect do
      core.client_stream(method) { |writer| writer.send_message("q") }
    end.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("not_found") }
  end

  it "does not replace an incidental send failure with HTTP fallback from missing remote status" do
    core, method, exchange, = client(kind: ConnectRPC::BidiStreamMethod)
    exchange.define_singleton_method(:write) { |_| raise IOError, "incidental send I/O" }
    exchange.define_singleton_method(:response_trailers) { {} }
    expect do
      core.bidi_stream(method) { |duplex| duplex.send_message("q") }
    end.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("unavailable") }
  end

  it "accepts a complete long leading-zero status block and authoritative OK on non-200 HTTP" do
    core, method, = client(body: envelope("reply"), status: "0" * 60_000, http_status: 503)
    expect(core.call(method, "q").message).to eq("reply")
  end

  it "keeps malformed, duplicate and mismatching rich status failures safe" do
    ["", "!!!!", "AA,AA", ConnectRPC::Metadata.encode_binary("malformed protobuf")].each do |value|
      core, method, = client(status: "5", extra_fields: { "grpc-status-details-bin" => [value] })
      expect { core.call(method, "q") }.to raise_error(ConnectRPC::Error) { |error|
        expect(error.code).to eq("internal")
        expect(error.message).not_to include("malformed protobuf")
        expect(error.cause).to be_nil
      }
    end
    core, method, = client(status: "5", extra_fields: { "grpc-message" => %w[one two] })
    expect { core.call(method, "q") }.to raise_error(ConnectRPC::Error) { |error|
      expect(error.code).to eq("internal")
    }
  end

  it "enforces independent raw status and complete fieldblock limits before protobuf parsing" do
    details = [ConnectRPC::ErrorDetail.new(type: "opaque.Type", value: "x" * 100)]
    raw = status_codec.encode_status(ConnectRPC::Error.new(:not_found, "rich", details: details))
    fields = { "grpc-status" => ["5"], "grpc-status-details-bin" => [ConnectRPC::Metadata.encode_binary(raw)] }
    expect do
      grpc.parse_status(fields, codec: status_codec, wire_limit: 1000, limit: raw.bytesize - 1)
    end.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("resource_exhausted") }
    expect do
      grpc.parse_status(fields, codec: status_codec, wire_limit: grpc.field_size(fields) - 1, limit: 1000)
    end.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("resource_exhausted") }
    emitted, replacement = grpc.terminal(ConnectRPC::Error.new(:internal, "x" * 1000), ConnectRPC::Metadata.new,
                                         codec: status_codec, wire_limit: 44, limit: 1000)
    expect(emitted).to eq("grpc-status" => ["8"])
    expect(replacement.code).to eq("resource_exhausted")
    expect do
      grpc.terminal(nil, ConnectRPC::Metadata.new, codec: status_codec, wire_limit: 43, limit: 1000)
    end.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("resource_exhausted") }
  end

  it "has no Connect one-second tail timer for gRPC completion" do
    core, method, exchange, = client(body: envelope("reply"))
    clock = 0.0
    allow_any_instance_of(ConnectRPC.const_get(:Streaming)::Session).to receive(:now) { clock }
    exchange.define_singleton_method(:finish) do |**|
      clock = 2.0
      nil
    end
    expect(core.call(method, "q", timeout: nil).message).to eq("reply")
  end

  %i[bytes absolute].each do |bound|
    it "bounds invalid-media recovery by the same #{bound} budget as framed recovery" do
      core, method, exchange, = client(kind: ConnectRPC::BidiStreamMethod)
      raw = StringIO.new("x" * 400_000)
      reads = []
      clock = 0.0
      allow_any_instance_of(ConnectRPC.const_get(:Streaming)::Session).to receive(:now) { clock }
      exchange.define_singleton_method(:response_head) do |**|
        ConnectRPC::GRPCHead.new(status: 200, end_stream: false, headers: { "content-type" => ["image/jpeg"] })
      end
      exchange.define_singleton_method(:write) { |_| raise IOError, "incidental write failure" }
      exchange.define_singleton_method(:read) do |length, timeout: nil|
        clock += 0.03 if bound == :absolute
        raw.read(length).tap { |chunk| reads << [chunk.bytesize, timeout] if chunk }
      end
      expect(binding).not_to receive(:decode)
      expect do
        core.bidi_stream(method, timeout: nil) { |duplex| duplex.send_message("q") }
      end.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("unavailable") }
      expect(reads.sum(&:first)).to be <= 262_144
      expect(reads.size).to be <= 4 if bound == :absolute
    end
  end

  it "uses a unary custom invoker without handler entries and passes ordinary Context" do
    method = rpc
    contexts = []
    target = ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: method.service, methods: [method])],
                                    binding: binding, status_codec: status_codec, protocol: :grpc,
                                    invoker: lambda { |declaration, message, context|
                                      expect(declaration).to equal(method)
                                      contexts << context
                                      message
                                    })
    output = Object.new
    chunks = []
    output.define_singleton_method(:write) do |bytes, **|
      chunks << bytes
      nil
    end
    output.define_singleton_method(:finish) do |headers:|
      chunks << headers
      nil
    end
    output.define_singleton_method(:abort) { nil }
    incoming = StringIO.new(envelope("request"))
    incoming.define_singleton_method(:read) { |n, **| super(n) }
    published = []
    result = target.call_grpc(path: method.path, http_method: "POST",
                              headers: { "content-type" => ["application/grpc"] },
                              input: incoming, output: output, publish: ->(head) { published << head }, http2: true)
    expect(result).to be_nil
    expect(contexts.one?).to be(true)
    expect(contexts.first).to be_instance_of(ConnectRPC::Context)
    expect(chunks.last).to eq("grpc-status" => ["0"])
    expect(published.size).to eq(1)
    expect(published.first.end_stream).to be(false)
  end
end

RSpec.describe "gRPC bounded framing and negotiation" do
  let(:grpc) { ConnectRPC.const_get(:GRPCProtocol) }

  def framing(bytes, **)
    source = StringIO.new(bytes)
    source.define_singleton_method(:read) { |length, **| super([length, 1].min) }
    grpc::Framing.new(input: source, request: true, wire_limit: 1024, limit: 1024, **)
  end

  def frame(bytes, flag = 0) = [flag, bytes.bytesize].pack("CN") + bytes

  it "retains empty and fragmented adjacent messages without a Connect terminal" do
    reader = framing(frame("") + frame("two"))
    expect(reader.read_frame).to eq([0, ""])
    expect(reader.read_frame).to eq([0, "two"])
    expect(reader.read_frame).to be_nil
  end

  [2, 3, 4, 128, 255].each do |flag|
    it "rejects reserved flag #{flag} as internal in the request direction" do
      expect { framing(frame("", flag)).read_frame }.to raise_error(ConnectRPC::Error) { |e|
        expect(e.code).to eq("internal")
      }
    end
  end

  ["\0", "\0\0\0\0", "#{[0, 9].pack('CN')}short"].each do |bytes|
    it "rejects truncation of #{bytes.bytesize} request bytes as internal" do
      expect { framing(bytes).read_frame }.to raise_error(ConnectRPC::Error) { |e|
        expect(e.code).to eq("internal")
      }
    end
  end

  it "checks uint32 wire lengths before requesting their payload" do
    source = Object.new
    seen = []
    source.define_singleton_method(:read) do |length, **|
      seen << length
      raise "payload read" unless seen.size == 1

      [0, 0xffff_ffff].pack("CN")
    end
    reader = grpc::Framing.new(input: source, request: true, wire_limit: 10, limit: 10)
    expect { reader.read_frame }.to raise_error(ConnectRPC::Error) { |e|
      expect(e.code).to eq("resource_exhausted")
    }
    expect(seen).to eq([5])
  end

  it "requires negotiated per-message gzip and validates CRC and decoded bounds" do
    gzip = ConnectRPC::Compression.encode("x" * 2000, limit: 4096)
    expect { framing(frame(gzip, 1)).read_frame }.to raise_error(ConnectRPC::Error) { |e|
      expect(e.code).to eq("internal")
    }
    expect { framing(frame(gzip, 1), encoding: :gzip).read_frame }.to raise_error(ConnectRPC::Error) { |e|
      expect(e.code).to eq("resource_exhausted")
    }
    corrupt = ConnectRPC::Compression.encode("small", limit: 4096)
    corrupt.setbyte(-8, corrupt.getbyte(-8) ^ 255)
    expect { framing(frame(corrupt, 1), encoding: :gzip).read_frame }.to raise_error(ConnectRPC::Error) { |e|
      expect(e.code).to eq("internal")
    }
    compressed = ConnectRPC::Compression.encode("small", limit: 4096)
    reader = framing(frame(compressed, 1) + frame(compressed, 1), encoding: :gzip)
    2.times { expect(reader.read_frame).to eq([1, "small"]) }
    expect(reader.read_frame).to be_nil
  end

  it "rounds timeout propagation upward, handles huge finite budgets and keeps nil unbounded only for Connect" do
    clock = 0.0
    deadline = ConnectRPC::Deadline.new(0.000000001, clock: -> { clock })
    expect(grpc.timeout(deadline)).to eq("1n")
    expect(grpc.timeout(ConnectRPC::Deadline.new(1e100))).to eq("99999999H")
    strategy = grpc::Strategy.new(codec: nil, wire_limit: 100, limit: 100)
    expect(strategy.wait(nil)).to eq(9_999_999.999)
    expect(strategy.wait(1)).to eq(1)
    expect(ConnectRPC.const_get(:Streaming)::ConnectStrategy.new.wait(nil)).to be_nil
    clock = 1.0
    expect { grpc.timeout(deadline) }.to raise_error(ConnectRPC::Error) { |e|
      expect(e.code).to eq("deadline_exceeded")
    }
  end

  it "distinguishes malformed singleton encoding from valid unrecognized request coding" do
    ["br", "GZIP", "custom+coding"].each do |value|
      expect { grpc.encoding({ "grpc-encoding" => [value] }, request: true) }.to raise_error(ConnectRPC::Error) { |e|
        expect(e.code).to eq("unimplemented")
      }
    end
    [["gzip", "gzip"], [""], ["gzip,identity"], ["gzip bad"]].each do |values|
      expect { grpc.encoding({ "grpc-encoding" => values }, request: true) }.to raise_error(ConnectRPC::Error) { |e|
        expect(e.code).to eq("internal")
      }
    end
    expect(grpc.response_encoding({ "grpc-accept-encoding" => ["unknown", "gzip,identity"] }, :gzip)).to eq(:gzip)
  end
end

RSpec.describe "gRPC complete response publication budget" do
  [-1, 0, 1].each do |delta|
    it "includes :status before publishing a head at limit #{delta}" do
      binding = Object.new
      binding.define_singleton_method(:encode) { |value, **| value }
      binding.define_singleton_method(:decode) { |value, **| value }
      binding.define_singleton_method(:encode_status) { |_| "" }
      binding.define_singleton_method(:decode_status) { |*, **| ConnectRPC::Error.new(:unknown) }
      method = ConnectRPC::Method.new(service: "test.Headers", name: "Call", request_type: String,
                                      response_type: String)
      base = { ":status" => ["200"], "content-type" => ["application/grpc+proto"],
               "grpc-accept-encoding" => ["identity"] }
      size = ConnectRPC.const_get(:GRPCProtocol).field_size(base)
      target = ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: method.service, methods: [method])],
                                      binding: binding, protocol: :grpc,
                                      handlers: { method.path => lambda { |message, context|
                                        context.response_metadata["x-pad"] = ["x" * (65_536 + delta - size - 37)]
                                        message
                                      } })
      input = StringIO.new("#{[0, 1].pack('CN')}q")
      input.define_singleton_method(:read) { |length, **| super(length) }
      output = Object.new
      output.define_singleton_method(:write) { |*, **| nil }
      output.define_singleton_method(:finish) { |**| nil }
      output.define_singleton_method(:abort) { nil }
      published = []
      work = lambda {
        target.call_grpc(path: method.path, http_method: "POST", headers: { "content-type" => ["application/grpc"] },
                         input: input, output: output, publish: ->(head) { published << head }, http2: true)
      }
      if delta.positive?
        expect(&work).to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("resource_exhausted") }
        expect(published).to be_empty
      else
        work.call
        expect(published.size).to eq(1)
      end
    end
  end
end
