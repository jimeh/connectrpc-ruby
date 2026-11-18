# frozen_string_literal: true

require "rack/mock"
require "stringio"

RSpec.describe "Semantic conventions 1.45.0 attributes" do
  let(:message) { AsyncHelpers::MESSAGE.new(name: "semconv") }

  def in_process(bus, **)
    target = server(instrumentation: bus, **)
    client(ConnectRPC::Testing::Transport.new(server: target), "https://in-process.invalid", instrumentation: bus)
  end

  def frame(bytes, flags = 0) = [flags, bytes.bytesize].pack("CN") + bytes

  # Native gRPC dispatch with in-memory input and output.
  def native_grpc(target, path)
    output = Object.new
    output.define_singleton_method(:write) { |*, **| nil }
    output.define_singleton_method(:finish) { |**| nil }
    output.define_singleton_method(:abort) { nil }
    body = StringIO.new(frame(message.to_proto))
    body.define_singleton_method(:read) { |length, **| super(length) }
    target.call_grpc(path: path, http_method: "POST", headers: { "content-type" => ["application/grpc"] },
                     input: body, output: output, publish: ->(_) {}, http2: true)
  end

  def rack_env(path, body, media, host: "api.example:8443")
    Rack::MockRequest.env_for(path, method: "POST", "CONTENT_TYPE" => media, "HTTP_HOST" => host, input: body)
  end

  it "records Connect unary request headers, response headers and trailers on both roles" do
    provider, exporter, bus = tracing(request_metadata: ["x-request"], response_metadata: %w[x-repeat x-echo],
                                      response_trailers: ["x-tail"])
    reply = in_process(bus).echo(message, metadata: { "x-request" => "sent" })
    expect(reply.trailers["x-tail"]).to eq(%w[three four])
    spans = rpc_spans(exporter)
    expect(spans.map(&:kind)).to contain_exactly(:client, :server)
    expect_no_retired_attributes(spans)
    spans.each do |span|
      expect(span.attributes).to include("rpc.status_code" => "OK", "rpc.request.header.x-request" => ["sent"],
                                         "rpc.response.header.x-repeat" => %w[one two],
                                         "rpc.response.header.x-echo" => ["sent"],
                                         "rpc.response.trailer.x-tail" => %w[three four])
      expect(span.attributes.keys).not_to include("rpc.response.header.trailer-x-tail")
    end
    server_span = spans.find { |span| span.kind == :server }
    # The in-process transport sends no host, so the server has no authority to record.
    expect(server_span.attributes.keys).not_to include("server.address", "server.port")
  ensure
    provider&.shutdown
  end

  it "records Connect error metadata and trailers on both roles" do
    provider, exporter, bus = tracing(response_metadata: ["x-error"], response_trailers: ["x-end"])
    expect { in_process(bus).fail(message) }.to raise_error(ConnectRPC::Error)
    spans = rpc_spans(exporter)
    expect(spans.size).to eq(2)
    spans.each do |span|
      expect(span.attributes).to include("rpc.status_code" => "invalid_argument",
                                         "rpc.response.header.x-error" => %w[a b],
                                         "rpc.response.trailer.x-end" => %w[c d])
    end
  ensure
    provider&.shutdown
  end

  it "records native gRPC error trailers, including error metadata after a committed head" do
    provider, exporter, bus = tracing(response_trailers: %w[x-tail x-meta x-err])
    rpc = ConnectRPC::ServerStreamMethod.new(service: "trace.Semconv", name: "Watch",
                                             request_type: AsyncHelpers::MESSAGE, response_type: AsyncHelpers::MESSAGE)
    handler = lambda do |request, stream, context|
      context.response_trailers["x-tail"] = ["t"]
      stream.send_message(request)
      raise ConnectRPC::Error.new(:not_found, "gone", metadata: { "x-meta" => "m" }, trailers: { "x-err" => "e" })
    end
    target = ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: rpc.service, rpcs: [rpc])],
                                    binding: ConnectRPC::Bindings::GoogleProtobuf.new, protocol: :grpc,
                                    handlers: { rpc.path => handler }, instrumentation: bus)
    native_grpc(target, rpc.path)
    span = rpc_spans(exporter).first
    expect(span.attributes).to include("rpc.status_code" => "NOT_FOUND", "rpc.response.trailer.x-tail" => ["t"],
                                       "rpc.response.trailer.x-meta" => ["m"], "rpc.response.trailer.x-err" => ["e"])
  ensure
    provider&.shutdown
  end

  describe "Rack request authority" do
    { "api.example:8443" => ["api.example", 8443], "api.example" => ["api.example", nil],
      "[2001:db8::1]:50051" => ["2001:db8::1", 50_051], "bad host" => [nil, nil] }.each do |host, (address, port)|
      it "records #{host.inspect} on Connect and gRPC-Web server spans" do
        provider, exporter, bus = tracing(response_metadata: ["x-repeat"], response_trailers: ["x-tail"])
        app = ConnectRPC::Rack.new(server: server(instrumentation: bus, protocol: %i[connect grpc_web]))
        proto = message.to_proto
        [[proto, "application/proto"], [frame(proto), "application/grpc-web+proto"]].each do |body, media|
          status, _headers, response = app.call(rack_env(AsyncHelpers::ECHO::ECHO.path, body, media, host: host))
          expect(status).to eq(200)
          response.close
        end
        spans = rpc_spans(exporter)
        expect(spans.map { |span| span.attributes["rpc.system.name"] }).to eq(%w[connectrpc grpc])
        expect_no_retired_attributes(spans)
        spans.each do |span|
          expect(span.kind).to eq(:server)
          expect(span.attributes["server.address"]).to eq(address)
          expect(span.attributes["server.port"]).to eq(port)
          expect(span.attributes).to include("rpc.status_code" => "OK", "rpc.response.header.x-repeat" => %w[one two],
                                             "rpc.response.trailer.x-tail" => %w[three four])
        end
      ensure
        provider&.shutdown
      end
    end
  end

  describe "native Async authority" do
    def native_request(authority, headers = {})
      wire = frame(message.to_proto)
      Protocol::HTTP::Request["POST", AsyncHelpers::ECHO::ECHO.path,
                              { "content-type" => "application/grpc-web+proto" }.merge(headers),
                              Protocol::HTTP::Body::Buffered.wrap(wire), authority: authority, scheme: "https"]
    end

    it "prefers the authority over an ordinary host header with and without a host lifecycle" do
      provider, exporter, bus = tracing
      application = ConnectRPC::Async::Application.new(server: server(instrumentation: bus, protocol: :grpc_web))
      request = native_request("rpc.example:9443", "host" => "ignored.example")
      lifecycle = application.start_call(request)
      application.call(request, lifecycle: lifecycle)
      lifecycle.finish(delivery: :completed)
      application.call(native_request("rpc.example", "host" => "ignored.example"))
      spans = rpc_spans(exporter)
      expect(spans.map { |span| span.attributes["server.address"] }).to eq(%w[rpc.example rpc.example])
      expect(spans.map { |span| span.attributes["server.port"] }).to eq([9443, nil])
      expect(spans.map { |span| span.attributes["rpc.status_code"] }).to eq(%w[OK OK])
    ensure
      provider&.shutdown
    end
  end

  describe "binary metadata" do
    let(:padded) { ConnectRPC::Metadata.encode_binary("\x00\x01".b) }
    let(:oversized) { ConnectRPC::Metadata.encode_binary("z" * 200) }
    # Unpadded, padded and coalesced inputs are recorded as standard padded base64;
    # malformed and oversized values are omitted.
    let(:values) { [padded, "AAE=", "AQ, Ag", "!!!", "AQ=", oversized] }
    let(:recorded) { %w[AAE= AAE= AQ== Ag==] }

    def binary_handler(values)
      lambda do |request, context|
        context.response_metadata["x-data-bin"] = values
        context.response_trailers["x-data-bin"] = values
        request
      end
    end

    it "records Connect headers and trailers as padded base64 on both roles" do
      provider, exporter, bus = tracing(request_metadata: ["x-data-bin"], response_metadata: ["x-data-bin"],
                                        response_trailers: ["x-data-bin"])
      metadata = ConnectRPC::Metadata.new.add_binary("x-data-bin", "\x00\x01".b)
      in_process(bus, echo: binary_handler(values)).echo(message, metadata: metadata)
      spans = rpc_spans(exporter)
      expect(spans.size).to eq(2)
      spans.each do |span|
        expect(span.attributes).to include("rpc.request.header.x-data-bin" => ["AAE="],
                                           "rpc.response.header.x-data-bin" => recorded,
                                           "rpc.response.trailer.x-data-bin" => recorded)
      end
    ensure
      provider&.shutdown
    end

    it "records gRPC headers and trailers as padded base64 and keeps the eight-entry limit" do
      provider, exporter, bus = tracing(response_metadata: ["x-data-bin"], response_trailers: ["x-data-bin"])
      many = Array.new(10) { |index| ConnectRPC::Metadata.encode_binary(index.chr) }
      native_grpc(server(instrumentation: bus, protocol: :grpc, echo: binary_handler([values.join(","), *many])),
                  AsyncHelpers::ECHO::ECHO.path)
      span = rpc_spans(exporter).first
      expected = recorded + many.first(4).map { |value| "#{value}==" }
      expect(span.attributes).to include("rpc.system.name" => "grpc", "rpc.response.header.x-data-bin" => expected,
                                         "rpc.response.trailer.x-data-bin" => expected)
    ensure
      provider&.shutdown
    end
  end

  it "validates the trailer allowlist like the metadata allowlists" do
    expect(ConnectRPC::OTel::Subscriber.new(response_trailers: %w[X-Tail x-data-bin]))
      .to be_a(ConnectRPC::OTel::Subscriber)
    [["trailer-x-tail"], ["grpc-status"], ["*"], ["bad name"], [:symbol], "x-tail", Array.new(33) { |i| "x-#{i}" },
     ["a" * 129]].each do |names|
      expect { ConnectRPC::OTel::Subscriber.new(response_trailers: names) }.to raise_error(ArgumentError)
    end
  end

  it "records outcome attributes at finish when an earlier subscriber escapes the processing callbacks" do
    fatal = Class.new(Exception) # rubocop:disable Lint/InheritException -- escape the isolated StandardError boundary
    escaping = Object.new
    escaping.define_singleton_method(:start) do |_|
      Object.new.tap { |observer| observer.define_singleton_method(:processing_finished) { |_| raise fatal } }
    end
    provider, exporter, bus = tracing(response_metadata: ["x-repeat"], response_trailers: ["x-tail"],
                                      before: [escaping])
    app = ConnectRPC::Rack.new(server: server(instrumentation: bus))
    expect { app.call(rack_env(AsyncHelpers::ECHO::ECHO.path, message.to_proto, "application/proto")) }
      .to raise_error(fatal)
    span = rpc_spans(exporter).first
    expect(span.attributes).to include("rpc.status_code" => "OK", "http.response.status_code" => 200,
                                       "rpc.response.header.x-repeat" => %w[one two],
                                       "rpc.response.trailer.x-tail" => %w[three four],
                                       "connectrpc.delivery" => "aborted", "error.type" => "delivery_aborted",
                                       "server.address" => "api.example", "server.port" => 8443)
    expect(span.attributes).to include("connectrpc.processing.duration", "connectrpc.request.wire_bytes",
                                       "connectrpc.response.wire_bytes")
  ensure
    provider&.shutdown
  end
end
