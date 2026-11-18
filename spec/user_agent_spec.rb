# frozen_string_literal: true

require "connectrpc/transports/net_http"
require_relative "support/socket_peer"

RSpec.describe "Client User-Agent" do
  include SocketPeer

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
  let(:unary) do
    ConnectRPC::Method.new(service: "test.Agent", name: "Get", request_type: String, response_type: String,
                           idempotency: :no_side_effects)
  end
  let(:stream) do
    ConnectRPC::ServerStreamMethod.new(service: "test.Agent", name: "Watch", request_type: String,
                                       response_type: String)
  end
  let(:connect_agent) { "connectrpc-ruby/#{ConnectRPC::VERSION} (ruby #{RUBY_VERSION})" }
  let(:grpc_agent) { "grpc-ruby-connectrpc/#{ConnectRPC::VERSION} (ruby #{RUBY_VERSION})" }

  def unary_headers(metadata: {}, interceptors: [], **)
    headers = nil
    transport = lambda do |request|
      headers = request.headers
      ConnectRPC::TransportResponse.new(status: 200, headers: { "content-type" => ["application/proto"] }, body: "ok")
    end
    ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding, transport: transport,
                           interceptors: interceptors, **).call(unary, "hi", metadata: metadata)
    headers
  end

  # Captures the opened stream's headers, then fails it before any exchange.
  def stream_headers(protocol, method, metadata: {})
    seen = []
    open = lambda do |request, &|
      seen << request.headers
      raise ConnectRPC::Error.new(:unavailable, "captured")
    end
    transport = Object.new
    if protocol == :grpc
      transport.define_singleton_method(:grpc_capabilities) { %i[unary server_stream client_stream bidi_stream] }
      transport.define_singleton_method(:open_grpc_stream, &open)
    else
      transport.define_singleton_method(:streaming_capabilities) { %i[server_stream client_stream bidi_stream] }
      transport.define_singleton_method(:open_stream, &open)
    end
    client = ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding, protocol: protocol,
                                    status_codec: (status_codec if protocol == :grpc), transport: transport)
    expect do
      if method == :unary
        client.call(unary, "hi", metadata: metadata)
      else
        client.server_stream(stream, "hi", metadata: metadata) { nil }
      end
    end.to raise_error(ConnectRPC::Error, "captured")
    seen.fetch(0)
  end

  it "defaults Connect unary POST and GET" do
    expect(ConnectRPC::Protocol::USER_AGENT).to eq(connect_agent).and be_frozen
    expect(unary_headers["user-agent"]).to eq([connect_agent])
    expect(unary_headers(use_get: true)["user-agent"]).to eq([connect_agent])
  end

  it "defaults Connect streaming, gRPC unary and gRPC streaming" do
    expect(stream_headers(:connect, :stream)["user-agent"]).to eq([connect_agent])
    expect(stream_headers(:grpc, :unary)["user-agent"]).to eq([grpc_agent])
    expect(stream_headers(:grpc, :stream)["user-agent"]).to eq([grpc_agent])
  end

  [["custom/1"], [""], []].each do |value|
    it "keeps call metadata user-agent #{value.inspect} on every client path" do
      metadata = { "User-Agent" => value }
      expect(unary_headers(metadata: metadata)["user-agent"]).to eq(value)
      expect(unary_headers(metadata: metadata, use_get: true)["user-agent"]).to eq(value)
      expect(stream_headers(:connect, :stream, metadata: metadata)["user-agent"]).to eq(value)
      expect(stream_headers(:grpc, :unary, metadata: metadata)["user-agent"]).to eq(value)
      expect(stream_headers(:grpc, :stream, metadata: metadata)["user-agent"]).to eq(value)
    end
  end

  it "keeps an interceptor's user-agent, and interceptors do not see the default" do
    seen = []
    interceptor = Object.new
    interceptor.define_singleton_method(:client_unary) do |call, &continuation|
      seen << call.request_metadata["user-agent"]
      call.request_metadata["user-agent"] = "intercepted/2"
      continuation.call
    end
    expect(unary_headers(interceptors: [interceptor])["user-agent"]).to eq(["intercepted/2"])
    expect(seen).to eq([nil])
  end

  describe "NetHTTP wire" do
    def wire_head(headers)
      observed = Queue.new
      peer = lambda do |socket|
        observed << read_request(socket)
        socket.write("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
        socket.read
      end
      with_peer(peer) do |url|
        request = transport_request(url)
        request.headers = request.headers.merge(headers)
        ConnectRPC::Transports::NetHTTP.new.call(request)
      end
      observed.pop(timeout: 0)
    end

    def agents(head) = head.scan(/^User-Agent:(.*)\r\n/i).flatten

    it "sends exactly the request's values and never Ruby's default" do
      expect(agents(wire_head({}))).to eq([])
      expect(agents(wire_head("user-agent" => []))).to eq([])
      expect(agents(wire_head("user-agent" => [""]))).to eq([" "])
      expect(agents(wire_head("user-agent" => [connect_agent]))).to eq([" #{connect_agent}"])
    end

    it "carries the client default end to end" do
      observed = Queue.new
      peer = lambda do |socket|
        observed << read_request(socket)
        socket.write("HTTP/1.1 200 OK\r\nContent-Type: application/proto\r\nContent-Length: 2\r\n\r\nok")
        socket.read
      end
      with_peer(peer) do |url|
        client = ConnectRPC::Client.new(base_url: url, binding: binding, transport: ConnectRPC::Transports::NetHTTP.new)
        expect(client.call(unary, "hi").message).to eq("ok")
      end
      expect(agents(observed.pop(timeout: 0))).to eq([" #{connect_agent}"])
    end
  end
end
