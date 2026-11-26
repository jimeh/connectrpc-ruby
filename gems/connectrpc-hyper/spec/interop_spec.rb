# frozen_string_literal: true

RSpec.describe "Interop with grpc-go and connect-go", :interop do
  origins = {
    "grpc_h2c" => [:grpc, false], "grpc_tls" => %i[grpc tls], "grpc_mtls" => %i[grpc mtls],
    "connect_h2c" => [:connect, false], "connect_tls" => %i[connect tls], "connect_mtls" => %i[connect mtls]
  }.freeze

  def transport_for(tls, **)
    tls ? hyper(**tls_options(mtls: tls == :mtls), **) : hyper(**)
  end

  origins.each do |name, (protocol, tls)|
    connections = tls ? %i[pooled] : %i[pooled per_call]
    connections.each do |mode|
      context "#{name} (#{mode})" do
        let(:transport) { transport_for(tls, connections: mode) }
        let(:peer_client) { client(peer[name], transport: transport, protocol: protocol) }

        it "makes unary calls with leading metadata and trailers" do
          response = peer_client.unary(MSG.new(value: "hello", payload: "\x00\xff".b), timeout: 5)
          expect([response.message.value, response.message.payload]).to eq(["hello", "\x00\xff".b])
          expect(response.metadata["x-head"]).to eq(["ready"])
          expect(response.trailers["x-tail"]).to eq(["done"])
        end

        it "decodes errors with typed details" do
          error = error_of { peer_client.unary(MSG.new(value: "error"), timeout: 5) }
          expect([error.code, error.message]).to eq(["not_found", "rich peer error"])
          detail = error.details.first.decode(binding: HyperHelpers::BINDING, type: MSG)
          expect([detail.value, detail.payload]).to eq(["opaque", "\x00\xff".b])
        end

        it "streams server messages with leading metadata, trailers and a late status" do
          received = []
          peer_client.server_stream(MSG.new(value: "first"), timeout: 5) { |reader| received = values(reader) }
          expect(received).to eq(["first", "second", ""])
          error = error_of do
            peer_client.server_stream(MSG.new(value: "error-after:2"), timeout: 5) do |reader|
              reader.each { |message| received << message.value }
            end
          end
          expect([error.code, error.message, received.last(2)]).to eq(["resource_exhausted", "late failure", %w[0 1]])
        end

        it "sends client streams incrementally" do
          from = peer.mark
          response = peer_client.client_stream(timeout: 5) do |writer|
            writer.send_message(MSG.new(value: "a"))
            peer.wait_event(/cs-recv a /, from: from)
            writer.send_message(MSG.new(value: "b"))
          end
          expect(response.message.value).to eq("a,b")
          expect(peer.since(from).grep(/cs-recv/)).to eq(["#{protocol} cs-recv a 0", "#{protocol} cs-recv b 0"])
        end

        it "interleaves bidi messages and ends after close_send" do
          from = peer.mark
          result = []
          peer_client.bidi(timeout: 5) do |stream|
            stream.send_message(MSG.new(value: "1"))
            result << stream.receive.value
            stream.send_message(MSG.new(value: "2"))
            result << stream.receive.value
            stream.close_send
            result << stream.receive
          end
          expect(result).to eq(["1", "2", nil])
          peer.wait_event(/#{protocol} bidi-eof/, from: from)
        end
      end
    end
  end

  context "with grpc-go's trailers-only error" do
    it "detects the HEADERS END_STREAM case and decodes its status and details" do
      error = error_of { client(peer["grpc_h2c"], transport: hyper).unary(MSG.new(value: "bare-error"), timeout: 5) }
      expect([error.code, error.message, error.details.size]).to eq(["not_found", "rich peer error", 1])
    end

    it "reports end_stream only when the status arrived in the response HEADERS" do
      transport = hyper
      heads = %w[trailers-only split-trailers].to_h do |scenario|
        request = ConnectRPC::StreamRequest.new(
          url: URI("#{peer['raw']}/raw.v1.Raw/Call"), http_method: "POST",
          headers: { "content-type" => ["application/grpc"], "te" => ["trailers"], "x-scenario" => [scenario] }
        )
        # The peer answers at the request HEADERS, so the request carries no
        # message: a write could race the response's end, which resets the
        # half-open stream and fails the write.
        transport.open_grpc_stream(request) do |exchange|
          exchange.close_send
          head = exchange.response_head(timeout: 5)
          nil while !head.end_stream && exchange.read(64, timeout: 5)
          trailers = head.end_stream ? nil : exchange.response_trailers
          exchange.finish(timeout: 5)
          [scenario, [head.end_stream, head.headers["grpc-status"], trailers&.fetch("grpc-status")]]
        end
      end
      expect(heads).to eq("trailers-only" => [true, ["5"], nil], "split-trailers" => [false, nil, ["5"]])
      expect(raw_error("split-trailers").then { |e| [e.code, e.message] }).to eq(%w[not_found split])
      expect(raw_error("trailers-only").then { |e| [e.code, e.message] }).to eq(%w[not_found bare])
    end

    def raw_error(scenario) = error_of { raw_call(peer["raw"], scenario, transport: hyper) }
  end

  context "with connect-go over HTTP/1.1" do
    it "makes unary, server-streaming and incremental client-streaming calls over cleartext HTTP/1.1" do
      from = peer.mark
      peer_client = client(peer["connect_h2c"], transport: hyper(http2: false), protocol: :connect)
      expect(peer_client.unary(MSG.new(value: "h1"), timeout: 5).message.value).to eq("h1")
      received = []
      peer_client.server_stream(MSG.new(value: "s"), timeout: 5) { |reader| received = values(reader) }
      expect(received).to eq(["s", "second", ""])
      response = peer_client.client_stream(timeout: 5) do |writer|
        writer.send_message(MSG.new(value: "a"))
        peer.wait_event(/connect cs-recv a /, from: from)
        writer.send_message(MSG.new(value: "b"))
      end
      expect(response.message.value).to eq("a,b")
      expect(peer.since(from).grep(/connect http/).map { |event| event.split[2] }.uniq).to eq(["HTTP/1.1"])
    end

    it "negotiates ALPN http/1.1 over TLS" do
      from = peer.mark
      peer_client = client(peer["connect_tls"], transport: hyper(http2: false, **tls_options), protocol: :connect)
      expect(peer_client.unary(MSG.new(value: "alpn"), timeout: 5).message.value).to eq("alpn")
      expect(peer.since(from).grep(/connect http/))
        .to eq(["connect http HTTP/1.1 /connectrpc.grpcinterop.v1.Peer/Unary"])
    end
  end

  it "sends Connect safe GET requests" do
    method = ConnectRPC::Method.new(service: "connectrpc.grpcinterop.v1.Peer", name: "Unary", request_type: MSG,
                                    response_type: MSG, idempotency: :no_side_effects)
    from = peer.mark
    response = core(peer["connect_h2c"], transport: hyper, protocol: :connect, use_get: true)
               .call(method, MSG.new(value: "get"), timeout: 5)
    expect([response.message.value, response.metadata["x-method"]]).to eq(["get", ["GET"]])
    expect(peer.since(from).grep(/connect http/)).to eq(["connect http HTTP/2.0 /connectrpc.grpcinterop.v1.Peer/Unary"])
  end
end
