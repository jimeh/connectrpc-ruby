# frozen_string_literal: true

RSpec.describe "Header, body and streaming limits" do
  def envelope(message) = [0, message.bytesize].pack("CN") + message

  def raw_exchange(transport, scenario)
    request = ConnectRPC::StreamRequest.new(
      url: URI("#{peer['raw']}/raw.v1.Raw/Call"), http_method: "POST",
      headers: { "content-type" => ["application/grpc"], "te" => ["trailers"], "x-scenario" => [scenario] }
    )
    transport.open_grpc_stream(request) do |exchange|
      exchange.write(envelope(""))
      exchange.close_send
      exchange.response_head(timeout: 5)
      nil while exchange.read(64, timeout: 5)
      exchange.response_trailers
    end
  end

  describe "gRPC response fields (core's 65,536-byte limit, enforced by the transport)" do
    %w[head trailers].product(%w[first last], [65_535, 65_536]).each do |block, position, size|
      it "accepts a #{block} block of #{size} bytes with the pad #{position}" do
        transport = hyper
        expect(raw_call(peer["raw"], "limit:#{block}:#{size}:#{position}", transport: transport).message)
          .to eq(MSG.new)
        expect { raw_exchange(transport, "limit:#{block}:#{size}:#{position}") }.not_to raise_error
      end
    end

    %w[head trailers].product(%w[first last]).each do |block, position|
      status = position == "first" ? "after" : "before"
      it "rejects a #{block} block of 65,537 bytes with grpc-status #{status} the pad" do
        transport = hyper
        error = error_of { raw_call(peer["raw"], "limit:#{block}:65537:#{position}", transport: transport) }
        expect([error.code, error.message]).to eq(["resource_exhausted", "gRPC field block exceeds byte limit"])
        # The transport's own check, beneath core's session.
        error = error_of { raw_exchange(transport, "limit:#{block}:65537:#{position}") }
        expect([error.code, error.message]).to eq(["resource_exhausted", "gRPC field block exceeds byte limit"])
      end
    end

    it "fails outbound gRPC fields over the limit before dispatch" do
      from = peer.mark
      transport = hyper
      error = error_of do
        raw_call(peer["raw"], "ok", transport: transport, metadata: { "x-scenario" => "ok", "x-big" => "b" * 65_536 })
      end
      expect([error.code, error.message]).to eq(["resource_exhausted", "gRPC field block exceeds byte limit"])
      expect(raw_call(peer["raw"], "ok", transport: transport,
                                         metadata: { "x-scenario" => "ok", "x-big" => "b" * 60_000 })
               .message).to eq(MSG.new)
      expect(peer.since(from).grep(/\Araw request/).size).to eq(1)
    end
  end

  describe "Connect unary response bodies" do
    it "fails resource_exhausted by advertised content-length and by incremental body" do
      advertised = TinyHTTP.new { |_request, socket| TinyHTTP.ok(socket, body: "x" * 100) }
      chunked = TinyHTTP.new do |_request, socket|
        socket.write("HTTP/1.1 200 OK\r\ncontent-type: application/proto\r\ntransfer-encoding: chunked\r\n\r\n")
        3.times { socket.write("10\r\n#{'y' * 16}\r\n") }
        socket.write("0\r\n\r\n")
      end
      [advertised, chunked].each do |server|
        peer_client = client(server.origin, transport: hyper(http2: false), protocol: :connect, response_limit: 40)
        error = error_of { peer_client.unary(MSG.new, timeout: 5) }
        expect([error.code, error.message]).to eq(["resource_exhausted", "Response exceeds byte limit"])
      end
      big = client(peer["connect_h2c"], transport: hyper, protocol: :connect, response_limit: 1024)
      error = error_of { big.unary(MSG.new(value: "big:4096"), timeout: 5) }
      expect([error.code, error.message]).to eq(["resource_exhausted", "Response exceeds byte limit"])
    ensure
      advertised&.close
      chunked&.close
    end
  end

  describe "streams" do
    it "fails an over-limit message with resource_exhausted" do
      %w[grpc_h2c connect_h2c].each do |name|
        protocol = name.start_with?("grpc") ? :grpc : :connect
        peer_client = client(peer[name], transport: hyper, protocol: protocol, response_limit: 4096)
        error = error_of { peer_client.server_stream(MSG.new(value: "count:1:8192"), timeout: 5) { |r| values(r) } }
        expect(error.code).to eq("resource_exhausted")
      end
    end

    it "bounds a flooding peer by the receive window" do
      from = peer.mark
      read = 0
      expect do
        client(peer["grpc_h2c"], transport: hyper).server_stream(MSG.new(value: "flood"), timeout: 10) do |reader|
          reader.each do
            read += 1
            break if read == 100
          end
        end
      end.to raise_error(ConnectRPC::StreamNotConsumedError)
      sent = peer.wait_event(/grpc ss-flood-done/, from: from).split[2].to_i
      # 1 KiB messages; the BDP window is capped at 16 MiB, like grpc-go's.
      expect(sent).to be_between(read, read + ((16 << 20) / 1024) + 64)
    end

    # The raw peer, unlike grpc-go's, never resets the stream at the deadline
    # it was sent, a reset that can land before the client's deadline and
    # release the blocked writer early.
    it "blocks writes to a peer that never reads until the deadline" do
      from = peer.mark
      written = 0
      started = monotonic
      metadata = { "x-scenario" => "noread" }
      error = error_of do
        core(peer["raw"], transport: hyper)
          .client_stream(HyperHelpers::RAW_CLIENT_STREAM, metadata: metadata, timeout: 1) do |writer|
          loop do
            writer.send_message(MSG.new(payload: "x".b * 65_536))
            written += 1
          end
        end
      end
      expect(error.code).to eq("deadline_exceeded")
      expect(monotonic - started).to be >= 0.9
      # The stream window plus h2's bounded send buffer, then the writer blocks.
      expect(written).to be < 40
      unread = peer.wait_event(/\Araw request noread /, from: from)[/ stream=(\d+) /, 1]
      peer.wait_event(/\Araw rst stream=#{unread} code=8 /, from: from)
    end
  end
end
