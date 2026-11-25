# frozen_string_literal: true

require_relative "support/half_open_peer"
require_relative "support/per_call_helpers"

# The owner-thread transports relay the inner HTTP/2 transport, so a response
# that ends while the request body waits for flow-control credit resets the
# stream with NO_ERROR and still delivers the server's status. The peer
# answers at the request's HEADERS and never grants credit past one frame,
# so the request cannot finish before the response ends.
RSpec.describe "Half-open HTTP/2 streams through owner-thread transports" do
  include HalfOpenPeer
  include PerCallHelpers

  def with_peer(response)
    peer = threaded_recording_peer(settings: { Protocol::HTTP2::Settings::INITIAL_WINDOW_SIZE => 16_384 },
                                   &half_open_handler(HalfOpenPeer::Static.new(response), false))
    yield peer.peer, "http://127.0.0.1:#{peer.peer.port}"
  ensure
    peer&.stop
  end

  def upload = 65_536

  def grpc_unary
    ConnectRPC::Method.new(service: "half", name: "Unary", request_type: AsyncHelpers::MESSAGE,
                           response_type: AsyncHelpers::MESSAGE)
  end

  def core(transport, origin, **)
    ConnectRPC::Client.new(base_url: origin, transport: transport, binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                           **)
  end

  def big_message = AsyncHelpers::MESSAGE.new(name: "x" * upload)

  # Every reset the peer read on its only connection, once it read them.
  def expect_one_reset(peer, connection: 0)
    eventually("the client's reset") { peer.wire(connection)[:reset_log].any? }
    expect(peer.wire(connection)[:reset_log].values).to eq([[Protocol::HTTP2::Error::NO_ERROR]])
    expect(peer.sent_resets).to be_empty
  end

  def expect_not_found
    raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("not_found") }
  end

  describe "ThreadedTransport" do
    it "returns a unary call's response and reuses the connection" do
      with_peer({ protocol: :unary, message: "done" }) do |peer, origin|
        transport = threaded(origin: origin)
        response = transport.call(request("#{origin}/half/unary", body: "x" * upload))
        expect([response.status, response.body]).to eq([200, "done"])
        expect(transport.call(request("#{origin}/after")).body).to eq("ok")
        expect(peer.accepted).to eq(1)
        expect_one_reset(peer)
      ensure
        transport&.close
      end
    end

    it "reports a native gRPC unary call's status" do
      with_peer({ protocol: :grpc, trailers_only: true }) do |peer, origin|
        transport = threaded(origin: origin)
        expect { core(transport, origin, protocol: :grpc).call(grpc_unary, big_message) }.to expect_not_found
        expect_one_reset(peer)
      ensure
        transport&.close
      end
    end

    it "reports a server stream's status" do
      with_peer({ protocol: :connect }) do |peer, origin|
        transport = threaded(origin: origin)
        method = half_open_method(ConnectRPC::ServerStreamMethod)
        expect { core(transport, origin).server_stream(method, big_message) { |reader| reader.each { nil } } }
          .to expect_not_found
        expect_one_reset(peer)
      ensure
        transport&.close
      end
    end
  end

  describe "PerCallTransport" do
    PerCallHelpers::PATHS.each do |path|
      it "returns a unary call's response on the #{path} path" do
        with_peer({ protocol: :unary, message: "done" }) do |peer, origin|
          transport = per_call
          result = on_path(path) do
            response = transport.call(request("#{origin}/half/unary", body: "x" * upload))
            [response.status, response.body]
          end
          expect(result).to eq([200, "done"])
          expect_one_reset(peer)
        end
      end

      it "reports a native gRPC unary call's status on the #{path} path" do
        with_peer({ protocol: :grpc, trailers_only: true }) do |peer, origin|
          transport = per_call
          result = on_path(path) { core(transport, origin, protocol: :grpc).call(grpc_unary, big_message) }
          expect(result).to be_a(ConnectRPC::Error)
          expect(result.code).to eq("not_found")
          expect_one_reset(peer)
        end
      end
    end
  end
end
