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

  def ok_reply
    { protocol: :grpc, message: AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new(name: "reply")), code: 0 }
  end

  # A native gRPC unary call whose OK response ended while its request
  # waited for credit: the response decides, as in grpc-go, and the write
  # failure the transport recorded stays out of the outcome.
  def ok_unary(transport, origin)
    outcomes = []
    subscriber = Object.new
    subscriber.define_singleton_method(:start) do |_call|
      Object.new.tap { |observer| observer.define_singleton_method(:processing_finished) { outcomes << it } }
    end
    bus = ConnectRPC::Instrumentation.new(subscribers: [subscriber])
    reply = core(transport, origin, protocol: :grpc, instrumentation: bus).call(grpc_unary, big_message)
    [reply.message.name, outcomes.map { [it.code, it.error] }]
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

    it "returns a native gRPC unary call's OK reply" do
      with_peer(ok_reply) do |peer, origin|
        transport = threaded(origin: origin)
        expect(ok_unary(transport, origin)).to eq(["reply", [["OK", nil]]])
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

      it "returns a native gRPC unary call's OK reply on the #{path} path" do
        with_peer(ok_reply) do |peer, origin|
          transport = per_call
          expect(on_path(path) { ok_unary(transport, origin) }).to eq(["reply", [["OK", nil]]])
          expect_one_reset(peer)
        end
      end
    end
  end
end
