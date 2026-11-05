# frozen_string_literal: true

require_relative "support/http1_client_helpers"
$LOAD_PATH.unshift(File.expand_path("../../../spec/fixtures/generation/ruby", __dir__))
require "generation/streaming_connect"

RSpec.describe ConnectRPC::Async::HTTP1Transport, "intentional abandonment" do
  include HTTP1ClientHelpers

  def partial_response(bytes, framing)
    fields = case framing
             when :chunked then "Transfer-Encoding: chunked\r\n"
             when :fixed then "Content-Length: #{bytes.bytesize + 8}\r\n"
             when :remainder then "Connection: close\r\n"
             end
    body = framing == :chunked ? "#{bytes.bytesize.to_s(16)}\r\n#{bytes}\r\n" : bytes
    "HTTP/1.1 200 OK\r\nContent-Type: application/connect+proto\r\n#{fields}\r\n#{body}"
  end

  def with_partial_response(bytes, framing, tls: false, &)
    release = Async::Promise.new
    peer = lambda do |native, socket, *_|
      incoming = native.read_request
      incoming.last&.each { |_| nil }
      socket.write(partial_response(bytes, framing))
      socket.flush if socket.respond_to?(:flush)
      release.wait
    end
    with_raw_http1(peer: peer, tls: tls, &)
  ensure
    release.resolve(nil)
  end

  def expect_abandoned(transport, operation, number, outcome, expected: :returned)
    expect(operation.fetch(:tasks).keys).to all(be_finished)
    expect(operation.fetch(:cleanup_task)).to be_finished
    expect(operation.fetch(:cleanup_done)).to be_resolved
    expect(operation[:cleanup_complete]).to be(true)
    expect(operation.fetch(:resource)).to be_finished
    expect(transport.instance_variable_get(:@calls)).to be_empty
    expect(transport.instance_variable_get(:@leases)).to be_empty
    expect(transport.instance_variable_get(:@idle)).to be_empty
    expect { IO.for_fd(number, autoclose: false) }.to raise_error(Errno::EBADF)
    expect(outcome).to eq(expected)
    expect(operation[:cleanup_error]).to be_nil
  end

  %i[chunked fixed remainder].each do |framing|
    [false, true].each do |tls|
      label = tls ? "TLS" : "TCP"
      it "permits explicit abort and normal return after a partial #{framing} response over #{label}" do
        with_partial_response("ok", framing, tls: tls) do |transport, origin|
          operation = nil
          number = nil
          outcome = begin
            transport.open_stream(stream_request(origin)) do |exchange|
              exchange.close_send
              expect(exchange.response_head.status).to eq(200)
              expect(exchange.read(2)).to eq("ok")
              operation = exchange.instance_variable_get(:@operation)
              number = operation.fetch(:resource).raw.fileno
              exchange.abort
              :returned
            end
          rescue ConnectRPC::Error => e
            e
          end
          expect_abandoned(transport, operation, number, outcome)
        end
      end

      it "permits public SS abandonment and normal return after a partial #{framing} response over #{label}" do
        message = GenerationFixture::Streaming::Item.new(name: "first")
        payload = GenerationFixture::Streaming::Item.encode(message)
        bytes = [0, payload.bytesize].pack("CN") + payload
        with_partial_response(bytes, framing, tls: tls) do |transport, origin|
          core = ConnectRPC::Client.new(base_url: origin, transport: transport,
                                        binding: ConnectRPC::Bindings::GoogleProtobuf.new)
          client = GenerationFixture::Streaming::Mixed::Client.new(client: core)
          operation = nil
          number = nil
          outcome = begin
            client.watch(GenerationFixture::Streaming::Item.new) do |reader|
              expect(reader.receive).to eq(message)
              operation = transport.instance_variable_get(:@calls).keys.fetch(0)
              number = operation.fetch(:resource).raw.fileno
              reader.abandon
              :returned
            end
          rescue ConnectRPC::Error => e
            e
          end
          expect_abandoned(transport, operation, number, outcome, expected: nil)
        end
      end
    end
  end

  it "permits implicit transport abandonment on normal block return" do
    with_partial_response("ok", :chunked) do |transport, origin|
      operation = nil
      number = nil
      outcome = begin
        transport.open_stream(stream_request(origin)) do |exchange|
          exchange.close_send
          expect(exchange.read(2)).to eq("ok")
          operation = exchange.instance_variable_get(:@operation)
          number = operation.fetch(:resource).raw.fileno
          :returned
        end
      rescue ConnectRPC::Error => e
        e
      end
      expect_abandoned(transport, operation, number, outcome)
    end
  end
end

RSpec.describe AsyncHelpers::INTERNAL::HTTP1Stream, "read half-close boundary" do
  it "ignores repeated read half-closes after the wrapped socket is locally closed" do
    socket, peer = Socket.pair(:UNIX, :STREAM, 0)
    stream = described_class.new(socket)
    socket.close
    2.times { expect(stream.close_read).to be_nil }
  ensure
    socket&.close unless socket&.closed?
    peer&.close
  end

  it "delegates read half-close to an open socket" do
    socket, peer = Socket.pair(:UNIX, :STREAM, 0)
    stream = described_class.new(socket)
    expect(socket).to receive(:close_read).and_call_original
    stream.close_read
    expect(socket).not_to be_closed
    expect(socket.read_nonblock(1, exception: false)).to be_nil
  ensure
    socket&.close
    peer&.close
  end

  it "preserves a genuine read half-close failure from an open socket" do
    socket, peer = Socket.pair(:UNIX, :STREAM, 0)
    stream = described_class.new(socket)
    failure = IOError.new("open socket half-close failed")
    allow(socket).to receive(:close_read).and_raise(failure)
    expect(socket).not_to be_closed
    expect { stream.close_read }.to raise_error(IOError) { |error| expect(error).to equal(failure) }
  ensure
    socket&.close
    peer&.close
  end
end
