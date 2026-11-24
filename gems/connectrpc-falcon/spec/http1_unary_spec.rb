# frozen_string_literal: true

require_relative "support/http1_helpers"

# An early unary rejection leaves its upload unread. As net/http does after a
# handler returns, Falcon reads up to 256 KiB of it before the response head,
# keeps the connection when that read reaches EOF, and otherwise closes after
# the reply.
RSpec.describe "Connect H1 unary unread input" do
  include ConnectH1Helpers

  # Core rejects a body over this limit; a declared one before reading it.
  def unary_server = server(request_limit: 100)

  def unary_head(framing, length: 200_000, extra: "")
    field = framing == :fixed ? "Content-Length: #{length}" : "Transfer-Encoding: chunked"
    "POST #{AsyncHelpers::ECHO::ECHO.path} HTTP/1.1\r\nHost: localhost\r\n" \
      "Content-Type: application/proto\r\n#{field}\r\n#{extra}\r\n"
  end

  # Records each response head write and every read of the unread
  # remainder, in order, and yields each unary read owner once created.
  def observe_wire
    events = []
    owners = Async::Queue.new
    resource_type = ConnectRPC::Falcon.const_get(:HTTP1Resource)
    allow(resource_type).to receive(:new).and_wrap_original do |constructor, *args, **options|
      constructor.call(*args, **options).tap do |resource|
        allow(resource.connection).to receive(:write_response).and_wrap_original do |write, *values|
          write.call(*values).tap { events << [:head, values[1]] }
        end
      end
    end
    input_type = ConnectRPC::Falcon.const_get(:HTTP1StreamInput)
    allow(input_type).to receive(:new).and_wrap_original do |constructor, *args, **options|
      constructor.call(*args, **options).tap do |owner|
        allow(owner.instance_variable_get(:@body)).to receive(:read).and_wrap_original do |read|
          read.call.tap { |bytes| events << [:read, bytes&.bytesize] }
        end
        owners.enqueue(owner)
      end
    end
    [events, owners]
  end

  def read_bytes(events) = events.sum { |kind, size| kind == :read ? size.to_i : 0 }

  def response(peer)
    status, headers = peer.head
    [status, headers, peer.socket.read(Integer(headers["content-length"].first))]
  end

  # Raw TCP half-close precedes TLS cleanup, so TLS EOF has no close_notify.
  def peer_eof?(peer)
    peer.socket.read.empty?
  rescue OpenSSL::SSL::SSLError => e
    e.message.include?("unexpected eof")
  end

  def expect_close_after_reply(peer)
    status, headers, body = response(peer)
    expect(status).to eq(429)
    expect(headers["connection"]).to eq(["close"])
    expect(body).to include("resource_exhausted")
    expect(peer_eof?(peer)).to be(true)
  end

  [false, true].product(%i[fixed chunked]).each do |secure, framing|
    label = "#{secure ? 'TLS' : 'TCP'}/#{framing}"

    it "reads a paused remainder before the head and keeps the connection on #{label}" do
      reactor do |task|
        events, owners = observe_wire
        with_host(unary_server, secure) do |_host, peer, _|
          peer.write(unary_head(framing))
          peer.write(upload("x" * 100_000, framing))
          wait_until(task, "the head or a read of the remainder") do
            events.any? { |kind,| kind == :head } || read_bytes(events).positive?
          end
          expect(events.map(&:first)).not_to include(:head)
          # The pause is the stimulus: async-http answered at once and read
          # the remainder only afterwards, or never after a partial read.
          task.sleep(0.3)
          expect(events.map(&:first)).not_to include(:head)
          owner = owners.dequeue
          peer.write(upload("x" * 100_000, framing, finish: true))
          status, headers, = task.with_timeout(2) { response(peer) }
          expect(status).to eq(429)
          expect(headers["connection"]).not_to include("close")
          expect(owner).not_to be_retiring
          heads = events.each_index.select { |index| events[index].first == :head }
          expect(heads.size).to eq(1)
          expect(read_bytes(events.drop(heads.first))).to eq(0)
          peer.write("GET /next HTTP/1.1\r\nHost: localhost\r\n\r\n")
          expect(task.with_timeout(2) { peer.head }.first).to eq(404)
        end
      end
    end
  end

  [false, true].each do |secure|
    label = secure ? "TLS" : "TCP"

    it "writes the head without reading a declared remainder of 256 KiB and closes after the reply on #{label}" do
      reactor do |task|
        events, = observe_wire
        with_host(unary_server, secure) do |_host, peer, _|
          peer.write(unary_head(:fixed, length: 262_144 + 1000))
          peer.write("x" * 1000)
          task.with_timeout(2) { expect_close_after_reply(peer) }
          expect(read_bytes(events)).to eq(0)
        end
      end
    end

    it "stops reading a chunked remainder at 256 KiB and closes after the reply on #{label}" do
      reactor do |task|
        events, owners = observe_wire
        with_host(unary_server, secure) do |_host, peer, _|
          peer.write(unary_head(:chunked))
          sending = task.async do
            # The server stops reading partway, then closes.
            peer.write(upload("x" * 524_288, :chunked, finish: true))
          rescue IOError, SystemCallError, OpenSSL::SSL::SSLError
            nil
          end
          task.with_timeout(2) { expect_close_after_reply(peer) }
          discarded = owners.dequeue.instance_variable_get(:@discarded)
          expect(discarded).to be >= 262_144
          expect(discarded).to be < 262_144 + 65_536
          expect(read_bytes(events)).to eq(discarded)
          expect(events.last(1).map(&:first)).to eq([:head])
          sending.stop
        end
      end
    end

    it "bounds a stalled remainder with request_body_timeout on #{label}" do
      reactor do |task|
        with_host(unary_server, secure, request_body_timeout: 0.2) do |_, peer, _|
          started = AsyncHelpers::INTERNAL.now
          peer.write(unary_head(:fixed))
          peer.write("x" * 1000)
          task.with_timeout(2) { expect_close_after_reply(peer) }
          expect(AsyncHelpers::INTERNAL.now - started).to be >= 0.15
        end
      end
    end

    it "bounds a stalled remainder with the Connect deadline on #{label}" do
      reactor do |task|
        with_host(unary_server, secure) do |_, peer, _|
          started = AsyncHelpers::INTERNAL.now
          peer.write(unary_head(:chunked, extra: "Connect-Timeout-Ms: 300\r\n"))
          peer.write(upload("x" * 1000, :chunked))
          task.with_timeout(2) { expect_close_after_reply(peer) }
          expect(AsyncHelpers::INTERNAL.now - started).to be >= 0.25
        end
      end
    end
  end

  it "answers an unanswered Expect: 100-continue request without reading its body" do
    reactor do |task|
      events, = observe_wire
      with_host(unary_server, false) do |_, peer, _|
        peer.write(unary_head(:fixed, extra: "Expect: 100-continue\r\n"))
        task.with_timeout(2) { expect_close_after_reply(peer) }
        expect(read_bytes(events)).to eq(0)
      end
    end
  end

  it "leaves a complete unary request and its connection as they were" do
    reactor do |task|
      events, = observe_wire
      with_host(server, false) do |_, peer, _|
        bytes = AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new(name: "whole"))
        peer.write(unary_head(:fixed, length: bytes.bytesize) + bytes)
        status, headers, = task.with_timeout(2) { response(peer) }
        expect(status).to eq(200)
        expect(headers["connection"]).not_to include("close")
        expect(events.map(&:first)).to eq([:head])
        peer.write("GET /next HTTP/1.1\r\nHost: localhost\r\n\r\n")
        expect(task.with_timeout(2) { peer.head }.first).to eq(404)
      end
    end
  end
end
