# frozen_string_literal: true

require_relative "support/http1_helpers"

RSpec.describe "Connect H1 read-first input retirement" do
  include ConnectH1Helpers

  # Records each response head write and every framed read of the original
  # input, in order, and yields the request's retirement owner once created.
  def observe_wire
    events = []
    owners = Async::Queue.new
    resource_type = ConnectRPC::Falcon.const_get(:HTTP1Resource)
    allow(resource_type).to receive(:new).and_wrap_original do |constructor, *args, **options|
      constructor.call(*args, **options).tap do |resource|
        # The override reads the remainder before calling the writer.
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

  def early_handler(returned)
    lambda { |*|
      returned.enqueue(true)
      AsyncHelpers::MESSAGE.new(name: "early")
    }
  end

  # Raw TCP half-close precedes TLS cleanup, so TLS EOF has no close_notify.
  def peer_eof?(peer)
    peer.socket.read.empty?
  rescue OpenSSL::SSL::SSLError => e
    e.message.include?("unexpected eof")
  end

  def expect_close_after_reply(peer)
    status, headers = peer.head
    expect(status).to eq(200)
    expect(headers["connection"]).to eq(["close"])
    expect(peer.body).to include("early")
    expect(peer_eof?(peer)).to be(true)
  end

  [false, true].product(%i[fixed chunked]).each do |secure, framing|
    label = "#{secure ? 'TLS' : 'TCP'}/#{framing}"

    it "reads a paused upload's remainder before writing the head on #{label}" do
      reactor do |task|
        _, cs, = methods
        events, owners = observe_wire
        returned = Async::Queue.new
        with_host(target({ cs.path => early_handler(returned) }), secure) do |_host, peer, _|
          peer.write(request_head(cs, framing, length: 200_000))
          peer.write(upload("x" * 100_000, framing))
          returned.dequeue
          owner = owners.dequeue
          wait_until(task, "the head or a read of the sent part") do
            events.any? { |kind,| kind == :head } || read_bytes(events) >= 100_000
          end
          expect(events.map(&:first)).not_to include(:head)
          # The pause is the stimulus: #132's Go client stalled its upload for
          # 550 ms or more, past the former 500 ms close after the reply.
          task.sleep(0.6)
          expect(events.map(&:first)).not_to include(:head)
          peer.write(upload("x" * 100_000, framing, finish: true))
          expect_close_after_reply(peer)
          heads = events.each_index.select { |index| events[index].first == :head }
          expect(heads.size).to eq(1)
          expect(read_bytes(events.first(heads.first))).to eq(200_000)
          expect(owner.instance_variable_get(:@discarded)).to eq(200_000)
        end
      end
    end
  end

  [false, true].each do |secure|
    label = secure ? "TLS" : "TCP"

    it "writes the head without reading a declared remainder of 256 KiB and closes after the reply on #{label}" do
      reactor do
        _, cs, = methods
        events, owners = observe_wire
        returned = Async::Queue.new
        with_host(target({ cs.path => early_handler(returned) }), secure) do |_host, peer, _|
          peer.write(request_head(cs, :fixed, length: 262_144 + 1000))
          peer.write("x" * 1000)
          returned.dequeue
          owner = owners.dequeue
          expect_close_after_reply(peer)
          expect(read_bytes(events)).to eq(0)
          expect(owner.instance_variable_get(:@discarded)).to eq(0)
        end
      end
    end

    it "stops reading a chunked remainder at 256 KiB and closes after the reply on #{label}" do
      reactor do |task|
        _, cs, = methods
        events, owners = observe_wire
        returned = Async::Queue.new
        with_host(target({ cs.path => early_handler(returned) }), secure) do |_host, peer, _|
          peer.write(request_head(cs, :chunked))
          sending = task.async do
            # The server stops reading partway, then closes.
            peer.write(upload("x" * 524_288, :chunked))
          rescue IOError, SystemCallError, OpenSSL::SSL::SSLError
            nil
          end
          returned.dequeue
          owner = owners.dequeue
          expect_close_after_reply(peer)
          discarded = owner.instance_variable_get(:@discarded)
          expect(discarded).to be >= 262_144
          expect(discarded).to be < 262_144 + 65_536
          expect(read_bytes(events)).to eq(discarded)
          expect(events.last(1).map(&:first)).to eq([:head])
          sending.stop
        end
      end
    end

    it "bounds a stalled remainder with request_body_timeout on #{label}" do
      reactor do
        _, cs, = methods
        _, owners = observe_wire
        returned = Async::Queue.new
        with_host(target({ cs.path => early_handler(returned) }), secure, request_body_timeout: 0.2) do |_, peer, _|
          peer.write(request_head(cs, :fixed, length: 100_000))
          peer.write("x" * 1000)
          returned.dequeue
          started = AsyncHelpers::INTERNAL.now
          owner = owners.dequeue
          expect_close_after_reply(peer)
          expect(AsyncHelpers::INTERNAL.now - started).to be_between(0.15, 1.5)
          expect(owner.instance_variable_get(:@discarded)).to eq(1000)
        end
      end
    end

    it "bounds a stalled remainder with the Connect deadline on #{label}" do
      reactor do
        _, cs, = methods
        _, owners = observe_wire
        returned = Async::Queue.new
        with_host(target({ cs.path => early_handler(returned) }), secure) do |_, peer, _|
          started = AsyncHelpers::INTERNAL.now
          peer.write(request_head(cs, :chunked, extra: "Connect-Timeout-Ms: 300\r\n"))
          peer.write(upload("x" * 1000, :chunked))
          returned.dequeue
          owner = owners.dequeue
          expect_close_after_reply(peer)
          expect(AsyncHelpers::INTERNAL.now - started).to be_between(0.25, 1.5)
          expect(owner.instance_variable_get(:@discarded)).to eq(1000)
        end
      end
    end
  end

  it "answers an unanswered Expect: 100-continue request without reading its body" do
    reactor do
      _, cs, = methods
      events, = observe_wire
      returned = Async::Queue.new
      with_host(target({ cs.path => early_handler(returned) }), false) do |_, peer, _|
        peer.write(request_head(cs, :fixed, extra: "Expect: 100-continue\r\n"))
        returned.dequeue
        expect_close_after_reply(peer)
        expect(read_bytes(events)).to eq(0)
      end
    end
  end

  it "reads the remainder before the final chunk when the head was published during processing" do
    reactor do
      _, cs, = methods
      events, owners = observe_wire
      received = Async::Queue.new
      handler = lambda do |reader, context|
        context.send_headers(metadata: { "x-early" => "yes" })
        received.enqueue(reader.receive.name)
        AsyncHelpers::MESSAGE.new(name: "early")
      end
      with_host(target({ cs.path => handler }), false) do |_, peer, _|
        first = frame("first")
        peer.write(request_head(cs, :fixed, length: first.bytesize + 50_000))
        status, headers = peer.head
        expect(status).to eq(200)
        expect(headers["x-early"]).to eq(["yes"])
        owner = owners.dequeue
        peer.write(first)
        expect(received.dequeue).to eq("first")
        peer.write("x" * 50_000)
        expect(peer.body).to include("early")
        expect(peer_eof?(peer)).to be(true)
        # No read follows delivery, so the whole remainder preceded the final chunk.
        expect(owner.instance_variable_get(:@discarded)).to eq(50_000)
        expect(events.count { |kind,| kind == :head }).to eq(1)
      end
    end
  end
end
