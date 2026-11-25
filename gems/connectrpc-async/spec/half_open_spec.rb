# frozen_string_literal: true

require_relative "support/half_open_peer"

# A server that ends its response with END_STREAM while the request body is
# still open, without resetting the stream, gets RST_STREAM(NO_ERROR) from the
# client, as grpc-go's client sends; later writes raise and the response
# still reaches the caller.
module HalfOpenPeerExamples
  module_function

  def request(origin)
    ConnectRPC::StreamRequest.new(url: URI("#{origin}/half/stream"), http_method: "POST",
                                  headers: { "content-type" => ["application/grpc+proto"] }, deadline: nil)
  end

  def unary(origin)
    ConnectRPC::TransportRequest.new(url: URI("#{origin}/half/unary"), http_method: "POST", headers: {}, body: "x",
                                     deadline: nil, response_limit: 1024)
  end
end

RSpec.describe "HTTP/2 half-open client streams" do
  include HalfOpenPeer

  def with_half_open_peer(settings: {}, wait_data: true, release: Async::Queue.new, **transport_options)
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      peer = half_open_peer(task, bound.sockets.first, release, settings: settings, wait_data: wait_data)
      origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin, **transport_options)
      yield task, transport, origin, peer, release
    ensure
      transport&.close(grace: 0)
      peer&.stop
      bound&.close
    end
  end

  # The follow-up call's frames follow the stream's on the one connection,
  # so the peer has read every frame the stream sent once it is answered.
  def expect_reuse(transport, origin, peer)
    expect(transport.call(request("#{origin}/after")).body).to eq("ok")
    expect(peer.accepted).to eq(1)
    expect(transport.instance_variable_get(:@resources).size).to eq(1)
    expect(lease_usage(transport)).to eq(0)
  end

  def one_frame_window = { HalfOpenPeer::WINDOW => 16_384 }

  def path_releases = Hash.new { |queues, path| queues[path] = Async::Queue.new }

  def connection_of(transport) = transport.instance_variable_get(:@resources).first.connection

  # Wraps the client connection's socket; FrameGate options select the frame.
  def wrap_client(transport, wrapper = FrameGate, **)
    resource = transport.instance_variable_get(:@resources).first
    wrapped = wrapper.new(resource.socket, **)
    resource.connection.stream.instance_variable_set(:@io, wrapped)
    wrapped
  end

  def core_client(transport, origin, protocol)
    options = protocol == :grpc ? { protocol: :grpc } : {}
    ConnectRPC::Client.new(base_url: origin, transport: transport, binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                           **options)
  end

  def message(name) = AsyncHelpers::MESSAGE.new(name: name)

  def expect_code(code)
    raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq(code) }
  end

  # Holds a sibling stream's first DATA frame mid-write, so the sibling owns
  # the frame guard until `gate.release`; `proceed` then lets it close its
  # side and read its response.
  def gated_sibling(task, transport, origin, gate, proceed)
    sibling = task.async(finished: false) do
      transport.open_stream(half_open_request(origin, :connect, path: "/half/sibling")) do |exchange|
        exchange.write(half_open_envelope(0, "sibling"))
        proceed.dequeue
        exchange.write("s" * 16_000)
        exchange.close_send
        [half_open_operation(transport).fetch(:response).stream, read_all(exchange)]
      end
    end
    gate.started.dequeue
    sibling
  end

  def expect_unavailable
    raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("unavailable") }
  end

  def read_all(exchange)
    bytes = +"".b
    while (chunk = exchange.read(16_384))
      break if chunk.empty?

      bytes << chunk
    end
    bytes
  end

  message = "reply"
  { grpc: :open_grpc_stream, connect: :open_stream, grpc_web: :open_stream }.each do |protocol, opener|
    it "resets a #{protocol} stream whose response ended half-open and raises from a later write" do
      with_half_open_peer do |_task, transport, origin, peer, release|
        stream = nil
        transport.public_send(opener, half_open_request(origin, protocol)) do |exchange|
          exchange.write(half_open_envelope(0, "first"))
          stream = half_open_operation(transport).fetch(:response).stream
          release.enqueue({ protocol: protocol, message: message })
          expect(exchange.response_head.status).to eq(200)
          bytes = read_all(exchange)
          expect(bytes).to start_with(half_open_envelope(0, message))
          case protocol
          when :grpc then expect(exchange.response_trailers["grpc-status"]).to eq(["5"])
          when :connect then expect(bytes).to end_with(half_open_envelope(2, HalfOpenPeer::NOT_FOUND))
          else expect(bytes).to end_with(half_open_envelope(0x80, "grpc-status: 5\r\ngrpc-message: missing\r\n"))
          end
          expect { exchange.write(half_open_envelope(0, "late")) }.to expect_unavailable
        end
        expect_reuse(transport, origin, peer)
        expect(client_resets(peer, stream.id)).to eq([Protocol::HTTP2::Error::NO_ERROR])
        expect(peer.wire[:ended]).not_to include(stream.id)
        expect(peer.wire[:data][stream.id]).to eq(half_open_envelope(0, "first").bytesize)
        expect(peer.sent_resets).to be_empty
      end
    end
  end

  it "resets a gRPC stream whose trailers-only response ended half-open" do
    with_half_open_peer do |_task, transport, origin, peer, release|
      stream = nil
      transport.open_grpc_stream(half_open_request(origin, :grpc)) do |exchange|
        exchange.write(half_open_envelope(0, "first"))
        stream = half_open_operation(transport).fetch(:response).stream
        release.enqueue({ protocol: :grpc, trailers_only: true })
        head = exchange.response_head
        expect(head.end_stream).to be(true)
        expect(head.headers["grpc-status"]).to eq(["5"])
        expect { exchange.write(half_open_envelope(0, "late")) }.to expect_unavailable
      end
      expect_reuse(transport, origin, peer)
      expect(client_resets(peer, stream.id)).to eq([Protocol::HTTP2::Error::NO_ERROR])
      expect(peer.sent_resets).to be_empty
    end
  end

  %i[connect grpc].each do |protocol|
    it "stops a core #{protocol} client stream at the response's end and reports the server's code" do
      with_half_open_peer do |task, transport, origin, peer, release|
        stream = nil
        late = 0
        expect do
          core_client(transport, origin, protocol).client_stream(half_open_method, timeout: nil) do |writer|
            writer.send_message(message("first"))
            stream = half_open_operation(transport).fetch(:response).stream
            release.enqueue({ protocol: protocol })
            wait_until(task, "response end read") { stream.state != :open }
            writer.send_message(message("late"))
            late += 1
          end
        end.to expect_code("not_found")
        expect(late).to eq(0)
        expect_reuse(transport, origin, peer)
        expect(client_resets(peer, stream.id)).to eq([Protocol::HTTP2::Error::NO_ERROR])
        expect(peer.sent_resets).to be_empty
      end
    end
  end

  # async-http resets a stream whose HEADERS fail validation, and
  # protocol-http2 then still records the frame's END_STREAM.
  it "sends only its own reset when gRPC trailers ending a half-open stream fail validation" do
    with_half_open_peer do |task, transport, origin, peer, release|
      stream = nil
      expect do
        core_client(transport, origin, :grpc).client_stream(half_open_method, timeout: nil) do |writer|
          writer.send_message(message("first"))
          stream = half_open_operation(transport).fetch(:response).stream
          release.enqueue({ protocol: :grpc, extra: [["x-large", "x" * 65_536]] })
          wait_until(task, "trailers read") { stream.state != :open }
        end
      end.to expect_code("resource_exhausted")
      expect(stream).not_to be_response_ended
      expect_reuse(transport, origin, peer)
      expect(client_resets(peer, stream.id)).to eq([Protocol::HTTP2::Error::ENHANCE_YOUR_CALM])
      expect(peer.sent_resets).to be_empty
    end
  end

  it "wakes an upload parked on flow control at the response's end and fails the write waiting to queue" do
    with_half_open_peer(settings: one_frame_window, wait_data: false) do |task, transport, origin, peer, release|
      stream = nil
      transport.open_stream(half_open_request(origin, :connect)) do |exchange|
        body = exchange.instance_variable_get(:@body)
        stream = half_open_operation(transport).fetch(:response).stream
        exchange.write("a" * 16_384)
        exchange.write("b" * 16_384)
        wait_until(task, "output parked on credit") do
          peer.wire[:data][stream.id] == 16_384 && body.queued_bytes.zero?
        end
        expect(stream.available_frame_size).to eq(0)
        # Queued behind the parked fragment, so this write returns.
        exchange.write("c" * 16_384)
        blocked = task.async(finished: false) { exchange.write("d" * 16_384) }
        wait_until(task, "write waits to queue") { body.instance_variable_get(:@changed).waiting? }
        release.enqueue({ protocol: :connect })
        wait_until(task, "blocked write ended") { blocked.finished? }
        expect { blocked.wait }.to expect_unavailable
        expect(read_all(exchange)).to eq(half_open_envelope(2, HalfOpenPeer::NOT_FOUND))
        expect(exchange.finish).to be_nil
      end
      expect_reuse(transport, origin, peer)
      expect(peer.wire[:data][stream.id]).to eq(16_384)
      expect(client_resets(peer, stream.id)).to eq([Protocol::HTTP2::Error::NO_ERROR])
    end
  end

  it "abandons DATA waiting for the frame guard at the response's end and leaves the credit to a sibling" do
    releases = path_releases
    with_half_open_peer(release: releases) do |task, transport, origin, peer, _release|
      expect(transport.call(request("#{origin}/warm")).body).to eq("ok")
      first = half_open_envelope(0, "first")
      main = sibling = gate = nil
      proceed = Async::Queue.new
      transport.open_stream(half_open_request(origin, :connect, path: "/half/main")) do |exchange|
        body = exchange.instance_variable_get(:@body)
        exchange.write(first)
        main = half_open_operation(transport).fetch(:response).stream
        wait_until(task, "first DATA read") { peer.wire[:data][main.id] == first.bytesize }
        gate = wrap_client(transport, frame_type: Protocol::HTTP2::DataFrame::TYPE)
        gate.armed = true
        sibling = gated_sibling(task, transport, origin, gate, proceed)
        exchange.write(half_open_envelope(0, "second"))
        wait_until(task, "DATA waits for the guard") { body.queued_bytes.zero? && guard_waiters(main.connection) == 1 }
        releases["/half/main"].enqueue({ protocol: :connect })
        wait_until(task, "response end read") { main.state != :open }
        gate.release.enqueue(true)
        expect(read_all(exchange)).to eq(half_open_envelope(2, HalfOpenPeer::NOT_FOUND))
      end
      proceed.enqueue(true)
      wait_until(task, "sibling END_STREAM read") { peer.wire[:ended].size == 2 }
      releases["/half/sibling"].enqueue({ protocol: :connect, code: 0 })
      sibling_stream, bytes = sibling.wait
      expect(bytes).to eq(half_open_envelope(2, "{}"))
      expect_reuse(transport, origin, peer)
      expect(peer.wire[:data][main.id]).to eq(first.bytesize)
      expect(main.remote_window.used).to eq(first.bytesize)
      expect(client_resets(peer, main.id)).to eq([Protocol::HTTP2::Error::NO_ERROR])
      expect(peer.wire[:data][sibling_stream.id]).to eq(half_open_envelope(0, "sibling").bytesize + 16_000)
      expect(peer.wire[:ended]).to include(sibling_stream.id)
      expect(client_resets(peer, sibling_stream.id)).to be_empty
    end
  end

  it "sends no reset when the peer resets after its END_STREAM before the client's reset is written" do
    releases = path_releases
    with_half_open_peer(release: releases) do |task, transport, origin, peer, _release|
      expect(transport.call(request("#{origin}/warm")).body).to eq("ok")
      main = sibling = gate = nil
      proceed = Async::Queue.new
      transport.open_stream(half_open_request(origin, :connect, path: "/half/main")) do |exchange|
        exchange.write(half_open_envelope(0, "first"))
        main = half_open_operation(transport).fetch(:response).stream
        wait_until(task, "first DATA read") { peer.wire[:data][main.id].positive? }
        gate = wrap_client(transport, frame_type: Protocol::HTTP2::DataFrame::TYPE)
        gate.armed = true
        sibling = gated_sibling(task, transport, origin, gate, proceed)
        releases["/half/main"].enqueue({ protocol: :connect, reset_after: Protocol::HTTP2::Error::NO_ERROR })
        wait_until(task, "peer reset read") { main.closed? }
        gate.release.enqueue(true)
        expect(read_all(exchange)).to eq(half_open_envelope(2, HalfOpenPeer::NOT_FOUND))
        expect { exchange.write(half_open_envelope(0, "late")) }.to expect_unavailable
      end
      proceed.enqueue(true)
      releases["/half/sibling"].enqueue({ protocol: :connect, code: 0 })
      sibling.wait
      expect_reuse(transport, origin, peer)
      expect(client_resets(peer, main.id)).to be_empty
      expect(peer.sent_resets).to eq([[0, main.id, Protocol::HTTP2::Error::NO_ERROR]])
    end
  end

  # Cleanup decides its reset inside the frame guard, after a sibling's frame
  # and the abandoned stream's own waiting DATA, so it sees what the peer
  # did meanwhile.
  {
    "writes only the NO_ERROR reset when the response ends" => [{ protocol: :connect }, :response_ended?,
                                                                [Protocol::HTTP2::Error::NO_ERROR]],
    "writes no reset when the peer resets the stream" => [{ protocol: :connect, reset: Protocol::HTTP2::Error::CANCEL },
                                                          :closed?, []]
  }.each do |outcome, (response, settled, resets)|
    it "#{outcome} while cleanup of an abandoned stream waits for the frame guard" do
      releases = path_releases
      with_half_open_peer(release: releases) do |task, transport, origin, peer, _release|
        abandoned = abandon_behind_sibling([task, transport, origin, peer, releases], response, settled)
        expect(abandoned.fetch(:call).wait).to eq(:abandoned)
        expect(abandoned.fetch(:operation)[:cleanup_error]).to be_nil
        abandoned.fetch(:proceed).enqueue(true)
        releases["/half/sibling"].enqueue({ protocol: :connect, code: 0 })
        abandoned.fetch(:sibling).wait
        expect_reuse(transport, origin, peer)
        main = abandoned.fetch(:main)
        expect(peer.wire[:data][main.id]).to eq(half_open_envelope(0, "first").bytesize)
        expect(client_resets(peer, main.id)).to eq(resets)
      end
    end
  end

  it "keeps a gRPC block's result when cleanup's NO_ERROR reset write fails after waiting for the frame guard" do
    releases = path_releases
    with_half_open_peer(release: releases) do |task, transport, origin, peer, _release|
      abandoned = abandon_behind_sibling([task, transport, origin, peer, releases], { protocol: :grpc },
                                         :response_ended?, protocol: :grpc, fail_resets: true)
      resource = abandoned.fetch(:main).connection.resource
      expect(abandoned.fetch(:call).wait).to eq(:abandoned)
      expect(abandoned.fetch(:operation)[:cleanup_error]).to be_nil
      expect(resource).to be_finished
      abandoned.fetch(:proceed).enqueue(true)
      expect { abandoned.fetch(:sibling).wait }.to expect_unavailable
      expect(transport.call(request("#{origin}/after")).body).to eq("ok")
      expect(peer.accepted).to eq(2)
      expect(client_resets(peer, abandoned.fetch(:main).id)).to be_empty
    end
  end

  # Opens /half/main, holds the guard with a gated sibling while the main
  # stream's second DATA waits for it, and returns the main block, whose
  # cleanup then also waits for the guard. The peer answers with `response`
  # and the guard is released once the stream reports `settled`. `setup` is
  # the task, transport, origin, peer and releases of #with_half_open_peer.
  def abandon_behind_sibling(setup, response, settled, protocol: :connect, fail_resets: false)
    task, transport, origin, peer, releases = setup
    first = half_open_envelope(0, "first")
    opened = Async::Queue.new
    go = Async::Queue.new
    abandon = Async::Queue.new
    opener = protocol == :grpc ? :open_grpc_stream : :open_stream
    call = task.async(finished: false) do
      transport.public_send(opener, half_open_request(origin, protocol, path: "/half/main")) do |exchange|
        exchange.write(first)
        opened.enqueue([half_open_operation(transport), exchange.instance_variable_get(:@body)])
        go.dequeue
        exchange.write(half_open_envelope(0, "second"))
        abandon.dequeue
        :abandoned
      end
    end
    operation, body = opened.dequeue
    main = operation.fetch(:response).stream
    wait_until(task, "first DATA read") { peer.wire[:data][main.id] == first.bytesize }
    gate = gate_data(transport, fail_resets)
    proceed = Async::Queue.new
    sibling = gated_sibling(task, transport, origin, gate, proceed)
    go.enqueue(true)
    wait_until(task, "DATA waits for the guard") { body.queued_bytes.zero? && guard_waiters(main.connection) == 1 }
    abandon.enqueue(true)
    wait_until(task, "cleanup waits for the guard") { operation[:cleaned] && guard_waiters(main.connection) == 2 }
    expect(main.state).to eq(:open)
    releases["/half/main"].enqueue(response)
    wait_until(task, "peer's frame read") { main.public_send(settled) }
    gate.release.enqueue(true)
    { call: call, operation: operation, main: main, sibling: sibling, proceed: proceed }
  end

  # An armed DATA gate on the client socket. With `fail_resets`, every
  # RST_STREAM write fails before it reaches the gate, which splits the held
  # write.
  def gate_data(transport, fail_resets)
    resource = transport.instance_variable_get(:@resources).first
    gate = FrameGate.new(resource.socket, frame_type: Protocol::HTTP2::DataFrame::TYPE)
    io = gate
    if fail_resets
      io = HalfOpenPeer::ResetFailure.new(gate)
      io.armed = true
    end
    resource.connection.stream.instance_variable_set(:@io, io)
    gate.armed = true
    gate
  end

  it "ends a request closed before the response ended with END_STREAM and no reset" do
    with_half_open_peer do |task, transport, origin, peer, release|
      stream = nil
      transport.open_stream(half_open_request(origin, :connect)) do |exchange|
        exchange.write(half_open_envelope(0, "first"))
        exchange.close_send
        stream = half_open_operation(transport).fetch(:response).stream
        wait_until(task, "request END_STREAM read") { peer.wire[:ended].include?(stream.id) }
        release.enqueue({ protocol: :connect })
        expect(read_all(exchange)).to eq(half_open_envelope(2, HalfOpenPeer::NOT_FOUND))
        exchange.finish
      end
      expect_reuse(transport, origin, peer)
      expect(client_resets(peer, stream.id)).to be_empty
    end
  end

  it "commits END_STREAM for a request closed while its DATA waited for the frame guard" do
    releases = path_releases
    with_half_open_peer(release: releases) do |task, transport, origin, peer, _release|
      expect(transport.call(request("#{origin}/warm")).body).to eq("ok")
      main = sibling = gate = nil
      proceed = Async::Queue.new
      transport.open_stream(half_open_request(origin, :connect, path: "/half/main")) do |exchange|
        body = exchange.instance_variable_get(:@body)
        exchange.write(half_open_envelope(0, "first"))
        main = half_open_operation(transport).fetch(:response).stream
        wait_until(task, "first DATA read") { peer.wire[:data][main.id].positive? }
        gate = wrap_client(transport, frame_type: Protocol::HTTP2::DataFrame::TYPE)
        gate.armed = true
        sibling = gated_sibling(task, transport, origin, gate, proceed)
        exchange.write(half_open_envelope(0, "second"))
        wait_until(task, "DATA waits for the guard") { body.queued_bytes.zero? && guard_waiters(main.connection) == 1 }
        exchange.close_send
        releases["/half/main"].enqueue({ protocol: :connect })
        wait_until(task, "response end read") { main.state != :open }
        gate.release.enqueue(true)
        expect(read_all(exchange)).to eq(half_open_envelope(2, HalfOpenPeer::NOT_FOUND))
        exchange.finish
      end
      proceed.enqueue(true)
      releases["/half/sibling"].enqueue({ protocol: :connect, code: 0 })
      sibling.wait
      expect_reuse(transport, origin, peer)
      sent = half_open_envelope(0, "first") + half_open_envelope(0, "second")
      expect(peer.wire[:data][main.id]).to eq(sent.bytesize)
      expect(peer.wire[:ended]).to include(main.id)
      expect(client_resets(peer, main.id)).to be_empty
    end
  end

  it "keeps the reset when the request closes after the response ended" do
    with_half_open_peer do |_task, transport, origin, peer, release|
      stream = nil
      transport.open_stream(half_open_request(origin, :connect)) do |exchange|
        exchange.write(half_open_envelope(0, "first"))
        stream = half_open_operation(transport).fetch(:response).stream
        release.enqueue({ protocol: :connect })
        expect(read_all(exchange)).to eq(half_open_envelope(2, HalfOpenPeer::NOT_FOUND))
        exchange.close_send
        exchange.finish
      end
      expect_reuse(transport, origin, peer)
      expect(client_resets(peer, stream.id)).to eq([Protocol::HTTP2::Error::NO_ERROR])
      expect(peer.wire[:ended]).not_to include(stream.id)
    end
  end

  it "resets a unary call whose response ended while its body waited for credit" do
    with_half_open_peer(settings: one_frame_window, wait_data: false) do |task, transport, origin, peer, release|
      call = task.async(finished: false) { transport.call(request("#{origin}/half/unary", body: "x" * 65_536)) }
      stream = nil
      wait_until(task, "unary body parked on credit") do
        stream = half_open_operation(transport)&.[](:response)&.stream
        stream && peer.wire[:data][stream.id] == 16_384
      end
      release.enqueue({ protocol: :unary, message: "done" })
      response = call.wait
      expect([response.status, response.body]).to eq([200, "done"])
      expect_reuse(transport, origin, peer)
      expect(peer.wire[:data][stream.id]).to eq(16_384)
      expect(client_resets(peer, stream.id)).to eq([Protocol::HTTP2::Error::NO_ERROR])
    end
  end

  # protocol-http2 opens the stream after its HEADERS flush; a response that
  # ended during the flush must not turn the request into a refusal.
  {
    grpc: lambda do |transport, origin|
      transport.open_grpc_stream(HalfOpenPeerExamples.request(origin)) do |exchange|
        head = exchange.response_head
        late = begin
          exchange.write("late")
        rescue ConnectRPC::Error => e
          e.code
        end
        [head.end_stream, head.headers["grpc-status"], late]
      end
    end,
    unary: lambda do |transport, origin|
      response = transport.call(HalfOpenPeerExamples.unary(origin))
      [response.status, response.body]
    end
  }.each do |kind, invoke|
    it "keeps a #{kind} response that ended during the request HEADERS flush and resets the stream" do
      with_half_open_peer(wait_data: false) do |task, transport, origin, peer, release|
        expect(transport.call(request("#{origin}/warm")).body).to eq("ok")
        gate = wrap_client(transport, frame_type: Protocol::HTTP2::HeadersFrame::TYPE, whole: true)
        gate.armed = true
        release.enqueue(kind == :grpc ? { protocol: :grpc, trailers_only: true } : { protocol: :unary })
        outcome = task.async(finished: false) { invoke.call(transport, origin) }
        gate.started.dequeue
        stream = connection_of(transport).streams.values.max_by(&:id)
        wait_until(task, "response ended during the HEADERS flush") { stream.state == :half_closed_remote }
        gate.release.enqueue(true)
        result = nil
        expect { result = outcome.wait }.not_to raise_error
        expect(result).to eq(kind == :grpc ? [true, ["5"], "unavailable"] : [200, ""])
        expect_reuse(transport, origin, peer)
        expect(client_resets(peer, stream.id)).to eq([Protocol::HTTP2::Error::NO_ERROR])
        # Warm, this request once, and the follow-up: never resent as refused.
        expect(peer.headers_frames.size).to eq(3)
      end
    end
  end

  it "reports the server's code while the reset waits behind a sibling's frame past core recovery" do
    releases = path_releases
    with_half_open_peer(release: releases) do |task, transport, origin, peer, _release|
      expect(transport.call(request("#{origin}/warm")).body).to eq("ok")
      ready = Async::Queue.new
      go = Async::Queue.new
      proceed = Async::Queue.new
      late = 0
      call = task.async(finished: false) do
        core_client(transport, origin, :connect).client_stream(half_open_method, timeout: nil) do |writer|
          writer.send_message(message("first"))
          ready.enqueue(true)
          go.dequeue
          writer.send_message(message("late"))
          late += 1
        end
      end
      ready.dequeue
      operation = half_open_operation(transport)
      main = operation.fetch(:response).stream
      wait_until(task, "first DATA read") { peer.wire[:data][main.id].positive? }
      gate = wrap_client(transport, frame_type: Protocol::HTTP2::DataFrame::TYPE)
      gate.armed = true
      sibling = gated_sibling(task, transport, origin, gate, proceed)
      releases["/half/Stream"].enqueue({ protocol: :connect })
      wait_until(task, "response end read") { main.state != :open }
      go.enqueue(true)
      # Core recovered the status and the call reached cleanup, which joins
      # the output still waiting to write its reset.
      wait_until(task, "cleanup started while the reset waits") { operation[:cleaned] }
      expect(main).not_to be_closed
      gate.release.enqueue(true)
      expect { call.wait }.to expect_code("not_found")
      expect(late).to eq(0)
      proceed.enqueue(true)
      releases["/half/sibling"].enqueue({ protocol: :connect, code: 0 })
      sibling.wait
      expect_reuse(transport, origin, peer)
      expect(client_resets(peer, main.id)).to eq([Protocol::HTTP2::Error::NO_ERROR])
    end
  end

  it "reports the server's code from unread response DATA when the reset write fails" do
    with_half_open_peer do |task, transport, origin, peer, release|
      expect(transport.call(request("#{origin}/warm")).body).to eq("ok")
      resource = transport.instance_variable_get(:@resources).first
      failure = wrap_client(transport, HalfOpenPeer::ResetFailure)
      ready = Async::Queue.new
      go = Async::Queue.new
      late = 0
      call = task.async(finished: false) do
        core_client(transport, origin, :connect).client_stream(half_open_method, timeout: nil) do |writer|
          writer.send_message(message("first"))
          ready.enqueue(true)
          go.dequeue
          writer.send_message(message("late"))
          late += 1
        end
      end
      ready.dequeue
      failure.armed = true
      release.enqueue({ protocol: :connect })
      wait_until(task, "failed reset retired the connection") { resource.finished? }
      go.enqueue(true)
      expect { call.wait }.to expect_code("not_found")
      expect(late).to eq(0)
      expect(transport.call(request("#{origin}/after")).body).to eq("ok")
      expect(peer.accepted).to eq(2)
    end
  end
end
