# frozen_string_literal: true

# Writes after the server ended the stream (#193). The raw peer ends each
# end-on-data stream at the request's first DATA frame, while the client is
# still sending. hyper drops the request body once the stream closed, so a
# write that starts after that raises; the write in flight then may still
# succeed. Each example writes until a write raises, bounded by the call
# deadline, so a write that never raises fails its assertion with
# deadline_exceeded.
RSpec.describe "Streaming writes after the stream ended", :interop do
  def reset = ["canceled", "HTTP/2 stream reset"]
  def closed = ["unavailable", "HTTP transport failed: stream closed"]
  # Message{value: "reply"} in the raw peer's 5-byte envelope.
  def reply = [0, 7].pack("CN") + MSG.encode(MSG.new(value: "reply"))
  # Far above the writes any stream here takes to end.
  def writes_cap = 10_000

  def envelope = [0, 0].pack("CN")

  def request(scenario, grpc: true, origin: peer["raw"])
    headers = { "content-type" => [grpc ? "application/grpc" : "application/connect+proto"],
                "x-scenario" => [scenario] }
    headers["te"] = ["trailers"] if grpc
    ConnectRPC::StreamRequest.new(url: URI("#{origin}/raw.v1.Raw/Upload"), http_method: "POST", headers: headers,
                                  deadline: ConnectRPC::Deadline.new(5))
  end

  def exchange(scenario, grpc: true, transport: hyper, &)
    if grpc
      transport.open_grpc_stream(request(scenario), &)
    else
      transport.open_stream(request(scenario, grpc: false), &)
    end
  end

  # The first error a write raised, writing until one does.
  def write_until_raised(exchange)
    loop { exchange.write(envelope) }
  rescue ConnectRPC::Error => e
    e
  end

  def outcome(error) = error && [error.code, error.message]

  # The response bytes up to EOF, then nil; or up to a failure, then it.
  def read_all(exchange)
    bytes = +"".b
    while (chunk = exchange.read(64, timeout: 5))
      bytes << chunk
    end
    [bytes, nil]
  rescue ConnectRPC::Error => e
    [bytes, e]
  end

  # The scenario's request stream id and client address, from its peer event.
  def request_of(scenario, from)
    event = peer.wait_event(/\Araw request #{Regexp.escape(scenario)} /, from: from)
    [event[/ stream=(\d+) /, 1], event[/ conn=(\S+)\z/, 1]]
  end

  def reset_pattern(stream, code: /\d+/)
    /\Araw rst stream=#{stream[0]} code=(#{code}) conn=#{Regexp.escape(stream[1])}\z/
  end

  # Waits for the client's RST_STREAM(NO_ERROR) on the request's stream,
  # within the call's 5 s deadline when the call is still open.
  def wait_reset(stream, from) = peer.wait_event(reset_pattern(stream, code: 0), from: from, timeout: 4)

  # The codes of every client RST_STREAM on the request's stream, in order.
  def reset_codes(stream, from) = peer.since(from).filter_map { it[reset_pattern(stream), 1] }

  # Runs an ok call on `transport` and expects it on the request's
  # connection: the peer accepted no other since `from`.
  def expect_reused(transport, stream, from)
    mark = peer.mark
    exchange("ok", transport: transport) do |call|
      call.close_send
      read_all(call)
      call.finish(timeout: 5)
    end
    expect([request_of("ok", mark)[1], peer.count(/\Araw accept /, from: from)]).to eq([stream[1], 1])
  end

  describe "an exchange (gRPC)" do
    it "raises a reset before the head as the reset, which the head then reports" do
      exchange("end-on-data:reset:8") do |call|
        error = write_until_raised(call)
        expect([*outcome(error), error.cause&.reset_code]).to eq([*reset, 8])
        expect(outcome(error_of { call.response_head(timeout: 5) })).to eq(reset)
      end
    end

    it "raises a reset after the head as the reset, which the body then reports" do
      exchange("end-on-data:head-reset:8") do |call|
        expect(outcome(write_until_raised(call))).to eq(reset)
        expect(call.response_head(timeout: 5).status).to eq(200)
        expect(outcome(read_all(call).last)).to eq(reset)
      end
    end

    it "raises stream closed after a status and RST_STREAM(NO_ERROR), then reads the status" do
      exchange("end-on-data:status:5") do |call|
        expect(outcome(write_until_raised(call))).to eq(closed)
        expect(call.response_head(timeout: 5).status).to eq(200)
        expect(read_all(call)).to eq(["".b, nil])
        expect(call.response_trailers["grpc-status"]).to eq(["5"])
      end
    end

    it "raises stream closed after a reply, then reads the reply and its status" do
      exchange("end-on-data:reply:0") do |call|
        expect(outcome(write_until_raised(call))).to eq(closed)
        call.response_head(timeout: 5)
        expect(read_all(call)).to eq([reply, nil])
        expect(call.response_trailers["grpc-status"]).to eq(["0"])
      end
    end

    # hyper's send side never learns the reset's code, and the response body
    # holds unread DATA ahead of the reset, so the write raises the generic
    # phrase; the body still reports the reset after the message.
    it "raises stream closed for a reset behind unread response DATA, which the body then reports" do
      exchange("end-on-data:data-reset:8") do |call|
        expect(outcome(write_until_raised(call))).to eq(closed)
        call.response_head(timeout: 5)
        bytes, error = read_all(call)
        expect([bytes, outcome(error)]).to eq([reply, reset])
      end
    end
  end

  describe "an exchange (Connect)" do
    it "raises a reset as the stream reset phrase" do
      exchange("end-on-data:reset:8", grpc: false) do |call|
        expect(outcome(write_until_raised(call))).to eq(["unavailable", "HTTP transport failed: stream reset"])
      end
    end

    it "raises stream closed after an end-stream message, then reads it" do
      exchange("end-on-data:status:5", grpc: false) do |call|
        expect(outcome(write_until_raised(call))).to eq(closed)
        expect(call.response_head(timeout: 5).status).to eq(200)
        bytes, error = read_all(call)
        end_stream = '{"error":{"code":"not_found","message":"ended"}}'
        expect([bytes.getbyte(0), bytes.byteslice(5..), error]).to eq([2, end_stream, nil])
      end
    end
  end

  describe "core's client stream" do
    def upload(scenario, protocol:)
      writes = 0
      metadata = { "x-scenario" => scenario }
      error = error_of do
        core(peer["raw"], transport: hyper, protocol: protocol)
          .client_stream(HyperHelpers::RAW_CLIENT_STREAM, metadata: metadata, timeout: 5) do |writer|
          writes_cap.times do
            writer.send_message(MSG.new)
            writes += 1
          end
        end
      end
      [outcome(error), writes]
    end

    %i[grpc connect].each do |protocol|
      it "stops writing and reports the server's status (#{protocol})" do
        result, writes = upload("end-on-data:status:5", protocol: protocol)
        expect(result).to eq(%w[not_found ended])
        expect(writes).to be < writes_cap / 10
      end

      # The baseline reports the status too, after every write: the bounded
      # write count and the client's reset prove the stream ended.
      it "stops writing on a half-open stream, resets it and reports the server's status (#{protocol})" do
        from = peer.mark
        result, writes = upload("end-on-data:status:5:open", protocol: protocol)
        expect(result).to eq(%w[not_found ended])
        expect(writes).to be < writes_cap / 10
        stream = request_of("end-on-data:status:5:open", from)
        wait_reset(stream, from)
        expect(reset_codes(stream, from)).to eq(["0"])
      end
    end

    %w[reset head-reset].each do |mode|
      it "stops writing and reports a gRPC #{mode} as its code" do
        result, writes = upload("end-on-data:#{mode}:8", protocol: :grpc)
        expect(result).to eq(reset)
        expect(writes).to be < writes_cap / 10
      end
    end
  end

  # A write that finds the body dropped waits for the response's outcome,
  # which `respond` publishes; the hold point parks that publication.
  describe "waiting for the response's outcome" do
    def parked_writer(transport, native_driver)
      native_driver.hold("respond:publish", 10)
      native_driver.hold("write:outcome", 0)
      writer = Thread.new do
        Thread.current.report_on_exception = false
        exchange("end-on-data:reset:8", transport: transport) { |call| write_until_raised(call) }
      end
      wait_until("the reset's outcome parked before publication") { native_driver.reached("respond:publish") == 1 }
      wait_until("the write waiting for the outcome") { native_driver.reached("write:outcome") == 1 }
      wait_parked(writer, "Exchange#write")
      writer
    end

    def release_points(native_driver) = %w[respond:publish write:outcome].each { native_driver&.release(it) }

    it "parks the write until the outcome is published, then raises it" do
      transport = hyper
      native_driver = driver(transport)
      writer = parked_writer(transport, native_driver)
      native_driver.release("respond:publish")
      expect(outcome(writer.value)).to eq(reset)
    ensure
      release_points(native_driver)
    end

    it "lands a Thread#raise on the parked write promptly" do
      transport = hyper
      native_driver = driver(transport)
      writer = parked_writer(transport, native_driver)
      sent = monotonic
      writer.raise(RuntimeError, "interrupted")
      error = begin
        writer.value
      rescue RuntimeError => e
        e
      end
      expect([error.class, error.message, monotonic - sent < 1]).to eq([RuntimeError, "interrupted", true])
    ensure
      release_points(native_driver)
    end
  end

  # h2 refuses a chunk of 2 GiB or more outright, which would drop the
  # request body while the response waits for it, so the extension rejects
  # such a write before copying or queuing it, and the stream goes on.
  # `check_write_len`'s Rust unit tests (lib.rs) pin both sides of the limit.
  it "rejects a write of 2 GiB or more before queuing it" do
    # A String's length is a C long, 32 bits on Windows, where no string
    # reaches the limit.
    skip "a String cannot reach 2 GiB where a long is 32 bits" if [0].pack("l!").bytesize < 8
    # On Linux the zero-filled string took about 1 ms and no resident memory,
    # as its pages stay untouched. A host that cannot reserve the address
    # space skips.
    big = begin
      "\0".b * (1 << 31)
    rescue NoMemoryError
      skip "this host cannot reserve a 2 GiB string"
    end
    from = peer.mark
    transport = hyper
    native_driver = driver(transport)
    # Armed with a zero bound, the point counts the requests handed to hyper:
    # the rejected write must not even send the request head.
    native_driver.hold("request:queued", 0)
    exchange("await-ok:big", transport: transport) do |call|
      expect { call.write(big) }.to raise_error(ArgumentError, "A streaming write must be smaller than 2 GiB")
      expect(native_driver.reached("request:queued")).to eq(0)
      expect(call.write(envelope)).to be_nil
      expect(native_driver.reached("request:queued")).to eq(1)
      call.close_send
      expect(call.response_head(timeout: 5).status).to eq(200)
    end
    peer.wait_event(/\Araw handled await-ok:big stream=\d+ bytes=5\z/, from: from)
  ensure
    native_driver&.release("request:queued")
    big = nil
  end

  # The check reads the string's own length, so a String subclass whose
  # sizes claim the limit still sends its bytes. Unlike the example above,
  # this one runs where a long is 32 bits too.
  it "judges a write by the string's own length, not its bytesize" do
    from = peer.mark
    lying = Class.new(String) { %i[bytesize length size].each { |name| define_method(name) { 1 << 31 } } }
    exchange("await-ok:own-length") do |call|
      expect(call.write(lying.new(envelope))).to be_nil
      call.close_send
      expect(call.response_head(timeout: 5).status).to eq(200)
    end
    peer.wait_event(/\Araw handled await-ok:own-length stream=\d+ bytes=5\z/, from: from)
  end

  # Forced teardown fires `shutdown` before it aborts the connection that
  # drops the upload, so a write in flight after the whole response arrived
  # raises a forced phrase, never the generic one. The status waits behind
  # the unread message, so the client has not seen the end and the writes
  # go on. The hold parks teardown between the two; the write must already
  # have its forced phrase then.
  it "raises a forced phrase from a write in flight when forced teardown ends the stream" do
    transport = hyper
    native_driver = driver(transport)
    native_driver.hold("force:shutdown", 10)
    from = peer.mark
    writer = nil
    closer = nil
    exchange("end-on-data:reply:0:open", transport: transport) do |call|
      call.write(envelope)
      peer.wait_event(/\Araw ended end-on-data:reply:0:open /, from: from)
      call.response_head(timeout: 5)
      writer = Thread.new do
        Thread.current.report_on_exception = false
        write_until_raised(call)
      end
      wait_parked(writer, "Exchange#write")
      closer = Thread.new { transport.close(grace: 0) }
      wait_until("forced teardown parked after shutdown") { native_driver.reached("force:shutdown") == 1 }
      expect(outcome(writer.value)).to eq(["unavailable", "HTTP transport failed: connection closed"])
      native_driver.release("force:shutdown")
      expect(closer.value).to be(false)
    end
  ensure
    native_driver&.release("force:shutdown")
    closer&.join(10)
  end

  # The reader holds the receive half, so the write cannot look at the body:
  # it raises the generic phrase, or the reset once the reader returned it.
  it "raises while a reader is parked on the same stream, and the reader gets the reset" do
    tag = "busy-reader-#{rand(1 << 30)}"
    exchange("reset-after-on:#{tag}:8") do |call|
      call.write(envelope)
      call.response_head(timeout: 5)
      reader = Thread.new do
        call.read(64, timeout: 5)
      rescue StandardError => e
        e
      end
      wait_parked(reader, "Exchange#read")
      peer.command("release #{tag}")
      expect(outcome(write_until_raised(call))).to eq(closed).or eq(reset)
      read = reader.value
      expect([read.class, read.message]).to eq([ConnectRPC::Error, reset.last])
    end
  end

  # A server that ends its response (END_STREAM) while the upload is open,
  # without resetting the stream (#228). Once the transport observes the end,
  # the client resets the stream with RST_STREAM(NO_ERROR), as grpc-go's
  # client does, and later writes raise; the response stays readable.
  # Examples that count resets finish their call, so no cleanup CANCEL
  # follows the reset.
  describe "a response that ends while the upload is open" do
    # Larger than the stream's whole 64 KiB window: hyper hands h2 the first
    # write's bytes at once and holds the second's waiting for window, so
    # against a peer that never reopens the window exactly two writes
    # return, and the third waits after queueing until something ends it.
    def beyond_window = "\0".b * 131_072

    def grpc_end = ["".b, nil]

    def connect_end
      end_stream = '{"error":{"code":"not_found","message":"ended"}}'
      [[2, end_stream.bytesize].pack("CN") + end_stream, nil]
    end

    # A thread writing `chunk` until a write raises, and the count of writes
    # that returned.
    def writer(call, chunk)
      written = [0]
      thread = Thread.new do
        Thread.current.report_on_exception = false
        loop do
          call.write(chunk)
          written[0] += 1
        end
      rescue ConnectRPC::Error, RuntimeError => e
        e
      end
      [thread, written]
    end

    # Waits until the third write queued and waits for window.
    def wait_window_spent(thread, written)
      wait_until("two writes returned, the third parked") { written[0] == 2 }
      wait_parked(thread, "Exchange#write")
    end

    def script(tag, steps) = "script:#{tag}:#{steps}"
    def tag = @tag ||= "half-open-#{rand(1 << 30)}"

    { grpc: true, connect: false }.each do |protocol, grpc|
      it "resets the stream once a status ended it, then reads the status (#{protocol})" do
        transport = hyper
        from = peer.mark
        scenario = "end-on-data:status:5:open"
        stream = nil
        exchange(scenario, grpc: grpc, transport: transport) do |call|
          call.write(envelope)
          stream = request_of(scenario, from)
          peer.wait_event(/\Araw ended #{scenario} /, from: from)
          expect(outcome(write_until_raised(call))).to eq(closed)
          wait_reset(stream, from)
          expect(call.response_head(timeout: 5).status).to eq(200)
          expect(read_all(call)).to eq(grpc ? grpc_end : connect_end)
          expect(call.response_trailers["grpc-status"]).to eq(["5"]) if grpc
          call.finish(timeout: 5)
        end
        expect(reset_codes(stream, from)).to eq(["0"])
        expect_reused(transport, stream, from)
      end

      # The status waits behind the message until it is read.
      it "resets the stream once the message ahead of the status was read (#{protocol})" do
        transport = hyper
        from = peer.mark
        scenario = "end-on-data:reply:5:open"
        stream = nil
        exchange(scenario, grpc: grpc, transport: transport) do |call|
          call.write(envelope)
          stream = request_of(scenario, from)
          peer.wait_event(/\Araw ended #{scenario} /, from: from)
          call.response_head(timeout: 5)
          expect(call.read(64, timeout: 5)).to eq(reply)
          expect(outcome(write_until_raised(call))).to eq(closed)
          wait_reset(stream, from)
          expect(read_all(call)).to eq(grpc ? grpc_end : connect_end)
          expect(call.response_trailers["grpc-status"]).to eq(["5"]) if grpc
          call.finish(timeout: 5)
        end
        expect(reset_codes(stream, from)).to eq(["0"])
        expect_reused(transport, stream, from)
      end
    end

    it "resets the stream of a trailers-only response, then reads its head" do
      from = peer.mark
      scenario = "end-on-data:trailers-only:5:open"
      stream = nil
      exchange(scenario) do |call|
        call.write(envelope)
        stream = request_of(scenario, from)
        peer.wait_event(/\Araw ended #{scenario} /, from: from)
        expect(outcome(write_until_raised(call))).to eq(closed)
        wait_reset(stream, from)
        head = call.response_head(timeout: 5)
        expect([head.status, head.end_stream, head.headers["grpc-status"]]).to eq([200, true, ["5"]])
        expect(read_all(call)).to eq(grpc_end)
        call.finish(timeout: 5)
      end
      expect(reset_codes(stream, from)).to eq(["0"])
    end

    it "resets the stream when a reader reaches the end, with no write in flight" do
      from = peer.mark
      scenario = script(tag, "head,gate,status=5")
      stream = nil
      exchange(scenario) do |call|
        call.write(envelope)
        stream = request_of(scenario, from)
        peer.command("release #{tag}.1")
        expect(call.response_head(timeout: 5).status).to eq(200)
        expect(read_all(call)).to eq(grpc_end)
        wait_reset(stream, from)
        call.finish(timeout: 5)
      end
      expect(reset_codes(stream, from)).to eq(["0"])
    end

    # Nobody reads: the waiting write observes the end itself. hyper does
    # not poll the request body while it waits for window, so the reset goes
    # out only once the window opens (a documented limitation).
    it "returns a write queued when the response ends, then raises the next" do
      transport = hyper
      from = peer.mark
      scenario = script(tag, "noread,head,gate,status=5,gate,window")
      stream = nil
      exchange(scenario, transport: transport) do |call|
        thread, written = writer(call, beyond_window)
        wait_window_spent(thread, written)
        stream = request_of(scenario, from)
        peer.command("release #{tag}.1")
        expect([outcome(thread.value), written[0]]).to eq([closed, 3])
        expect(call.response_head(timeout: 5).status).to eq(200)
        expect(read_all(call)).to eq(grpc_end)
        call.finish(timeout: 5)
      end
      # After the call completed, the reset waits for the window too; a
      # later call on the connection is the ordering boundary.
      expect_reused(transport, stream, from)
      expect(reset_codes(stream, from)).to eq([])
      peer.command("release #{tag}.2")
      wait_reset(stream, from)
      expect(reset_codes(stream, from)).to eq(["0"])
    end

    # An interrupted write leaves its bytes queued, so the next write waits
    # to queue its own.
    it "raises a write waiting to queue when the response ends" do
      from = peer.mark
      scenario = script(tag, "noread,head,gate,status=5,gate,window")
      stream = nil
      exchange(scenario) do |call|
        thread, written = writer(call, beyond_window)
        wait_window_spent(thread, written)
        thread.raise(RuntimeError, "interrupted")
        expect(thread.value.message).to eq("interrupted")
        waiting = Thread.new do
          Thread.current.report_on_exception = false
          call.write(envelope)
        rescue ConnectRPC::Error => e
          e
        end
        wait_parked(waiting, "Exchange#write")
        stream = request_of(scenario, from)
        peer.command("release #{tag}.1")
        expect(outcome(waiting.value)).to eq(closed)
        peer.command("release #{tag}.2")
        wait_reset(stream, from)
        expect(call.response_head(timeout: 5).status).to eq(200)
        expect(read_all(call)).to eq(grpc_end)
        call.finish(timeout: 5)
      end
      expect(reset_codes(stream, from)).to eq(["0"])
    end

    # A reader that took the receive half may take the waiting write's wake
    # with it; returning the half must wake the write to look again.
    {
      "the head" => ["noread,gate,head,gate,status=5", "Exchange#response_head"],
      "a message" => ["noread,head,gate,reply,gate,status=5", "Exchange#read"]
    }.each do |what, (steps, frame)|
      it "notices the end that arrives after a reader returned with #{what}" do
        from = peer.mark
        scenario = script(tag, steps)
        message = frame == "Exchange#read"
        exchange(scenario) do |call|
          thread, written = writer(call, beyond_window)
          wait_window_spent(thread, written)
          reader = Thread.new { message ? call.read(64, timeout: 5) : call.response_head(timeout: 5).status }
          wait_parked(reader, frame)
          peer.command("release #{tag}.1")
          expect(reader.value).to eq(message ? reply : 200)
          peer.command("release #{tag}.2")
          peer.wait_event(/\Araw script #{tag} status=5\z/, from: from)
          expect([outcome(thread.value), written[0]]).to eq([closed, 3])
        end
      end
    end

    # The client's own end of the upload commits a clean END_STREAM: the
    # response ends after close_send, and once the window opens the queued
    # bytes and END_STREAM go out, with no reset.
    it "ends the upload cleanly when the writer closed its side first" do
      transport = hyper
      from = peer.mark
      scenario = script(tag, "noread,head,gate,status=5,gate,window")
      stream = nil
      exchange(scenario, transport: transport) do |call|
        thread, written = writer(call, beyond_window)
        wait_window_spent(thread, written)
        thread.raise(RuntimeError, "interrupted")
        expect(thread.value.message).to eq("interrupted")
        call.close_send
        stream = request_of(scenario, from)
        peer.command("release #{tag}.1")
        expect(call.response_head(timeout: 5).status).to eq(200)
        expect(read_all(call)).to eq(grpc_end)
        peer.command("release #{tag}.2")
        request_end = /\Araw request-end stream=#{stream[0]} conn=#{Regexp.escape(stream[1])}\z/
        peer.wait_event(request_end, from: from, timeout: 4)
        call.finish(timeout: 5)
      end
      expect_reused(transport, stream, from)
      expect(reset_codes(stream, from)).to eq([])
    end
  end

  describe "over HTTP/1.1" do
    # One connection: reads the request head, then runs the block with the
    # socket, then reads the upload until the client goes.
    def h1_server
      server = TCPServer.new("127.0.0.1", 0)
      served = Thread.new do
        socket = server.accept
        while (line = socket.gets("\r\n")) && line != "\r\n"; end
        yield socket
        loop { socket.readpartial(65_536) }
      rescue IOError, SystemCallError
        nil
      ensure
        socket&.close
      end
      [server, served]
    end

    # A chunked head, then end of response without a chunk: the client's
    # body fails, while the server still reads the upload.
    def truncated(socket)
      socket.write("HTTP/1.1 200 OK\r\ncontent-type: application/connect+proto\r\ntransfer-encoding: chunked\r\n\r\n")
      socket.close_write
    end

    # A chunked head, then a malformed chunk size: the client's body fails
    # with a phrase no dropped upload gives.
    def malformed(socket)
      socket.write("HTTP/1.1 200 OK\r\ncontent-type: application/connect+proto\r\ntransfer-encoding: chunked\r\n\r\n" \
                   "zz\r\n")
    end

    def malformed_response = ["unavailable", "HTTP transport failed: malformed response"]

    # Answers once the first upload chunk arrived, so the call's first write,
    # which dispatches the request, has returned before the response ends it.
    def answer_after_upload(socket)
      socket.readpartial(65_536)
      yield
    end

    def h1_exchange(server, headers: {}, transport: hyper(http2: false), &)
      request = request("h1", grpc: false, origin: "http://127.0.0.1:#{server.addr[1]}")
      request.headers.merge!(headers)
      transport.open_stream(request, &)
    end

    # Writes once, waits until the response task published the response
    # head, then writes until one raises: the
    # write that finds the upload dropped then finds the head and the body
    # error behind it.
    def write_after_head(call, native_driver)
      call.write(envelope)
      wait_until("the response head") { native_driver.reached("respond:published") == 1 }
      write_until_raised(call)
    end

    after do
      @server&.close
      @served&.join(5)
    end

    def connection_closed = ["unavailable", "HTTP transport failed: connection closed"]

    # hyper's HTTP/1.1 body channel hands its error out once, so whichever
    # operation consumes it keeps it for the next.
    it "keeps a body error a write consumed for the next read" do
      @server, @served = h1_server { |socket| answer_after_upload(socket) { malformed(socket) } }
      transport = hyper(http2: false)
      native_driver = driver(transport)
      native_driver.hold("respond:published", 0)
      h1_exchange(@server, transport: transport) do |call|
        expect(outcome(write_after_head(call, native_driver))).to eq(malformed_response)
        expect(call.response_head(timeout: 5).status).to eq(200)
        expect(outcome(read_all(call).last)).to eq(malformed_response)
      end
    ensure
      native_driver&.release("respond:published")
    end

    it "keeps a body error a read consumed for the next write" do
      @server, @served = h1_server { |socket| answer_after_upload(socket) { malformed(socket) } }
      h1_exchange(@server) do |call|
        call.write(envelope)
        expect(call.response_head(timeout: 5).status).to eq(200)
        expect(outcome(read_all(call).last)).to eq(malformed_response)
        expect(outcome(write_until_raised(call))).to eq(malformed_response)
      end
    end

    it "raises the body error of a response that ended without a chunk" do
      @server, @served = h1_server { |socket| answer_after_upload(socket) { truncated(socket) } }
      transport = hyper(http2: false)
      native_driver = driver(transport)
      native_driver.hold("respond:published", 0)
      h1_exchange(@server, transport: transport) do |call|
        expect(outcome(write_after_head(call, native_driver))).to eq(connection_closed)
        expect(call.response_head(timeout: 5).status).to eq(200)
        expect(outcome(read_all(call).last)).to eq(connection_closed)
      end
    ensure
      native_driver&.release("respond:published")
    end

    # hyper drops a body that reached its declared Content-Length while the
    # response is pending, so the write raises without waiting for it, with
    # the phrase HTTP/1.1 gives every body it dropped before the response
    # reported a failure. A length of 0 is a body hyper never polls.
    it "raises a write past the declared Content-Length without waiting for the response" do
      answered = Queue.new
      @server, @served = h1_server { |_socket| answered.pop }
      h1_exchange(@server, headers: { "content-length" => ["5"] }) do |call|
        call.write(envelope)
        expect(outcome(write_until_raised(call))).to eq(connection_closed)
      end
    ensure
      answered << :done
    end

    it "raises a write to a declared Content-Length of 0 without waiting for the response" do
      answered = Queue.new
      @server, @served = h1_server { |_socket| answered.pop }
      h1_exchange(@server, headers: { "content-length" => ["0"] }) do |call|
        expect(outcome(write_until_raised(call))).to eq(connection_closed)
      end
    ensure
      answered << :done
    end

    # After a keep-alive HTTP/1.0 response hyper sends the next request on
    # that socket as HTTP/1.0 and drops an unsized upload body at once, while
    # the response is pending: the write raises without waiting for it.
    it "raises a write on a pooled HTTP/1.0 connection without waiting for the response" do
      answered = Queue.new
      @server, @served = h1_server do |socket|
        socket.write("HTTP/1.0 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
        while (line = socket.gets("\r\n")) && line != "\r\n"; end
        answered.pop
      end
      transport = hyper(http2: false)
      url = URI("http://127.0.0.1:#{@server.addr[1]}/raw.v1.Raw/Upload")
      unary = ConnectRPC::TransportRequest.new(url: url, http_method: "GET", headers: {}, response_limit: 1024,
                                               deadline: ConnectRPC::Deadline.new(5))
      expect(transport.call(unary).status).to eq(200)
      request = request("h1", grpc: false, origin: "http://127.0.0.1:#{@server.addr[1]}")
      transport.open_stream(request) do |call|
        expect(outcome(write_until_raised(call))).to eq(connection_closed)
      end
    ensure
      answered << :done
    end

    # HTTP/1.1 never waits for the response's outcome; the write raises the
    # connection's failure whether or not the response task published it.
    it "raises the connection's failure when the connection resets mid-upload" do
      @server, @served = h1_server do |socket|
        socket.readpartial(65_536)
        socket.setsockopt(Socket::Option.linger(true, 0))
        socket.close
      end
      h1_exchange(@server) do |call|
        expect(outcome(write_until_raised(call))).to eq(connection_closed)
        expect(outcome(error_of { call.response_head(timeout: 5) })).to eq(connection_closed)
      end
    end
  end
end
