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

  # h2 refuses a chunk this large outright, which would drop the request
  # body while the response waits for it; the bytesize stands in for 2 GiB.
  it "rejects a write of 2 GiB or more before queuing it" do
    from = peer.mark
    exchange("ok") do |call|
      big = +"x"
      big.define_singleton_method(:bytesize) { 1 << 31 }
      expect { call.write(big) }.to raise_error(ArgumentError, "A streaming write must be smaller than 2 GiB")
    end
    expect(peer.since(from).grep(/\Araw request /)).to eq([])
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

  # A stream the server ends with END_STREAM and no reset stays half-open:
  # hyper's pipe keeps sending, as connectrpc-async does (a documented
  # limitation; grpc-go's client resets such a stream itself).
  it "keeps accepting writes on a half-open stream, then reads the status" do
    from = peer.mark
    exchange("end-on-data:status:5:open") do |call|
      call.write(envelope)
      peer.wait_event(/\Araw ended end-on-data:status:5:open /, from: from)
      # Reading to EOF first: the client has then processed END_STREAM.
      expect(call.response_head(timeout: 5).status).to eq(200)
      expect(read_all(call)).to eq(["".b, nil])
      expect(call.response_trailers["grpc-status"]).to eq(["5"])
      20.times { call.write(envelope) }
      call.close_send
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

    def h1_exchange(server, headers: {}, &)
      request = request("h1", grpc: false, origin: "http://127.0.0.1:#{server.addr[1]}")
      request.headers.merge!(headers)
      hyper(http2: false).open_stream(request, &)
    end

    after do
      @server&.close
      @served&.join(5)
    end

    def connection_closed = ["unavailable", "HTTP transport failed: connection closed"]

    # hyper's HTTP/1.1 body channel hands its error out once, so whichever
    # operation consumes it keeps it for the next.
    it "keeps a body error a write consumed for the next read" do
      @server, @served = h1_server { |socket| truncated(socket) }
      h1_exchange(@server) do |call|
        expect(outcome(write_until_raised(call))).to eq(connection_closed)
        expect(call.response_head(timeout: 5).status).to eq(200)
        expect(outcome(read_all(call).last)).to eq(connection_closed)
      end
    end

    it "keeps a body error a read consumed for the next write" do
      @server, @served = h1_server { |socket| truncated(socket) }
      h1_exchange(@server) do |call|
        call.write(envelope)
        expect(call.response_head(timeout: 5).status).to eq(200)
        expect(outcome(read_all(call).last)).to eq(connection_closed)
        expect(outcome(write_until_raised(call))).to eq(connection_closed)
      end
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
        socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, [1, 0].pack("ii"))
        socket.close
      end
      h1_exchange(@server) do |call|
        expect(outcome(write_until_raised(call))).to eq(connection_closed)
        expect(outcome(error_of { call.response_head(timeout: 5) })).to eq(connection_closed)
      end
    end
  end
end
