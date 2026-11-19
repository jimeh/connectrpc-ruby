# frozen_string_literal: true

RSpec.describe "Transport failures" do
  def failure(origin, protocol: :grpc, transport: hyper, value: "x", timeout: 5)
    error_of { client(origin, transport: transport, protocol: protocol).unary(MSG.new(value: value), timeout: timeout) }
  end

  it "uses fixed phrases with the native failure as cause" do
    refused = TCPServer.new("127.0.0.1", 0).then { |server| server.addr[1].tap { server.close } }
    error = failure("http://127.0.0.1:#{refused}")
    expect([error.code, error.message]).to eq(["unavailable", "HTTP transport failed: connection refused"])
    expect([error.cause.class.name, error.cause.kind, error.cause.errno])
      .to eq(["ConnectRPC::Hyper::Native::Failure", :refused, Errno::ECONNREFUSED::Errno])

    error = failure(peer["grpc_tls"])
    expect([error.code, error.message]).to eq(["unavailable", "HTTP transport failed: TLS verification failed"])

    closing = TCPServer.new("127.0.0.1", 0)
    closer = Thread.new { loop { closing.accept.close } }
    error = failure("http://127.0.0.1:#{closing.addr[1]}", protocol: :connect)
    expect([error.code, error.message]).to eq(["unavailable", "HTTP transport failed: connection closed"])

    error = failure(peer["grpc_h2c"], value: "hang", timeout: 0.2)
    expect([error.code, error.message, error.cause]).to eq(["deadline_exceeded", "Deadline exceeded", nil])
  ensure
    closer&.kill
    closing&.close
  end

  it "bounds TCP and TLS setup by connect_timeout, well inside the call deadline", :linux_only do
    port, cleanup = full_backlog
    silent = TCPServer.new("127.0.0.1", 0)
    acceptor = Thread.new { loop { silent.accept } }
    [["http://127.0.0.1:#{port}", {}], ["https://127.0.0.1:#{silent.addr[1]}", tls_options]].each do |origin, tls|
      started = monotonic
      error = failure(origin, transport: hyper(connect_timeout: 0.3, **tls), timeout: 10)
      expect([error.code, error.message]).to eq(["unavailable", "HTTP transport failed: timed out"])
      expect(error.cause.kind).to eq(:connect_timeout)
      expect(monotonic - started).to be_between(0.29, 3)
    end
  ensure
    cleanup&.call
    acceptor&.kill
    silent&.close
  end

  # The shim resolves peer.pair to 127.0.0.2 (a full backlog on the peer's
  # port drops its SYNs), then 127.0.0.1 (the peer). A per-call dial is the
  # call's own, so Go's partialDeadline splits the earlier of connect_timeout
  # and the call deadline, giving the first address max(3 s / 2, 2 s) = 2 s
  # and leaving 1 s for the peer. (A pool dial splits connect_timeout alone;
  # see the pooling specs.)
  it "gives each address of an unshared dial its share of the earlier of connect_timeout and the deadline",
     :dns_shim do
    environment = stall_environment
    port = URI(peer["grpc_h2c"]).port
    blackhole = Socket.new(:INET, :STREAM)
    blackhole.bind(Addrinfo.tcp("127.0.0.2", port))
    blackhole.listen(0)
    fillers = Array.new(8) do
      socket = Socket.new(:INET, :STREAM)
      socket.connect_nonblock(Addrinfo.tcp("127.0.0.2", port), exception: false)
      socket
    end
    # [connect_timeout, call deadline]
    budgets = [[3, 10], [20, 3]]
    output, status = ruby_child(<<~RUBY, env: environment)
      results = #{budgets.inspect}.map do |connect_timeout, timeout|
        transport = ConnectRPC::Hyper::Transport.new(connections: :per_call, connect_timeout: connect_timeout,
                                                     proxy: nil)
        started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        reply = begin
          client("http://peer.pair:#{port}", transport).unary(MSG.new(value: "second"), timeout: timeout).message.value
        rescue ConnectRPC::Error => e
          [e.code, e.message]
        end
        transport.close(grace: 0)
        [reply, (Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).round(2)]
      end
      p results
    RUBY
    expect(status).to be_success
    results = eval(output.lines.last) # rubocop:disable Security/Eval
    expect(results.map(&:first)).to eq(Array.new(budgets.size, "second"))
    expect(results.map(&:last)).to all(be_between(1.9, 2.9))
  ensure
    fillers&.each(&:close)
    blackhole&.close
  end

  it "records a per-wait timeout as a lifecycle failure before its fixed phrase" do
    recorded = []
    lifecycle = Object.new
    lifecycle.define_singleton_method(:record_failure) { |error| recorded << error }
    request = ConnectRPC::StreamRequest.new(url: URI("#{peer['grpc_h2c']}/connectrpc.grpcinterop.v1.Peer/Bidi"),
                                            http_method: "POST", headers: { "content-type" => ["application/grpc"] },
                                            deadline: ConnectRPC::Deadline.new(10), lifecycle: lifecycle)
    error = error_of { hyper.open_grpc_stream(request) { |exchange| exchange.response_head(timeout: 0.2) } }
    expect([error.code, error.message, error.cause]).to eq(["unavailable", "HTTP transport failed: timed out", nil])
    expect(recorded.map { |failure| [failure.class.name, failure.kind] })
      .to eq([["ConnectRPC::Hyper::Native::Failure", :timeout]])
  end

  it "accepts an HTTP/1.1 response head larger than hyper's defaults, up to Go's 10 MiB bound" do
    big = Array.new(300) { |index| ["x-big-#{index}", "v" * 4096] }.to_h
    server = TinyHTTP.new { |_request, socket| TinyHTTP.ok(socket, headers: big) }
    request = ConnectRPC::TransportRequest.new(url: URI("#{server.origin}/svc/Method"), http_method: "POST",
                                               headers: { "content-type" => ["application/proto"] }, body: "",
                                               response_limit: 1024, deadline: ConnectRPC::Deadline.new(5))
    response = hyper(http2: false).call(request)
    expect(response.status).to eq(200)
    expect(response.headers.keys.grep(/\Ax-big-/).size).to eq(300)
    expect(response.headers["x-big-299"]).to eq(["v" * 4096])
  ensure
    server&.close
  end

  # hyper caps chunked trailer fields at max_headers (1,024 here): the
  # decoder rejects the 1,025th line with an InvalidData I/O error.
  it "accepts HTTP/1.1 chunked trailers up to 1,024 fields and reports more as a malformed response" do
    count = 0
    server = TinyHTTP.new do |_request, socket|
      trailers = Array.new(count) { |index| "x-trailer-#{index}: v\r\n" }.join
      socket.write("HTTP/1.1 200 OK\r\ncontent-type: application/proto\r\ntransfer-encoding: chunked\r\n\r\n" \
                   "0\r\n#{trailers}\r\n")
    end
    transport = hyper(http2: false)
    request = lambda do
      ConnectRPC::TransportRequest.new(url: URI("#{server.origin}/svc/Method"), http_method: "POST",
                                       headers: { "content-type" => ["application/proto"] }, body: "",
                                       response_limit: 1024, deadline: ConnectRPC::Deadline.new(5))
    end
    count = 1024
    expect(transport.call(request.call).status).to eq(200)
    count = 1025
    error = error_of { transport.call(request.call) }
    expect([error.code, error.message, error.cause.kind])
      .to eq(["unavailable", "HTTP transport failed: malformed response", :protocol])
  ensure
    server&.close
  end

  it "reports name resolution failures", :dns_shim do
    environment = stall_environment
    output, status = ruby_child(<<~RUBY, env: environment)
      events = File.open(ENV.fetch("STALL_EVENTS"), "r+")
      thread = Thread.new do
        client("http://peer.stall:1", ConnectRPC::Hyper::Transport.new).unary(MSG.new, timeout: 5)
      rescue ConnectRPC::Error => e
        [e.code, e.message, e.cause.kind]
      end
      events.gets
      File.write(ENV.fetch("STALL_RELEASE"), "x")
      p thread.value
    RUBY
    expect(status).to be_success
    expect(output.chomp).to eq('["unavailable", "HTTP transport failed: name resolution failed", :dns]')
  end

  describe "server RST_STREAM", :interop do
    { 3 => "resource_exhausted", 7 => "unavailable", 8 => "canceled", 11 => "resource_exhausted",
      12 => "permission_denied", 2 => "internal", 1 => "internal" }.each do |code, mapped|
      it "maps gRPC reset code #{code} to #{mapped}" do
        error = error_of do
          metadata = { "x-scenario" => "reset-after:#{code}" }
          core(peer["raw"], transport: hyper)
            .server_stream(HyperHelpers::RAW_SERVER_STREAM, MSG.new, metadata: metadata,
                                                                     timeout: 5) { |reader| values(reader) }
        end
        expect([error.code, error.message, error.cause&.reset_code]).to eq([mapped, "HTTP/2 stream reset", code])
      end
    end

    it "reports a Connect reset as unavailable" do
      error = error_of { raw_call(peer["raw"], "reset-after:8", transport: hyper, protocol: :connect) }
      expect([error.code, error.message]).to eq(["unavailable", "HTTP transport failed: stream reset"])
    end
  end
end
