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
