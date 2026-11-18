# frozen_string_literal: true

RSpec.describe ConnectRPC::Hyper::Transport do
  def os_threads = Dir.children("/proc/self/task").size

  it "advertises every shape over HTTP/2 and Connect server and client streaming over HTTP/1.1" do
    expect(hyper.streaming_capabilities).to eq(%i[server_stream client_stream bidi_stream])
    expect(hyper.grpc_capabilities).to eq(%i[unary server_stream client_stream bidi_stream])
    expect(hyper(http2: false).streaming_capabilities).to eq(%i[server_stream client_stream])
    expect(hyper(http2: false).grpc_capabilities).to eq([])
  end

  it "starts no thread or socket at construction, even with TLS options; the first call starts the runtime" do
    options = tls_options(mtls: true)
    output, status = ruby_child(<<~RUBY)
      threads = -> { Dir.children("/proc/self/task").size }
      sockets = -> { Dir.children("/proc/self/fd").count { |fd| File.readlink("/proc/self/fd/\#{fd}").start_with?("socket:") rescue false } }
      before = [threads.call, sockets.call]
      transport = ConnectRPC::Hyper::Transport.new(keepalive: ConnectRPC::Keepalive.new(time: 1), **#{options.inspect})
      constructed = [threads.call, sockets.call]
      client(#{peer['grpc_mtls'].dump}, transport).unary(MSG.new(value: "x"), timeout: 5)
      names = Dir.children("/proc/self/task").map { |task| File.read("/proc/self/task/\#{task}/comm").chomp }
      puts [before, constructed, names.tally.fetch("hyper-runtime", 0), threads.call - before.first].inspect
      puts transport.inspect
    RUBY
    expect(status).to be_success
    before, constructed, runtime, added = eval(output.lines.first) # rubocop:disable Security/Eval
    expect(constructed).to eq(before)
    # Two workers, plus a blocking-pool thread while the trust store loads.
    expect(runtime).to be_between(2, 3)
    expect(added).to eq(runtime)
    expect(output.lines.last.chomp).to eq("#<ConnectRPC::Hyper::Transport connections=pooled http2=true tls=true " \
                                          "proxy=environment state=open>")
    expect(output).not_to include(peer.certs)
  end

  [
    [{ connections: :shared }, "connections must be :pooled or :per_call"],
    [{ idle_timeout: 0 }, "idle_timeout must be finite positive seconds or nil"],
    [{ idle_timeout: Float::INFINITY }, "idle_timeout must be finite positive seconds or nil"],
    [{ connect_timeout: -1 }, "connect_timeout must be finite positive seconds or nil"],
    [{ connect_timeout: "20" }, "connect_timeout must be finite positive seconds or nil"],
    [{ keepalive: 60 }, "keepalive must be a Keepalive or nil"],
    [{ http2: nil }, "http2 must be true or false"]
  ].each do |options, message|
    it "rejects #{options.inspect}" do
      expect { described_class.new(**options) }.to raise_error(ArgumentError, message)
    end
  end

  it "accepts nil idle and connect timeouts and a Keepalive" do
    expect { hyper(idle_timeout: nil, connect_timeout: nil, keepalive: ConnectRPC::Keepalive.new(time: 5)) }
      .not_to raise_error
  end

  it "requires a block for streams and validates close grace" do
    transport = hyper
    request = ConnectRPC::StreamRequest.new(url: URI("http://127.0.0.1:1/a/B"), http_method: "POST", headers: {})
    expect { transport.open_stream(request) }.to raise_error(ArgumentError, "A streaming block is required")
    expect { transport.open_grpc_stream(request) }.to raise_error(ArgumentError, "A gRPC block is required")
    expect { transport.close(grace: -1) }.to raise_error(ArgumentError, "grace must be finite nonnegative seconds")
  end

  it "keeps native internals private" do
    expect(ConnectRPC::Hyper.constants.sort).to eq(%i[Transport VERSION])
  end
end
