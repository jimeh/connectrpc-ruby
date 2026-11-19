# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../../../spec/fixtures/generation/ruby", __dir__))
require "generation/streaming_connect"
require_relative "support/threaded_stream_helpers"
require_relative "support/per_call_helpers"

RSpec.describe "PerCallTransport calls, origins and validation" do
  include ThreadedStreamHelpers
  include PerCallHelpers

  # A Connect and gRPC peer for unary, GET and server streaming that counts
  # raw accepts.
  def mixed_host(**)
    threaded_host(server: mixed_server(watch: lambda { |message, writer, _|
      2.times { |index| writer.send_message(item("#{message.name}-#{index}")) }
    }), **)
  end

  # Server streams return nil; the names come from the reader.
  def streamed(typed, name)
    names = []
    typed.watch(item(name)) { |reader| reader.each { |message| names << message.name } }
    names
  end

  it "validates options at construction and holds only frozen configuration" do
    expect { per_call(connect_timeout: 0) }
      .to raise_error(ArgumentError, "connect_timeout must be finite positive seconds")
    expect { per_call(ca_file: "/nonexistent/ca.pem") }
      .to raise_error(ArgumentError, "Invalid TLS client configuration: ca_file")
    expect { per_call(certificate_chain: ["bad"], private_key: "bad") }
      .to raise_error(ArgumentError, "Invalid TLS client configuration: certificate_chain")
    threads = Thread.list.size
    transport = with_ca { |ca_file| per_call(ca_file: ca_file, connect_timeout: 2) }
    expect([transport.frozen?, Thread.list.size]).to eq([true, threads])
    expect(transport.inspect).to eq("#<ConnectRPC::Async::PerCallTransport connect_timeout=2 proxy=environment>")
    expect([transport.streaming_capabilities, transport.grpc_capabilities])
      .to eq([%i[server_stream], %i[unary server_stream]])
  end

  PerCallHelpers::PATHS.each do |path|
    it "rejects unusable request URLs and TLS options with http before any socket on the #{path} path" do
      peer = threaded_host(server: server)
      port = peer.peer.port
      plain = per_call
      tls = with_ca { |ca_file| per_call(ca_file: ca_file) }
      invalid = [
        [plain, "ftp://127.0.0.1:#{port}/x", "Request URL must be HTTP(S) with a host and no userinfo"],
        [plain, "http:/x", "Request URL must be HTTP(S) with a host and no userinfo"],
        [plain, "http:///x", "Request URL must be HTTP(S) with a host and no userinfo"],
        [plain, "http://user:secret@127.0.0.1:#{port}/x", "Request URL must be HTTP(S) with a host and no userinfo"],
        [tls, "http://127.0.0.1:#{port}/x", "TLS options require an HTTPS origin"]
      ]
      with_boundaries do |boundaries|
        errors = on_path(path) do
          invalid.map { |transport, url, _| capture_error { transport.call(request(url)) } }
        end
        expect(errors.map { |error| [error.class, error.message] })
          .to eq(invalid.map { |*, message| [ArgumentError, message] })
        expect([boundaries.dialed, boundaries.take, peer.peer.accepts.size]).to eq([[], [], 0])
      end
      expect { plain.open_stream(request("http://127.0.0.1:#{port}/x")) }
        .to raise_error(ArgumentError, "A streaming block is required")
      expect { plain.open_grpc_stream(request("http://127.0.0.1:#{port}/x")) }
        .to raise_error(ArgumentError, "A gRPC block is required")
    ensure
      peer&.stop
    end
  end

  it "has core reject client and bidi streaming before any I/O in both protocols" do
    peer = mixed_host
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = per_call
    errors = %i[connect grpc].flat_map do |protocol|
      typed = mixed_client(transport, origin, protocol: protocol)
      [capture_error { typed.collect { |stream| stream.send_message(item("x")) } },
       capture_error { typed.chat { |stream| stream.send_message(item("x")) } }]
    end
    expect(errors.map { |error| [error.class, error.message] }).to eq(
      [[ArgumentError, "Transport does not support client_stream"],
       [ArgumentError, "Transport does not support bidi_stream"]] * 2
    )
    expect(peer.peer.accepts.size).to eq(0)
  ensure
    peer&.stop
  end

  %i[h2c tls mtls].each do |mode|
    PerCallHelpers::PATHS.each do |path|
      it "runs Connect POST, GET and server streams and gRPC unary and server streams over #{mode} on the #{path} " \
         "path, one closed connection each" do
        with_ca do |ca_file|
          tls, scheme, options = threaded_mode(mode, ca_file)
          peer = mixed_host(tls: tls)
          origin = "#{scheme}://127.0.0.1:#{peer.peer.port}"
          transport = per_call(**options)
          connect = mixed_client(transport, origin)
          get = mixed_client(transport, origin, use_get: true)
          grpc = mixed_client(transport, origin, protocol: :grpc)
          with_boundaries do |boundaries|
            results = on_path(path) do
              children = Async::Task.current? && Async::Task.current.children.to_a
              values = [
                connect.lookup(item("post")).message.name,
                get.lookup(item("get")).message.name,
                streamed(connect, "ss"),
                grpc.lookup(item("grpc")).message.name,
                streamed(grpc, "gss")
              ]
              [values, children && Async::Task.current.children.to_a == children]
            end
            raise results if results.is_a?(Exception)

            values, same_children = results
            expect(values).to eq(["post", "get", %w[ss-0 ss-1], "grpc", %w[gss-0 gss-1]])
            expect(same_children).to be(true) if path == :inline
            observations = boundaries.take
            expect(observations.size).to eq(5)
            observations.each { |observation| expect_released(observation) }
            expect(peer.peer.accepts.size).to eq(5)
          end
        ensure
          peer&.stop
        end
      end
    end
  end

  it "sends the request URL unchanged, with its path prefix and GET query, and the origin authority" do
    seen = Thread::Queue.new
    peer = threaded_recording_peer do |incoming|
      seen << [incoming.method, incoming.authority, incoming.path]
      incoming.send_response(Protocol::HTTP::Response[200, {}, ["ok"]])
    end
    port = peer.peer.port
    transport = per_call
    PerCallHelpers::PATHS.each do |path|
      responses = on_path(path) do
        [transport.call(request("http://127.0.0.1:#{port}/prefix/svc/Get?message=abc&encoding=proto",
                                method: "GET")).status,
         transport.call(request("http://LOCALHOST:#{port}/prefix/svc/Post")).status]
      end
      expect(responses).to eq([200, 200])
    end
    observed = 4.times.map { pop_within(seen, "a recorded request") }
    expect(observed).to eq(
      [["GET", "127.0.0.1:#{port}", "/prefix/svc/Get?message=abc&encoding=proto"],
       ["POST", "LOCALHOST:#{port}", "/prefix/svc/Post"]] * 2
    )
    expect(peer.peer.accepted).to eq(4)
  ensure
    peer&.stop
  end

  # Core sets the default; the transport sends whatever the request carries.
  PerCallHelpers::PATHS.each do |path|
    it "sends core's default User-Agent for Connect and gRPC, and a caller's own, on the #{path} path" do
      agents = Thread::Queue.new
      peer = threaded_recording_peer do |incoming|
        agents << incoming.headers["user-agent"].to_s
        incoming.send_response(Protocol::HTTP::Response[200, {}, ["ok"]])
      end
      origin = "http://127.0.0.1:#{peer.peer.port}"
      transport = per_call
      on_path(path) do
        %i[connect grpc].each do |protocol|
          typed = mixed_client(transport, origin, protocol: protocol)
          capture_error { typed.lookup(item("default")) }
          capture_error { typed.lookup(item("custom"), metadata: { "user-agent" => "custom-agent/1.0" }) }
        end
      end
      expect(4.times.map { pop_within(agents, "a recorded User-Agent") }).to eq(
        [ConnectRPC::Protocol::USER_AGENT, "custom-agent/1.0", ConnectRPC.const_get(:GRPCProtocol)::USER_AGENT,
         "custom-agent/1.0"]
      )
    ensure
      peer&.stop
    end
  end

  it "dials an IPv6 literal by address and keeps its brackets in the authority" do
    begin
      Socket.tcp_server_sockets("::1", 0).each(&:close)
    rescue SystemCallError
      skip "IPv6 loopback is unavailable"
    end
    seen = Thread::Queue.new
    peer = ThreadedHelpers::PeerThread.new do |task|
      bound = IO::Endpoint.tcp("::1", 0).bound
      recording = ConnectionPeer::RecordingServer.new(task, bound.sockets.first) do |incoming|
        seen << incoming.authority
        incoming.send_response(Protocol::HTTP::Response[200, {}, ["ok"]])
      end
      recording.define_singleton_method(:port) { bound.sockets.first.local_address.ip_port }
      [recording, lambda {
        recording.stop
        bound.close
      }]
    end
    port = peer.peer.port
    transport = per_call
    statuses = PerCallHelpers::PATHS.map do |path|
      on_path(path) { transport.call(request("http://[::1]:#{port}/v6")).status }
    end
    expect(statuses).to eq([200, 200])
    expect(2.times.map { pop_within(seen, "an IPv6 request") }).to eq(["[::1]:#{port}"] * 2)
  ensure
    peer&.stop
  end

  # The dial hook stops each call before any socket opens and reports the
  # origin the inner transport would dial.
  it "derives the dialed origin from the URL, omitting default ports from the authority" do
    dialed = Thread::Queue.new
    stop = Class.new(StandardError)
    trace = TracePoint.new(:call) do |point|
      origin = point.self.instance_variable_get(:@origin)
      dialed << [origin.scheme, origin.hostname, origin.port, origin.authority]
      raise stop, "stopped before connecting"
    end
    trace.enable(target: ConnectRPC::Async::Transport.instance_method(:dial))
    transport = per_call
    urls = %w[http://h.invalid/a https://h.invalid/a http://h.invalid:8080/a https://h.invalid:8443/a
              https://h.invalid:443/a]
    PerCallHelpers::PATHS.each do |path|
      errors = on_path(path) { urls.map { |url| capture_error { transport.call(request(url)) } } }
      expect(errors.map { |error| [error.class, error.code] }).to all(eq([ConnectRPC::Error, "unavailable"]))
    end
    observed = (urls.size * 2).times.map { pop_within(dialed, "a dial") }
    expect(observed).to eq([
      ["http", "h.invalid", 80, "h.invalid"], ["https", "h.invalid", 443, "h.invalid"],
      ["http", "h.invalid", 8080, "h.invalid:8080"], ["https", "h.invalid", 8443, "h.invalid:8443"],
      ["https", "h.invalid", 443, "h.invalid"]
    ] * 2)
  ensure
    trace&.disable
  end

  it "serves several origins in turn and concurrently from one instance, each call on its own connection" do
    peers = 3.times.map { threaded_host(server: server) }
    origins = peers.map { |peer| "http://127.0.0.1:#{peer.peer.port}" }
    transport = per_call
    with_boundaries do |boundaries|
      in_turn = PerCallHelpers::PATHS.flat_map do |path|
        on_path(path) do
          origins.map { |origin| client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: origin)).message.name }
        end
      end
      expect(in_turn).to eq(origins * 2)
      boundaries.take.each { |observation| expect_released(observation) }
      callers = 12.times.map do |index|
        Thread.new do
          origin = origins[index % 3]
          client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "t#{index}")).message.name
        end
      end
      inline = reactor do |task|
        6.times.map do |index|
          task.async do
            origin = origins[index % 3]
            client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "i#{index}")).message.name
          end
        end.map(&:wait)
      end
      expect(callers.map { |caller| value_within(caller, "a concurrent caller") })
        .to eq(12.times.map { |index| "t#{index}" })
      expect(inline).to eq(6.times.map { |index| "i#{index}" })
      observations = boundaries.take
      expect(observations.map { |observation| observation.fetch(:path) }.tally).to eq(threaded: 12, inline: 6)
      observations.each { |observation| expect_released(observation) }
      expect(peers.map { |peer| peer.peer.accepts.size }).to eq([8, 8, 8])
    end
  ensure
    peers&.each(&:stop)
  end
end
