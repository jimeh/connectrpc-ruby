# frozen_string_literal: true

RSpec.describe "ThreadedTransport construction and Connect unary from plain threads" do
  it "validates every argument at construction without a reactor, thread or socket" do
    peer = threaded_host(server: server)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    expect(Fiber.scheduler).to be_nil
    invalid = [
      [{ origin: "http://127.0.0.1:1/prefix" }, "Origin must contain only an HTTP(S) scheme, host and port"],
      [{ origin: "ftp://127.0.0.1:1" }, "Origin must contain only an HTTP(S) scheme, host and port"],
      [{ origin: origin, connection_limit: 0 }, "connection_limit must be a positive integer"],
      [{ origin: origin, max_requests: 1.5 }, "max_requests must be a positive integer"],
      [{ origin: origin, connect_timeout: 0 }, "connect_timeout must be finite positive seconds"],
      [{ origin: origin, connection_policy: nil }, "connection_policy must be a ConnectionPolicy"],
      [{ origin: origin, connection_observer: Object.new }, "connection_observer must respond to call"],
      [{ origin: origin, ca_file: "ca.pem" }, "TLS options require an HTTPS origin"],
      [{ origin: "https://127.0.0.1:1", ca_file: "/nonexistent/ca.pem" }, "Invalid TLS client configuration"]
    ]
    invalid.each do |options, message|
      expect { threaded(**options) }.to raise_error(ArgumentError, message)
    end
    threads = Thread.list.size
    transport = threaded(origin: origin)
    expect([Thread.list.size, peer.peer.accepts.size, transport.connection_state]).to eq([threads, 0, :idle])
    expect(transport.inspect).to include("state=open", "connection_state=idle", origin)
    expect(transport.close).to be(true)
    expect([Thread.list.size, peer.peer.accepts.size, transport.connection_state]).to eq([threads, 0, :closed])
  ensure
    peer&.stop
  end

  %i[h2c tls mtls].each do |mode|
    it "multiplexes a generated client on eight threads over one #{mode} connection and one owner thread" do
      with_ca do |ca_file|
        tls, scheme, options = threaded_mode(mode, ca_file)
        entered = Thread::Queue.new
        release = Thread::Queue.new
        peer = threaded_host(tls: tls, server: server(echo: lambda { |message, _|
          entered << message.name
          release.pop
          message
        }))
        port = peer.peer.port
        origin = "#{scheme}://127.0.0.1:#{port}"
        transport = threaded(origin: origin, **options)
        typed = client(transport, origin)
        callers = 8.times.map { |index| Thread.new { typed.echo(AsyncHelpers::MESSAGE.new(name: "c#{index}")) } }
        expect(8.times.map { pop_within(entered, "eight calls in the handler") }.sort)
          .to eq(8.times.map { |index| "c#{index}" })
        expect([peer.peer.accepts.size, owner_threads(port).size]).to eq([1, 1])
        8.times { release << true }
        expect(callers.map { |caller| value_within(caller, "a multiplexed caller").message.name })
          .to eq(8.times.map { |index| "c#{index}" })
        expect([peer.peer.accepts.size, transport.connection_state, admitted(transport)]).to eq([1, :ready, 0])
        expect(transport.close).to be(true)
        expect(owner_threads(port)).to be_empty
      ensure
        8.times { release&.<<(true) }
        transport&.close(grace: 0)
        peer&.stop
      end
    end
  end

  it "grows to a second connection under a low peer stream limit from the same owner thread" do
    release = Thread::Queue.new
    limit = { Protocol::HTTP2::Settings::MAXIMUM_CONCURRENT_STREAMS => 2 }
    peer = threaded_recording_peer(settings: limit) do |incoming|
      release.pop
      incoming.send_response(Protocol::HTTP::Response[200, {}, [incoming.path]])
    end
    port = peer.peer.port
    origin = "http://127.0.0.1:#{port}"
    transport = threaded(origin: origin, connection_limit: 2)
    callers = 4.times.map { |index| Thread.new { transport.call(request("#{origin}/#{index}")).body } }
    eventually("four streams dispatched") { peer.peer.headers_frames.size == 4 }
    expect([peer.peer.accepted, owner_threads(port).size]).to eq([2, 1])
    4.times { release << true }
    expect(callers.map { |caller| value_within(caller, "a caller on the grown pool") }).to eq(%w[/0 /1 /2 /3])
    expect(peer.peer.accepted).to eq(2)
  ensure
    4.times { release&.<<(true) }
    transport&.close(grace: 0)
    peer&.stop
  end

  [true, false].each do |retry_refused|
    it "forwards retry_refused: #{retry_refused} to the owner's transport for a refused unary call" do
      seen = Hash.new(0)
      invocations = []
      peer = threaded_recording_peer do |incoming|
        if (seen[incoming.path] += 1) == 1
          incoming.stream.send_reset_stream(Protocol::HTTP2::Error::REFUSED_STREAM)
        else
          invocations << incoming.path
          incoming.send_response(Protocol::HTTP::Response[200, {}, [incoming.path]])
        end
      end
      origin = "http://127.0.0.1:#{peer.peer.port}"
      policy = ConnectRPC::Async::ConnectionPolicy.new(retry_refused: retry_refused)
      transport = threaded(origin: origin, connection_policy: policy)
      caller = Thread.new do
        transport.call(request("#{origin}/refused")).body
      rescue ConnectRPC::Error => e
        e
      end
      result = value_within(caller, "the refused caller")
      if retry_refused
        expect([result, invocations]).to eq(["/refused", ["/refused"]])
      else
        expect([result.code, invocations]).to eq(["unavailable", []])
      end
      expect([peer.peer.headers_frames.size, peer.peer.accepted]).to eq([retry_refused ? 2 : 1, 1])
    ensure
      transport&.close(grace: 0)
      peer&.stop
    end
  end

  it "names the owner thread by host and port only" do
    peer = threaded_host(server: server)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "named"))
    expect(engine_of(transport).thread.name).to eq("connectrpc-async 127.0.0.1:#{peer.peer.port}")
  ensure
    transport&.close(grace: 0)
    peer&.stop
  end

  it "sends safe unary GET and charges its request target to the byte budget" do
    peer = threaded_host(server: server)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    seen = []
    events = Thread::Queue.new
    transport = threaded(origin: origin, connection_observer: ->(event) { events << event },
                         connection_policy: ConnectRPC::Async::ConnectionPolicy.new(max_pending_bytes: 256))
    recording = lambda do |outgoing|
      seen << [outgoing.http_method, outgoing.body]
      transport.call(outgoing)
    end
    response = client(recording, origin, use_get: true).echo(AsyncHelpers::MESSAGE.new(name: "get"))
    expect([response.message.name, seen]).to eq(["get", [["GET", ""]]])
    large = request("#{origin}/example.v1.EchoService/Echo?message=#{'a' * 300}", method: "GET")
    expect { transport.call(large) }.to unavailable_error
    pop_until(events, "admission_rejected event") { |event| event.type == :admission_rejected }
    expect([admitted(transport), peer.peer.accepts.size]).to eq([0, 1])
  ensure
    transport&.close(grace: 0)
    peer&.stop
  end

  it "sends the target the caller passed even if the caller mutates its URI strings while the call is queued" do
    peer = threaded_host(server: server)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    transport.call(request("#{origin}/example.v1.EchoService/Echo",
                           body: AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new(name: "warm")),
                           headers: { "content-type" => ["application/proto"] }))
    gate = hold_owner(transport)
    body = AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new(name: "queued"))
    outgoing = request("#{origin}/example.v1.EchoService/Echo", body: body,
                                                                headers: { "content-type" => ["application/proto"] })
    caller = Thread.new do
      transport.call(outgoing)
    rescue StandardError => e
      e
    end
    caller.report_on_exception = false
    eventually("call queued behind the held owner") { admitted(transport) == 1 && blocked?(caller) }
    outgoing.url.path.replace("/example.v1.EchoService/Fail")
    gate << true
    response = value_within(caller, "the queued call")
    expect(response).to be_a(ConnectRPC::TransportResponse)
    expect(response.status).to eq(200)
    expect(AsyncHelpers::MESSAGE.decode(response.body).name).to eq("queued")
  ensure
    gate&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  it "has no streaming or gRPC capability, so core rejects those shapes before any I/O" do
    peer = threaded_host(server: server)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    %i[streaming_capabilities open_stream grpc_capabilities open_grpc_stream].each do |name|
      expect(transport).not_to respond_to(name)
    end
    binding = ConnectRPC::Bindings::GoogleProtobuf.new
    core = ConnectRPC::Client.new(base_url: origin, binding: binding, transport: transport)
    method = ConnectRPC::ServerStreamMethod.new(service: "test.Streams", name: "ServerStream",
                                                request_type: AsyncHelpers::MESSAGE,
                                                response_type: AsyncHelpers::MESSAGE)
    expect { core.server_stream(method, AsyncHelpers::MESSAGE.new) { nil } }
      .to raise_error(ArgumentError, "Transport does not support server_stream")
    grpc = ConnectRPC::Client.new(base_url: origin, binding: binding, transport: transport, protocol: :grpc)
    expect { grpc.call(AsyncHelpers::ECHO::ECHO, AsyncHelpers::MESSAGE.new) }
      .to raise_error(ArgumentError, "Transport does not support unary gRPC")
    expect([peer.peer.accepts.size, engine_of(transport)]).to eq([0, nil])
  ensure
    transport&.close(grace: 0)
    peer&.stop
  end

  it "rejects full admission with unavailable and reports it on the owner thread" do
    entered = Thread::Queue.new
    release = Thread::Queue.new
    peer = threaded_host(server: server(echo: lambda { |message, _|
      entered << true
      release.pop
      message
    }))
    origin = "http://127.0.0.1:#{peer.peer.port}"
    events = Thread::Queue.new
    transport = threaded(origin: origin, max_requests: 1,
                         connection_observer: ->(event) { events << [event.type, Thread.current] })
    typed = client(transport, origin)
    held = Thread.new { typed.echo(AsyncHelpers::MESSAGE.new(name: "held")) }
    pop_within(entered, "the held call in the handler")
    expect { typed.echo(AsyncHelpers::MESSAGE.new(name: "rejected")) }.to unavailable_error
    observed = pop_until(events, "admission_rejected event") { |type, _| type == :admission_rejected }
    expect(observed.last).to equal(engine_of(transport).thread)
    release << true
    expect(value_within(held, "the held caller").message.name).to eq("held")
    expect(admitted(transport)).to eq(0)
  ensure
    release&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end

  it "raises LifecycleError for call and close from the observer and the owner thread" do
    peer = threaded_host(server: server)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    errors = Thread::Queue.new
    transport = nil
    observer = lambda do |event|
      next unless event.type == :state_changed && event.state == :ready

      [-> { transport.call(request("#{origin}/x")) }, -> { transport.close }].each do |operation|
        operation.call
      rescue StandardError => e
        errors << e
      end
    end
    transport = threaded(origin: origin, connection_observer: observer)
    typed = client(transport, origin)
    expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "ok")).message.name).to eq("ok")
    2.times { expect(pop_within(errors, "an observer LifecycleError")).to be_a(AsyncHelpers::LIFECYCLE_ERROR) }
    expect(on_owner(transport) { transport.close }.last).to be_a(AsyncHelpers::LIFECYCLE_ERROR)
    expect(on_owner(transport) { transport.connection_state }).to eq(%i[ok ready])
    expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "still")).message.name).to eq("still")
  ensure
    transport&.close(grace: 0)
    peer&.stop
  end
end
