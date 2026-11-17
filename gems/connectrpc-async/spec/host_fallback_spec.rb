# frozen_string_literal: true

RSpec.describe "Host native fallback" do
  def recording_fallback(seen)
    lambda do |request|
      seen << [request.method, request.path]
      Protocol::HTTP::Response[200, { "content-type" => "text/plain" }, ["fallback #{request.path}"]]
    end
  end

  def h2c(host)
    raw = IO::Endpoint.tcp("127.0.0.1", host.port).connect
    [raw, Async::HTTP::Protocol::HTTP2.client(raw)]
  end

  def get(connection, host, path, method: "GET", protocol: nil)
    response = connection.call(Protocol::HTTP::Request[method, path, scheme: "http",
                                                                     authority: "127.0.0.1:#{host.port}",
                                                                     protocol: protocol])
    [response.status, response.read]
  ensure
    response&.close
  end

  it "routes RPC to core and every other request to the fallback with its target intact" do
    reactor do
      seen = []
      host = ConnectRPC::Async::Host.new(server: server, fallback: recording_fallback(seen)).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      expect(client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "post")).message.name).to eq("post")
      expect(client(transport, origin, use_get: true).echo(AsyncHelpers::MESSAGE.new(name: "get")).message.name)
        .to eq("get")
      raw, connection = h2c(host)
      expect(get(connection, host, "/metrics")).to eq([200, "fallback /metrics"])
      expect(get(connection, host, "/some/page?q=1&r=two", method: "DELETE"))
        .to eq([200, "fallback /some/page?q=1&r=two"])
      # A registered service prefix stays with core, including unknown methods:
      # core rejects the missing content type instead of the fallback's 200.
      status, = get(connection, host, "/example.v1.EchoService/Unknown", method: "POST")
      expect(status).to eq(415)
      expect(seen).to eq([%w[GET /metrics], ["DELETE", "/some/page?q=1&r=two"]])
    ensure
      connection&.close
      AsyncHelpers::INTERNAL.raw_close(raw) if raw
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end

  it "answers unrelated paths with a bare 404 without a fallback" do
    reactor do
      host = ConnectRPC::Async::Host.new(server: server).start
      raw, connection = h2c(host)
      expect(get(connection, host, "/metrics")).to eq([404, nil])
    ensure
      connection&.close
      AsyncHelpers::INTERNAL.raw_close(raw) if raw
      host&.close(grace: 0)
    end
  end

  it "rejects extended CONNECT to the fallback with 501 without calling it" do
    reactor do
      seen = []
      host = ConnectRPC::Async::Host.new(server: server, fallback: recording_fallback(seen)).start
      raw, connection = h2c(host)
      expect(get(connection, host, "/socket", method: "CONNECT", protocol: "websocket")).to eq([501, nil])
      expect(get(connection, host, "/after")).to eq([200, "fallback /after"])
      expect(seen).to eq([%w[GET /after]])
    ensure
      connection&.close
      AsyncHelpers::INTERNAL.raw_close(raw) if raw
      host&.close(grace: 0)
    end
  end

  # Plain CONNECT (RFC 9113 8.5) carries no :path, so core routing fails its
  # stream before the fallback, with or without one, as before fallbacks existed.
  it "never passes plain CONNECT to the fallback and keeps serving siblings" do
    reactor do
      seen = []
      host = ConnectRPC::Async::Host.new(server: server, fallback: recording_fallback(seen)).start
      raw, connection = h2c(host)
      connect = Protocol::HTTP::Request["CONNECT", nil, authority: "example.test:443"]
      expect { connection.call(connect) }.to raise_error(Protocol::HTTP::RemoteError)
      expect(get(connection, host, "/after")).to eq([200, "fallback /after"])
      expect(seen).to eq([%w[GET /after]])
    ensure
      connection&.close
      AsyncHelpers::INTERNAL.raw_close(raw) if raw
      host&.close(grace: 0)
    end
  end

  it "replaces protocol-switching fallback responses with 501 and closes them" do
    reactor do
      switching = Protocol::HTTP::Response[101, {}, nil]
      upgraded = Protocol::HTTP::Response[200, {}, ["body"]]
      upgraded.protocol = "websocket"
      body = upgraded.body
      responses = [switching, upgraded]
      host = ConnectRPC::Async::Host.new(server: server, fallback: ->(_request) { responses.shift }).start
      raw, connection = h2c(host)
      expect([get(connection, host, "/a"), get(connection, host, "/b")]).to eq([[501, nil], [501, nil]])
      expect(responses).to be_empty
      expect(body).to be_empty
    ensure
      connection&.close
      AsyncHelpers::INTERNAL.raw_close(raw) if raw
      host&.close(grace: 0)
    end
  end

  it "fails only a raising fallback request and keeps serving siblings" do
    reactor do
      host = ConnectRPC::Async::Host.new(server: server, fallback: lambda { |request|
        raise "fallback failure" if request.path == "/boom"

        Protocol::HTTP::Response[200, {}, ["ok"]]
      }).start
      raw, connection = h2c(host)
      # The session resets only the failed stream.
      expect { get(connection, host, "/boom") }.to raise_error(Protocol::HTTP::RemoteError)
      expect(get(connection, host, "/fine")).to eq([200, "ok"])
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      expect(client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "rpc")).message.name).to eq("rpc")
    ensure
      transport&.close(grace: 0)
      connection&.close
      AsyncHelpers::INTERNAL.raw_close(raw) if raw
      host&.close(grace: 0)
    end
  end

  it "admits fallback requests under max_requests" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      host = ConnectRPC::Async::Host.new(server: server, max_requests: 1, fallback: lambda { |_request|
        entered.enqueue(true)
        release.dequeue
        Protocol::HTTP::Response[200, {}, ["held"]]
      }).start
      first_raw, first = h2c(host)
      second_raw, second = h2c(host)
      held = task.async { get(first, host, "/held") }
      entered.dequeue
      expect { get(second, host, "/rejected") }.to raise_error(Protocol::HTTP::RefusedError)
      release.enqueue(true)
      expect(held.wait).to eq([200, "held"])
    ensure
      release&.enqueue(true)
      [first, second].each { |connection| connection&.close }
      [first_raw, second_raw].each { |raw| AsyncHelpers::INTERNAL.raw_close(raw) if raw }
      host&.close(grace: 0)
    end
  end

  it "rejects a non-callable fallback at construction" do
    expect { ConnectRPC::Async::Host.new(server: server, fallback: 42) }
      .to raise_error(ArgumentError, "fallback must be callable")
  end
end
