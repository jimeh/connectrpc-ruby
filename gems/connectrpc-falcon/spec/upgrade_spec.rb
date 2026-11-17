# frozen_string_literal: true

require_relative "support/http1_wire"

RSpec.describe "Falcon HTTP/1.1 Upgrade offers" do
  def h2c_offer = "Connection: Upgrade, HTTP2-Settings\r\nUpgrade: h2c\r\nHTTP2-Settings: AAMAAABkAAQAoAAAAAIAAAAA\r\n"
  def websocket_offer = "Connection: Upgrade\r\nUpgrade: websocket\r\n"

  def read_response(peer)
    status, fields = peer.head
    body = if fields["content-length"].any?
             peer.socket.read(Integer(fields["content-length"].first))
           elsif fields["transfer-encoding"].any?
             peer.body
           end
    [status, fields, body]
  end

  # Sends one request on a fresh connection. `closed` is true when the server
  # announced and performed a close after the response, so no later bytes on
  # the connection were parsed as a request.
  def offered(origin, bytes)
    peer = ConnectH1Wire.new(origin)
    peer.write(bytes)
    status, fields, body = read_response(peer)
    [status, body, fields["connection"] == ["close"] && peer.socket.read == ""]
  ensure
    peer&.close
  end

  def echo_body(name) = AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new(name: name))

  it "serves Rack routes and unary POST and GET RPCs that carry an h2c offer, then closes" do
    reactor do
      seen = []
      rack = lambda do |env|
        seen << env["PATH_INFO"]
        [200, { "content-type" => "text/plain" }, ["ordinary"]]
      end
      falcon(rack_app: rack) do |_host, origin|
        request = "GET /ordinary HTTP/1.1\r\nHost: localhost\r\nAccept-Encoding: identity\r\n#{h2c_offer}\r\n"
        expect(offered(origin, request)).to eq([200, "ordinary", true])

        bytes = echo_body("offered")
        request = "POST #{AsyncHelpers::ECHO::ECHO.path} HTTP/1.1\r\nHost: localhost\r\n" \
                  "Content-Type: application/proto\r\nContent-Length: #{bytes.bytesize}\r\n#{h2c_offer}\r\n#{bytes}"
        status, body, closed = offered(origin, request)
        expect([status, AsyncHelpers::MESSAGE.decode(body).name, closed]).to eq([200, "offered", true])

        query = "encoding=proto&base64=1&message=#{[echo_body('get')].pack('m0').tr('+/', '-_').delete('=')}"
        status, body, closed = offered(origin, "GET #{AsyncHelpers::ECHO::ECHO.path}?#{query} HTTP/1.1\r\n" \
                                               "Host: localhost\r\n#{h2c_offer}\r\n")
        expect([status, AsyncHelpers::MESSAGE.decode(body).name, closed]).to eq([200, "get", true])
        expect(seen).to eq(["/ordinary"])
      end
    end
  end

  it "never parses bytes after an offering request as another request" do
    reactor do
      seen = []
      rack = lambda do |env|
        seen << env["PATH_INFO"]
        [200, { "content-type" => "text/plain" }, ["served"]]
      end
      falcon(rack_app: rack) do |_host, origin|
        # A proxy that tunnels on the client's offer alone forwards this
        # second request without inspecting it.
        smuggled = "GET /admin HTTP/1.1\r\nHost: localhost\r\n\r\n"
        expect(offered(origin, "GET /chat HTTP/1.1\r\nHost: localhost\r\nAccept-Encoding: identity\r\n" \
                               "#{websocket_offer}\r\n#{smuggled}")).to eq([200, "served", true])
        chunked = "POST /upload HTTP/1.1\r\nHost: localhost\r\nAccept-Encoding: identity\r\n" \
                  "Transfer-Encoding: chunked\r\n#{websocket_offer}\r\n5\r\nhello\r\n0\r\n\r\n"
        expect(offered(origin, "#{chunked}#{smuggled}")).to eq([200, "served", true])
        expect(seen).to eq(%w[/chat /upload])
      end
    end
  end

  it "exposes a WebSocket offer to Rack and rejects every way of accepting it" do
    reactor do
      offers = []
      rack = lambda do |env|
        offers << [env["PATH_INFO"], env["HTTP_UPGRADE"], Array(env["rack.protocol"])]
        request = env["protocol.http.request"]
        switch = { "upgrade" => "websocket" }
        case env["PATH_INFO"]
        when "/protocol" then next [200, { "rack.protocol" => "websocket" }, []]
        when "/101" then next [101, switch, []]
        when "/send-interim" then request.send_interim_response(101, switch)
        when "/write-interim" then request.write_interim_response(101, switch)
        when "/interim-callback" then request.interim_response.call(101, switch)
        end
        [200, { "content-type" => "text/plain" }, ["accepted"]]
      end
      falcon(rack_app: rack) do |_host, origin|
        routes = %w[/protocol /101 /send-interim /write-interim /interim-callback]
        routes.each do |route|
          request = "POST #{route} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n#{websocket_offer}\r\nhello"
          expect(offered(origin, request).values_at(0, 2)).to eq([501, true]), route
        end
        expect(offers).to eq(routes.map { |route| [route, "websocket", ["websocket"]] })
      end
    end
  end

  it "still sends non-switching interim responses from Rack" do
    reactor do
      rack = lambda do |env|
        env["protocol.http.request"].send_interim_response(103, { "link" => "</app.css>; rel=preload" })
        [200, { "content-type" => "text/plain" }, ["hinted"]]
      end
      falcon(rack_app: rack) do |_host, origin|
        peer = ConnectH1Wire.new(origin)
        peer.write("GET /hinted HTTP/1.1\r\nHost: localhost\r\nAccept-Encoding: identity\r\n#{websocket_offer}\r\n")
        status, fields = peer.head
        expect([status, fields["link"]]).to eq([103, ["</app.css>; rel=preload"]])
        expect(read_response(peer).values_at(0, 2)).to eq([200, "hinted"])
      ensure
        peer&.close
      end
    end
  end

  it "still rejects HTTP/2 upgrade fields, :protocol and extended CONNECT before fallback" do
    reactor do
      seen = []
      rack = lambda do |env|
        seen << env["PATH_INFO"]
        [200, { "content-type" => "text/plain" }, ["h2"]]
      end
      falcon(rack_app: rack) do |_host, origin|
        authority = URI(origin).authority
        raw = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
        connection = Async::HTTP::Protocol::HTTP2.client(raw)
        call = lambda do |method, path, headers: {}, protocol: nil|
          request = Protocol::HTTP::Request[method, path, headers, nil, scheme: "http", authority: authority,
                                                                        protocol: protocol]
          response = connection.call(request)
          [response.status, response.read]
        end
        expect(call.call("GET", "/upgrade", headers: { "upgrade" => "websocket" }).first).to eq(501)
        expect(call.call("GET", "/protocol", protocol: "websocket").first).to eq(501)
        expect(call.call("CONNECT", "/chat", protocol: "websocket").first).to eq(501)
        expect(call.call("GET", "/plain")).to eq([200, "h2"])
        expect(seen).to eq(["/plain"])
      ensure
        connection&.close
        AsyncHelpers::INTERNAL.raw_close(raw)
      end
    end
  end
end
