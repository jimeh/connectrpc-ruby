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

  def echo_body(name) = AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new(name: name))

  it "ignores an h2c offer on Rack routes, unary POST and GET RPCs and keeps the connection usable" do
    reactor do
      seen = []
      rack = lambda do |env|
        seen << env["PATH_INFO"]
        [200, { "content-type" => "text/plain" }, ["ordinary"]]
      end
      falcon(rack_app: rack) do |_host, origin|
        peer = ConnectH1Wire.new(origin)
        peer.write("GET /ordinary HTTP/1.1\r\nHost: localhost\r\nAccept-Encoding: identity\r\n#{h2c_offer}\r\n")
        expect(read_response(peer).values_at(0, 2)).to eq([200, "ordinary"])

        bytes = echo_body("offered")
        peer.write("POST #{AsyncHelpers::ECHO::ECHO.path} HTTP/1.1\r\nHost: localhost\r\n" \
                   "Content-Type: application/proto\r\nContent-Length: #{bytes.bytesize}\r\n#{h2c_offer}\r\n#{bytes}")
        status, _fields, body = read_response(peer)
        expect(status).to eq(200)
        expect(AsyncHelpers::MESSAGE.decode(body).name).to eq("offered")

        query = "encoding=proto&base64=1&message=#{[echo_body('get')].pack('m0').tr('+/', '-_').delete('=')}"
        peer.write("GET #{AsyncHelpers::ECHO::ECHO.path}?#{query} HTTP/1.1\r\nHost: localhost\r\n#{h2c_offer}\r\n")
        status, _fields, body = read_response(peer)
        expect(status).to eq(200)
        expect(AsyncHelpers::MESSAGE.decode(body).name).to eq("get")
        expect(seen).to eq(["/ordinary"])
      ensure
        peer&.close
      end
    end
  end

  it "exposes a WebSocket offer to Rack, rejects accepting it and keeps framing for the next request" do
    reactor do
      offers = []
      rack = lambda do |env|
        offers << [env["PATH_INFO"], env["HTTP_UPGRADE"], Array(env["rack.protocol"])]
        case env["PATH_INFO"]
        when "/protocol" then [200, { "rack.protocol" => "websocket" }, []]
        when "/101" then [101, { "upgrade" => "websocket" }, []]
        else [200, { "content-type" => "text/plain" }, ["after"]]
        end
      end
      falcon(rack_app: rack) do |_host, origin|
        peer = ConnectH1Wire.new(origin)
        # The unread offering bodies must be framed by Content-Length, not
        # read to connection close, or the next request becomes body bytes.
        %w[/protocol /101].each do |route|
          peer.write("POST #{route} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n#{websocket_offer}\r\nhello")
          expect(read_response(peer).first).to eq(501)
        end
        peer.write("GET /after HTTP/1.1\r\nHost: localhost\r\nAccept-Encoding: identity\r\n\r\n")
        expect(read_response(peer).values_at(0, 2)).to eq([200, "after"])
        expect(offers).to eq([["/protocol", "websocket", ["websocket"]], ["/101", "websocket", ["websocket"]],
                              ["/after", nil, []]])
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
