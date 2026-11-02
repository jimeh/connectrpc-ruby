# frozen_string_literal: true

require "protocol/http/body/writable"

RSpec.describe "HTTP/2 backend header boundaries" do
  def raw_request(connection, origin, fields, body, trailers: nil)
    response = connection.create_response
    response.instance_variable_set(:@request, Protocol::HTTP::Request["POST", AsyncHelpers::ECHO::ECHO.path,
                                                                      scheme: "http", authority: URI(origin).authority])
    headers = [[":method", "POST"], [":scheme", "http"], [":authority", URI(origin).authority],
               [":path", AsyncHelpers::ECHO::ECHO.path], ["content-type", "application/proto"], *fields]
    response.stream.send_headers(headers)
    response.stream.send_data(body, trailers ? 0 : Protocol::HTTP2::END_STREAM)
    response.stream.send_headers(trailers, Protocol::HTTP2::END_STREAM) if trailers
    connection.read_response(response)
    [response.status, response.read]
  ensure
    response&.close
  end

  it "normalizes integer lengths and preserves leading repeats while ignoring actual trailing HEADERS" do
    reactor do
      observed = []
      host = ConnectRPC::Async::Host.new(server: server(echo: lambda { |message, context|
        observed << context.request_metadata["x-repeat"]
        message
      })).start
      origin = "http://127.0.0.1:#{host.port}"
      raw = IO::Endpoint.tcp("127.0.0.1", host.port).connect
      connection = Async::HTTP::Protocol::HTTP2.client(raw)
      bytes = AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new(name: "abcdefgh"))
      expect(bytes.bytesize).to eq(10)
      %w[+10 0xa 1_0].each do |length|
        status, body = raw_request(connection, origin,
                                   [["content-length", length], %w[x-repeat a], %w[x-repeat b]], bytes,
                                   trailers: [%w[x-repeat ignored]])
        expect(status).to eq(200)
        expect(AsyncHelpers::MESSAGE.decode(body).name).to eq("abcdefgh")
      end
      expect(observed).to eq([%w[a b], %w[a b], %w[a b]])
    ensure
      connection&.close
      AsyncHelpers::INTERNAL.raw_close(raw)
      host&.close(grace: 0)
    end
  end

  it "keeps malformed and duplicate lengths and singleton-header policy outside the core handler" do
    reactor do
      seen = []
      host = ConnectRPC::Async::Host.new(server: server(echo: lambda { |message, _|
        seen << true
        message
      })).start
      origin = "http://127.0.0.1:#{host.port}"
      connection = nil
      raw = nil
      [[%w[content-length garbage]], [%w[content-length 0], %w[content-length 0]],
       [["content-type", "application/proto"]]].each_with_index do |fields, index|
        raw = IO::Endpoint.tcp("127.0.0.1", host.port).connect
        connection = Async::HTTP::Protocol::HTTP2.client(raw)
        if index == 2
          status, = raw_request(connection, origin, fields, "")
          expect(status).to eq(400)
        else
          expect { raw_request(connection, origin, fields, "") }.to raise_error do |error|
            expect(error).to be_a(Protocol::HTTP2::Error).or be_a(IOError).or be_a(EOFError)
          end
        end
        connection.close
        AsyncHelpers::INTERNAL.raw_close(raw)
      end
      expect(seen).to be_empty
    ensure
      connection&.close
      AsyncHelpers::INTERNAL.raw_close(raw)
      host&.close(grace: 0)
    end
  end

  it "ignores response trailing HEADERS even when they are processed before the caller resumes" do
    reactor do |task|
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      listener = bound.sockets.first
      peer = task.async do
        raw, = listener.accept
        connection = Async::HTTP::Protocol::HTTP2.server(raw)
        incoming = connection.requests.dequeue
        incoming.stream.send_headers([[":status", "200"], ["content-type", "application/proto"],
                                      ["x-repeat", "a"], ["x-repeat", "b"], ["trailer-x-tail", "leading"]])
        incoming.stream.send_headers([["x-repeat", "ignored"], ["trailer-x-tail", "ignored"]], Protocol::HTTP2::END_STREAM)
        incoming.body&.close
        Async::Condition.new.wait
      ensure
        connection&.close
        AsyncHelpers::INTERNAL.raw_close(raw)
      end
      origin = "http://127.0.0.1:#{listener.local_address.ip_port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      response = client(transport, origin).echo(AsyncHelpers::MESSAGE.new)
      expect(response.metadata["x-repeat"]).to eq(%w[a b])
      expect(response.trailers["x-tail"]).to eq(["leading"])
    ensure
      transport&.close(grace: 0)
      peer&.cancel
      peer&.wait
      bound&.close
    end
  end
end
