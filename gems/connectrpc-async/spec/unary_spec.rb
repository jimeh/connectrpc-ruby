# frozen_string_literal: true

RSpec.describe "HTTP/2 unary product integration" do
  %i[proto json].each do |codec|
    %i[h2c tls].each do |mode|
      it "runs generated #{codec} success, presence, errors and repeated metadata over #{mode}" do
        reactor do
          with_ca do |ca_file|
            host = ConnectRPC::Async::Host.new(server: server, tls: mode == :tls ? host_tls : nil).start
            origin = "#{mode == :tls ? 'https' : 'http'}://localhost:#{host.port}"
            transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: mode == :tls ? ca_file : nil)
            typed = client(transport, origin, codec: codec)
            message = AsyncHelpers::MESSAGE.new(name: "", count: 9_007_199_254_740_993, mood: :HAPPY,
                                                data: "\x00\xff".b)
            response = typed.echo(message, metadata: { "x-request" => %w[a b] })
            expect(response.message).to eq(message)
            expect(response.message.has_name?).to be_truthy
            expect(response.metadata["x-repeat"]).to eq(%w[one two])
            expect(response.metadata["x-echo"]).to eq(%w[a b])
            expect(response.trailers["x-tail"]).to eq(%w[three four])
            expect { typed.fail(message) }.to raise_error(ConnectRPC::Error) { |error|
              expect(error.code).to eq("invalid_argument")
              expect(error.message).to eq("deliberate")
              expect(AsyncHelpers::MESSAGE.decode(error.details.first.value)).to eq(message)
              expect(error.metadata["x-error"]).to eq(%w[a b])
              expect(error.trailers["x-end"]).to eq(%w[c d])
            }
            connection = transport.instance_variable_get(:@resources).first.connection
            expect(connection.version).to eq("HTTP/2")
            socket = transport.instance_variable_get(:@resources).first.socket
            expect(socket.alpn_protocol).to eq("h2") if mode == :tls
          ensure
            transport&.close
            host&.close
          end
        end
      end
    end
  end

  it "routes case-sensitive service paths and unknown RPCs, strips query only, and rejects verbs" do
    reactor do
      host = ConnectRPC::Async::Host.new(server: server).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      headers = { "content-type" => ["application/proto"] }
      expect(transport.call(request("#{origin}/Example.v1.EchoService/Echo", headers: headers)).status).to eq(404)
      expect(transport.call(request("#{origin}/example.v1.EchoService/Unknown", headers: headers)).status).to eq(501)
      expect(transport.call(request("#{origin}#{AsyncHelpers::ECHO::ECHO.path}?x=1",
                                    headers: headers)).status).to eq(200)
      expect(transport.call(request("#{origin}#{AsyncHelpers::ECHO::ECHO.path}", method: "GET",
                                                                                 headers: headers)).status).to eq(405)
      expect(transport.call(request("#{origin}#{AsyncHelpers::ECHO::ECHO.path}")).status).to eq(415)
    ensure
      transport&.close
      host&.close
    end
  end

  it "enforces declared and incremental message bounds" do
    reactor do
      host = ConnectRPC::Async::Host.new(server: server(request_limit: 32)).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      typed = client(transport, origin)
      expect { typed.echo(AsyncHelpers::MESSAGE.new(name: "x" * 200_000)) }.to raise_error(ConnectRPC::Error) { |e|
        expect(e.code).to eq("resource_exhausted")
      }
      connection = transport.instance_variable_get(:@resources).first
      expect(connection.raw).not_to be_closed
      small = client(transport, origin, response_limit: 1)
      expect { small.echo(AsyncHelpers::MESSAGE.new(name: "hello")) }.to raise_error(ConnectRPC::Error) { |e|
        expect(e.code).to eq("resource_exhausted")
      }
      expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "after")).message.name).to eq("after")
      expect(transport.instance_variable_get(:@resources).first).to equal(connection)
    ensure
      transport&.close
      host&.close
    end
  end

  it "reads exact chunk boundaries without treating an empty remainder as EOF" do
    body = Protocol::HTTP::Body::Buffered.new(%w[ab cd ef])
    input = AsyncHelpers::INTERNAL::Input.new(body)
    expect([input.read(2), input.read(1), input.read(1), input.read(10),
            input.read(2)]).to eq(["ab", "c", "d", "ef", nil])
  end
end
