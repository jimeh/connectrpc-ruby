# frozen_string_literal: true

RSpec.describe ConnectRPC::Falcon::Server do
  %i[proto json].each do |codec|
    %i[identity gzip].each do |encoding|
      it "serves native #{codec} #{encoding} over real h2c and HTTP/1 without outer encoding" do
        reactor do
          options = encoding == :gzip ? { encoding: :gzip, accept_encoding: %i[gzip identity] } : {}
          falcon(server: server(encoding: encoding)) do |_host, origin|
            transport = ConnectRPC::Async::Transport.new(origin: origin)
            typed = client(transport, origin, codec: codec, **options)
            message = AsyncHelpers::MESSAGE.new(name: "native-#{codec}-#{encoding}")
            expect(typed.echo(message).message).to eq(message)
            binding = ConnectRPC::Bindings::GoogleProtobuf.new
            bytes = binding.encode(message, type: AsyncHelpers::MESSAGE, codec: codec)
            content_type = "application/#{codec == :proto ? 'proto' : 'json'}"
            headers = { "content-type" => content_type, "accept-encoding" => "gzip" }
            status, headers, response = http1(
              origin, "#{AsyncHelpers::ECHO::ECHO.path}?q=1", method: "POST", body: bytes,
                                                              headers: headers
            )
            expect(status).to eq(200)
            if encoding == :gzip
              expect(headers["content-encoding"]).to eq(["gzip"])
              response = ConnectRPC::Compression.decode(response, limit: 1024)
            else
              expect(headers["content-encoding"]).to be_nil
            end
            expect(response).to eq(bytes)
          ensure
            transport&.close(grace: 0)
          end
        end
      end
    end
  end

  it "passes ordinary Rack routes through Falcon middleware while keeping registered unknown RPCs in core" do
    reactor do
      seen = []
      rack = lambda do |env|
        seen << env["PATH_INFO"]
        [200, { "content-type" => "text/plain" }, ["ordinary"]]
      end
      falcon(rack_app: rack) do |_host, origin|
        expect(http1(origin, "/ordinary").values_at(0, 2)).to eq([200, "ordinary"])
        status, = http1(origin, "/example.v1.EchoService/Missing", method: "POST",
                                                                   headers: { "content-type" => "application/proto" })
        expect(status).to eq(501)
        expect(seen).to eq(["/ordinary"])
      end
    end
  end

  it "rejects scheme/TLS mismatch before serving" do
    reactor do
      [["http", host_tls], ["https", nil]].each do |scheme, tls|
        endpoint = ConnectRPC::Falcon::Endpoint.parse("#{scheme}://127.0.0.1:0")
        expect { described_class.new(server: server, endpoint: endpoint, tls: tls) }.to raise_error(ArgumentError)
      end
    end
  end
  it "rejects invalid and cross-thread close before mutating the listener" do
    reactor do
      falcon(rack_app: ->(_env) { [200, {}, ["alive"]] }) do |host, origin|
        expect { host.close(grace: -1) }.to raise_error(ArgumentError)
        failure = Thread.new do
          host.close
        rescue StandardError => e
          e
        end.value
        expect(failure).to be_a(AsyncHelpers::LIFECYCLE_ERROR)
        expect(http1(origin, "/still-serving").values_at(0, 2)).to eq([200, "alive"])
      end
    end
  end
end
