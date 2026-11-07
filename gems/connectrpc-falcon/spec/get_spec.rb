# frozen_string_literal: true

RSpec.describe "Falcon native GET composition" do
  %i[proto json].each do |codec|
    it "serves generated #{codec} GET over H2 and bodyless H1 while preserving fallback queries" do
      reactor do
        seen = []
        fallback = lambda do |env|
          seen << [env["PATH_INFO"], env["QUERY_STRING"]]
          [200, { "content-type" => "text/plain" }, ["ordinary"]]
        end
        falcon(server: server(encoding: :gzip), rack_app: fallback, cache: true) do |_host, origin|
          transport = ConnectRPC::Async::Transport.new(origin: origin)
          requests = []
          observed = lambda do |request|
            requests << request
            transport.call(request)
          end
          typed = client(observed, origin, codec: codec, use_get: true, encoding: :gzip)
          message = AsyncHelpers::MESSAGE.new(name: "falcon GET 雪")
          expect(typed.echo(message, metadata: { "x-request" => ["get"] }).message).to eq(message)
          outgoing = requests.last
          expect(outgoing.http_method).to eq("GET")
          status, headers, bytes = http1(
            origin, outgoing.url.request_uri, body: nil,
                                              headers: { "accept-encoding" => "gzip", "x-request" => "get" }
          )
          expect(status).to eq(200)
          expect(headers["vary"].map(&:downcase)).to eq(["accept-encoding"])
          expect(headers["x-echo"]).to eq(["get"])
          expect(headers["trailer-x-tail"]).to eq(%w[three four])
          decoded = ConnectRPC::Compression.decode(bytes, limit: 4096)
          binding = ConnectRPC::Bindings::GoogleProtobuf.new
          expect(binding.decode(decoded, type: AsyncHelpers::MESSAGE, codec: codec)).to eq(message)
          expect(http1(origin, "/ordinary?x=a%2Bb", body: nil).last).to eq("ordinary")
          expect(seen).to eq([["/ordinary", "x=a%2Bb"]])
        ensure
          transport&.close(grace: 0)
        end
      end
    end
  end

  it "bypasses fallback caching and encoding for repeated native identity JSON GETs" do
    reactor do
      calls = 0
      core = server(echo: lambda { |message, context|
        calls += 1
        context.response_metadata["x-echo"] = context.request_metadata["x-request"]
        message
      })
      fallback = ->(_) { raise "Native GET reached fallback" }
      falcon(server: core, rack_app: fallback, cache: true) do |_host, origin|
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        outgoing = nil
        observed = lambda do |request|
          outgoing = request
          transport.call(request)
        end
        message = AsyncHelpers::MESSAGE.new(name: "native identity")
        typed = client(observed, origin, codec: :json, use_get: true)
        expect(typed.echo(message, metadata: { "x-request" => ["h2"] }).message).to eq(message)
        %w[first second].each do |value|
          status, headers, bytes = http1(origin, outgoing.url.request_uri, body: nil,
                                                                           headers: { "x-request" => value,
                                                                                      "accept-encoding" => "gzip" })
          expect(status).to eq(200)
          expect(headers["x-echo"]).to eq([value])
          expect(headers["content-encoding"]).to be_nil
          expect(headers["x-cache"]).to be_nil
          expect(headers["etag"]).to be_nil
          expect(AsyncHelpers::MESSAGE.decode_json(bytes)).to eq(message)
        end
        expect(calls).to eq(3)
      ensure
        transport&.close(grace: 0)
      end
    end
  end
end
