# frozen_string_literal: true

RSpec.describe "Rails native Connect GET" do
  let(:binding) { ConnectRPC::Bindings::GoogleProtobuf.new }
  let(:type) { Example::V1::EchoMessage }
  let(:service) { Example::V1::EchoService }

  %i[proto json].each do |codec|
    it "runs generated #{codec} gzip GET through scoped Rails callbacks and metadata" do
      observed = []
      trace = []
      transport = lambda do |request|
        observed << request
        env = Rack::MockRequest.env_for(request.url.to_s, method: request.http_method, input: request.body)
        env["connectrpc.test.trace"] = trace
        env["HTTP_HOST"] = "www.example.com"
        request.headers.each do |key, values|
          env[key == "content-type" ? "CONTENT_TYPE" : "HTTP_#{key.upcase.tr('-', '_')}"] = values.join(", ")
        end
        status, headers, body = Rack::Lint.new(RailsIntegration::APP).call(env)
        bytes = +"".b
        body.each { |chunk| bytes << chunk }
        body.close if body.respond_to?(:close)
        ConnectRPC::TransportResponse.new(status: status, headers: headers, body: bytes)
      end
      core = ConnectRPC::Client.new(base_url: "http://www.example.com/rpc", binding: binding, transport: transport,
                                    codec: codec, encoding: :gzip, use_get: true)
      typed = service::Client.new(client: core)
      message = type.new(name: "GET 雪 +", data: "\x00\xff".b)
      result = typed.echo(message, metadata: { "x-gzip" => "yes" })
      expect(result.message).to eq(message)
      expect(result.metadata["vary"]).to eq(["Accept-Encoding"])
      expect(result.metadata["x-leading"]).to eq(%w[one two])
      expect(result.trailers["x-trailing"]).to eq(["three"])
      expect(observed.last.http_method).to eq("GET")
      expect(observed.last.body).to eq("")
      expect(trace.map(&:first)).to eq(%i[before around_before handler after around_ensure])
      expect { typed.echo(message, metadata: { "x-gzip" => "yes", "x-case" => "auth" }) }
        .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("unauthenticated") }
      expect { typed.fail(message, metadata: { "x-gzip" => "yes" }) }.to raise_error(ConnectRPC::Error) { |error|
        expect(error.code).to eq("invalid_argument")
      }
      expect(observed.last.http_method).to eq("POST")
    end
  end

  it "retains framework-owned Base safe-verb CSRF policy and rejects unsafe GET before callbacks" do
    message = type.new(name: "safe")
    query = URI.encode_www_form("connect" => "v1", "base64" => "1", "encoding" => "proto",
                                "message" => [message.to_proto].pack("m0").tr("+/", "-_").delete("="))
    mock = Rack::MockRequest.new(RailsIntegration::APP)
    response = mock.get("/base#{service::ECHO.path}?#{query}", "HTTP_HOST" => "www.example.com")
    expect(BaseEchoController.allow_forgery_protection).to be(true)
    expect(response.status).to eq(200)
    expect(type.decode(response.body)).to eq(message)
    expect(response["x-base"]).to eq(["before"])
    expect(response["trailer-x-base"]).to eq(["after"])
    trace = []
    response = mock.get("/rpc#{service::FAIL.path}?bad=%", "HTTP_HOST" => "www.example.com",
                                                           "connectrpc.test.trace" => trace)
    expect(response.status).to eq(405)
    expect(response["allow"]).to eq(["POST"])
    expect(trace).to be_empty
  end
end
