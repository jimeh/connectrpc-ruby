# frozen_string_literal: true

RSpec.describe "Native Rails gRPC-Web hooks" do
  %w[application/grpc-web application/grpc-web-text].each do |media|
    it "runs #{media} through actual controller callbacks and keeps application security headers" do
      trace = []
      message = Example::V1::EchoMessage.encode(Example::V1::EchoMessage.new(name: "web"))
      bytes = [0, message.bytesize].pack("CN") + message
      text = media.end_with?("-text")
      response = Rack::MockRequest.new(RailsIntegration::APP).post(
        "/web/example.v1.EchoService/Echo",
        input: text ? [bytes].pack("m0") : bytes,
        "CONTENT_TYPE" => media,
        "HTTP_HOST" => "www.example.com",
        "HTTP_ORIGIN" => "https://browser.example",
        "HTTP_X_CASE" => "cookie",
        "connectrpc.test.trace" => trace
      )
      body = text ? response.body.scan(/.{4}/).map { |group| group.unpack1("m0") }.join : response.body
      expect(response.status).to eq(200)
      expect(body).to start_with(bytes)
      expect(body).to include("grpc-status: 0\r\n")
      expect(trace.map(&:first)).to eq(%i[before around_before handler after around_ensure])
      expect(response["access-control-allow-origin"]).to eq(["https://browser.example"])
      expect(response["content-security-policy"]).to eq("default-src 'none'")
      expect(response["set-cookie"]).to eq(["first=1; path=/", "second=2; path=/"])
      expect(WebEchoController.action_methods).not_to include("connect_protocol", "connect_status_codec",
                                                              "connect_cors")
    end
  end

  it "runs preflight before callbacks and keeps actual Base CSRF checks enabled" do
    mock = Rack::MockRequest.new(RailsIntegration::APP)
    trace = []
    response = mock.request("OPTIONS", "/web/example.v1.EchoService/Echo",
                            "HTTP_HOST" => "www.example.com",
                            "HTTP_ORIGIN" => "https://browser.example",
                            "HTTP_ACCESS_CONTROL_REQUEST_METHOD" => "POST",
                            "connectrpc.test.trace" => trace)
    expect(response.status).to eq(204)
    expect(trace).to be_empty
    expect(BaseWebEchoController.allow_forgery_protection).to be(true)
    invalid = mock.post("/base-web/example.v1.EchoService/Echo",
                        "HTTP_HOST" => "www.example.com",
                        "CONTENT_TYPE" => "application/grpc-web",
                        input: [0, 0].pack("CN"))
    expect(invalid.status).to eq(200)
    expect(invalid.body).to include("grpc-status: 13\r\n")

    token = mock.get("/base/ordinary",
                     "HTTP_HOST" => "www.example.com")
    cookies = Array(token["set-cookie"]).map { |entry| entry.split(";", 2).first }.join("; ")
    valid = mock.post("/base-web/example.v1.EchoService/Echo",
                      "HTTP_HOST" => "www.example.com",
                      "CONTENT_TYPE" => "application/grpc-web",
                      "HTTP_ORIGIN" => "http://www.example.com",
                      "HTTP_COOKIE" => cookies,
                      "HTTP_X_CSRF_TOKEN" => JSON.parse(token.body).fetch("token"),
                      input: [0, 0].pack("CN"))
    expect(valid.body).to include("grpc-status: 0\r\n")
    expect(valid["x-base"]).to eq(["before"])
    expect(valid.body).to include("x-base: after\r\n")
  end
end
