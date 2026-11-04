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

  def web_call(verb: "POST", origin: "https://browser.example", **env)
    headers = { "HTTP_HOST" => "www.example.com", "CONTENT_TYPE" => "application/grpc-web",
                "HTTP_ACCESS_CONTROL_REQUEST_METHOD" => "POST" }
    headers["HTTP_ORIGIN"] = origin if origin
    Rack::MockRequest.new(RailsIntegration::APP).request(
      verb, "/web/example.v1.EchoService/Echo", **headers, input: [0, 0].pack("CN"), **env
    )
  end

  before { RailsIntegration::APP.eager_load! }

  %w[POST OPTIONS].product(["https://browser.example", "https://denied.example", nil]).each do |verb, origin|
    it "rejects conflicting default credentials before #{verb} publication with Origin #{origin.inspect}" do
      allow_any_instance_of(WebEchoController).to receive(:connect_cors).and_return(
        ConnectRPC::GRPCWebCORS.new(origins: ["https://browser.example"])
      )
      original = ActionDispatch::Response.default_headers
      ActionDispatch::Response.default_headers = original.merge("access-control-allow-credentials" => "true")
      expect { web_call(verb: verb, origin: origin) }
        .to raise_error(ConnectRPC::Error, "Conflicting application CORS policy")
    ensure
      ActionDispatch::Response.default_headers = original if original
    end
  end

  [false, true].product([false, true]).each do |host_callback, processing_failure|
    it "keeps #{host_callback ? 'callback' : 'body'} ownership on CORS failure, error=#{processing_failure}" do
      outcomes = []
      completions = []
      observer = Object.new
      observer.define_singleton_method(:processing_finished) { |outcome| outcomes << outcome }
      observer.define_singleton_method(:finished) { |completion| completions << completion }
      subscriber = Object.new
      subscriber.define_singleton_method(:start) { |_| observer }
      allow_any_instance_of(WebEchoController).to receive(:connect_instrumentation).and_return(
        ConnectRPC::Instrumentation.new(subscribers: [subscriber])
      )
      conflict = -> { response.set_header("access-control-allow-origin", ["*"]) }
      WebEchoController.before_action(conflict, only: :connect)
      failure = RuntimeError.new("original handler failure")
      allow_any_instance_of(WebEchoController).to receive(:rpc_echo).and_raise(failure) if processing_failure
      callbacks = []
      env = host_callback ? { "rack.response_finished" => callbacks } : {}
      escaped = nil
      expect { web_call(**env) }.to raise_error(ConnectRPC::Error, "Conflicting application CORS policy") { |e| escaped = e }
      expect(outcomes.size).to eq(1)
      expect(outcomes.first.code).to eq(processing_failure ? "internal" : "OK")
      expect(outcomes.first.error).to equal(processing_failure ? failure : nil)
      if host_callback
        expect(completions).to be_empty
        expect(callbacks).not_to be_empty
        2.times { callbacks.reverse_each { |callback| callback.call(env, 500, {}, escaped) } }
      end
      expect(completions.size).to eq(1)
      expect(completions.first.delivery).to eq(:aborted)
      expect(completions.first.error).to equal(escaped)
      expect(completions.first.outcome).to equal(outcomes.first)
    ensure
      WebEchoController.skip_before_action(conflict, only: :connect) if conflict
    end
  end

  it "rejects callback credential sharing omitted by a false core policy" do
    allow_any_instance_of(WebEchoController).to receive(:connect_cors).and_return(
      ConnectRPC::GRPCWebCORS.new(origins: ["https://browser.example"])
    )
    conflict = -> { response.set_header("access-control-allow-credentials", ["true"]) }
    WebEchoController.before_action(conflict, only: :connect)
    expect { web_call }.to raise_error(ConnectRPC::Error, "Conflicting application CORS policy")
  ensure
    WebEchoController.skip_before_action(conflict, only: :connect) if conflict
  end

  it "allows identical policy fields while preserving security, cookies and normal/error callbacks" do
    identical = -> { response.set_header("access-control-allow-credentials", ["true"]) }
    WebEchoController.before_action(identical, only: :connect)
    %w[cookie argument].each do |scenario|
      trace = []
      result = web_call("HTTP_X_CASE" => scenario, "connectrpc.test.trace" => trace)
      expect(result.status).to eq(200)
      expect(result.body).to include("grpc-status: #{scenario == 'cookie' ? 0 : 3}\r\n")
      expect(result["access-control-allow-credentials"]).to eq(["true"])
      expect(trace.map(&:first)).to include(:before, :handler, :around_ensure)
      next unless scenario == "cookie"

      expect(result["content-security-policy"]).to eq("default-src 'none'")
      expect(result["set-cookie"]).to eq(["first=1; path=/", "second=2; path=/"])
    end
  ensure
    WebEchoController.skip_before_action(identical, only: :connect) if identical
  end

  it "preserves application CORS with cors:nil and on mixed Connect GET" do
    original = ActionDispatch::Response.default_headers
    ActionDispatch::Response.default_headers = original.merge("access-control-allow-credentials" => "true")
    allow_any_instance_of(WebEchoController).to receive(:connect_cors).and_return(nil)
    expect(web_call["access-control-allow-credentials"]).to eq("true")
    allow_any_instance_of(WebEchoController).to receive(:connect_cors).and_call_original
    allow_any_instance_of(EchoController).to receive(:connect_protocol).and_return(%i[connect grpc_web])
    allow_any_instance_of(EchoController).to receive(:connect_status_codec).and_return(ConnectRPC::Bindings::GoogleProtobuf.new)
    allow_any_instance_of(EchoController).to receive(:connect_cors).and_return(
      ConnectRPC::GRPCWebCORS.new(origins: ["https://browser.example"])
    )
    result = Rack::MockRequest.new(RailsIntegration::APP).get(
      "/example.v1.EchoService/Echo?encoding=proto&base64=1&message=",
      "HTTP_HOST" => "www.example.com", "HTTP_ORIGIN" => "https://denied.example",
      "CONTENT_TYPE" => "application/grpc-web"
    )
    expect(result.status).to eq(200)
    expect(result["access-control-allow-credentials"]).to eq("true")
  ensure
    ActionDispatch::Response.default_headers = original if original
  end
end
