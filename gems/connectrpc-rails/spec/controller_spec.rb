# frozen_string_literal: true

require "json"
require "timeout"
require "zlib"

RSpec.describe ConnectRPC::Rails::Controller do
  let(:binding) { ConnectRPC::Bindings::GoogleProtobuf.new }
  let(:type) { Example::V1::EchoMessage }
  let(:service) { Example::V1::EchoService }
  let(:app) { RailsIntegration::APP }

  def rpc(path = "/example.v1.EchoService/Echo", codec: :proto, body: nil, **options)
    body ||= binding.encode(type.new(name: "Jim", count: 42), type: type, codec: codec)
    Rack::MockRequest.new(app).post(path, { "CONTENT_TYPE" => "application/#{codec == :proto ? 'proto' : 'json'}",
                                            "HTTP_HOST" => "www.example.com", input: body }.merge(options))
  end

  it "rejects streaming media and declarations before binding or entering the Rails action lifecycle" do
    rpc = ConnectRPC::ServerStreamMethod.new(service: service::SERVICE.name, name: "Watch",
                                             request_type: type, response_type: type)
    mixed = ConnectRPC::Service.new(name: rpc.service, methods: [*service::SERVICE.methods, rpc])
    allow_any_instance_of(EchoController).to receive(:connect_service).and_return(mixed)
    allow_any_instance_of(EchoController).to receive(:connect_binding).and_return(binding)
    expect_any_instance_of(EchoController).not_to receive(:connect)
    expect(binding).not_to receive(:decode)
    expect(binding).not_to receive(:encode)
    { "application/connect+proto" => 415, "application/proto" => 501 }.each do |media, status|
      response = rpc(rpc.path, body: "must not read", "CONTENT_TYPE" => media)
      expect(response.status).to eq(status)
    end
  end

  it "runs generated typed binary and JSON clients through real Rails with presence, metadata and details" do
    transport = lambda do |request|
      env = Rack::MockRequest.env_for(request.url.to_s, method: request.http_method, input: request.body)
      env["HTTP_HOST"] = request.url.host
      request.headers.each do |key, values|
        env[key == "content-type" ? "CONTENT_TYPE" : "HTTP_#{key.upcase.tr('-', '_')}"] = values.join(", ")
      end
      status, headers, body = Rack::Lint.new(app).call(env)
      bytes = +""
      body.each { |chunk| bytes << chunk }
      body.close if body.respond_to?(:close)
      ConnectRPC::TransportResponse.new(status: status, headers: headers, body: bytes)
    end
    %i[proto json].product(%i[identity gzip]).each do |codec, encoding|
      core = ConnectRPC::Client.new(base_url: "http://www.example.com/rpc", binding: binding,
                                    codec: codec, transport: transport, encoding: encoding)
      client = service::Client.new(client: core)
      result = client.echo(type.new(name: "", count: 4_294_967_296, data: "\x00\xff".b),
                           metadata: { "x-gzip" => ["yes"] })
      expect(result.message.has_name?).to be_truthy
      expect(result.message.name).to eq("")
      expect(result.message.count).to eq(4_294_967_296)
      expect(result.message.data).to eq("\x00\xff".b)
      expect(result.metadata["x-leading"]).to eq(%w[one two])
      expect(result.trailers["x-trailing"]).to eq(["three"])
      expect do
        client.fail(type.new(name: "detail"), metadata: { "x-gzip" => ["yes"] })
      end.to raise_error(ConnectRPC::Error) { |error|
               expect(error.code).to eq("invalid_argument")
               expect(error.message).to eq("safe failure")
               expect(error.metadata["x-error"]).to eq(%w[
                                                         one two
                                                       ])
               expect(error.trailers["x-end"]).to eq(%w[
                                                       three four
                                                     ])
               expect(error.details.first.decode(
                 binding: binding, type: type
               ).name).to eq("detail")
             }
    end
  end

  it "exposes only connect as the concern's public action and keeps hooks/accessors/handlers private" do
    expect(described_class.public_instance_methods(false)).to eq([:connect])
    private_methods = %i[connect_service connect_binding connect_handlers connect_limits connect_request
                         connect_context connect_method]
    [EchoController, BaseEchoController].each do |controller|
      expect(controller.action_methods).to eq(Set.new(%w[connect ordinary]))
      expect(controller.private_instance_methods).to include(*private_methods, :rpc_echo)
    end
  end

  it "runs native callbacks in order with the same typed objects and emits instrumentation" do
    trace = []
    payloads = []
    subscriber = ActiveSupport::Notifications.subscribe("process_action.action_controller") do |*args|
      payloads << args.last
    end
    response = rpc("/rpc/example.v1.EchoService/Echo?query=hello", "connectrpc.test.trace" => trace,
                                                                   "HTTP_X_CASE" => "params")
    expect(response.status).to eq(200)
    expect(response["x-params"]).to eq(["hello|hello|Echo|Echo"])
    expect(trace.map(&:first)).to eq(%i[before around_before handler after around_ensure])
    before, handler, after = trace.values_at(0, 2, 3)
    expect(before.drop(1)).to eq(handler.drop(1))
    expect(after.drop(1)).to eq(handler.drop(1))
    expect(before[1]).to be_a(type)
    expect(before[2]).to be_a(ConnectRPC::Context)
    expect(before[3]).to equal(service::ECHO)
    expect(payloads.last).to include(action: "connect", status: 200)
  ensure
    ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
  end

  it "runs around ensure and native rescue mappings without leaking exception text" do
    trace = []
    response = rpc("connectrpc.test.trace" => trace, "HTTP_X_CASE" => "argument")
    expect(JSON.parse(response.body)).to include("code" => "invalid_argument", "message" => "mapped safely")
    expect(trace.map(&:first)).to eq(%i[before around_before handler around_ensure])
    response = rpc("/rescue/example.v1.EchoService/Fail")
    expect(JSON.parse(response.body)).to include("code" => "invalid_argument", "message" => "safe failure")
  end

  it "rejects callback authorization and ignores typed success after a native non-200 halt" do
    expect(JSON.parse(rpc("HTTP_X_CASE" => "auth").body)).to include("code" => "unauthenticated",
                                                                     "message" => "Sign in")
    expect(JSON.parse(rpc("HTTP_X_CASE" => "after-halt").body)).to include("code" => "permission_denied")
  end

  it "maps only unambiguous native HTTP statuses and fails closed on raw success, redirect, and ambiguous statuses" do
    statuses = {
      401 => "unauthenticated", 403 => "permission_denied", 404 => "not_found", 429 => "resource_exhausted",
      499 => "canceled", 501 => "unimplemented", 503 => "unavailable", 504 => "deadline_exceeded",
      200 => "internal", 400 => "internal", 409 => "internal", 500 => "internal", 502 => "internal"
    }
    statuses.each do |status, code|
      response = rpc("HTTP_X_CASE" => "raw", "HTTP_X_STATUS" => status.to_s)
      expect(JSON.parse(response.body)["code"]).to eq(code), "HTTP #{status}: #{response.body}"
      expect(response.body).not_to include("private Rails body")
    end
    expect(JSON.parse(rpc("HTTP_X_CASE" => "redirect").body)["code"]).to eq("internal")
  end

  it "handles the framework's real native rate limiter in this Rails lane" do
    response = rpc("/rate/example.v1.EchoService/Echo")
    expect(response.status).to eq(429)
    expect(JSON.parse(response.body)["code"]).to eq("resource_exhausted")
  end

  it "reports the original unexpected application error and backtrace through Rails.error" do
    reports = []
    reporter = Object.new
    reporter.define_singleton_method(:report) { |error, **options| reports << [error, options] }
    Rails.error.subscribe(reporter)
    response = rpc("HTTP_X_CASE" => "bug")
    expect(JSON.parse(response.body)).to include("code" => "internal", "message" => "Internal error")
    report = reports.find { |error, options| error.message == "secret bug" && options[:source] == "connectrpc.rails" }
    expect(report).not_to be_nil
    expect(reports.count { |_error, options| options[:source] == "connectrpc.rails" }).to eq(1)
    expect(report.first.backtrace.join).to include("rpc_echo")
    expect(report.last[:handled]).to be(false)
  ensure
    Rails.error.unsubscribe(reporter) if reporter
  end

  it "preserves unknown deeper suffixes, raw method case/escapes, and unrelated routes" do
    %w[Unknown echo Echo/extra Echo/extra/Echo %45cho].each do |suffix|
      response = rpc("/rpc/example.v1.EchoService/#{suffix}")
      expect(response.status).to eq(501), "#{suffix}: #{response.status} #{response.body}"
      expect(JSON.parse(response.body)["code"]).to eq("unimplemented")
    end
    expect(rpc("/example.v1.EchoService").status).to eq(501)
    expect(rpc("/rpc/example.v1.EchoService//Echo/").status).to eq(200)
    expect { Rack::MockRequest.new(app).get("/unrelated", "HTTP_HOST" => "www.example.com") }
      .to raise_error(ActionController::RoutingError)
    expect { rpc("/wrong/other.Service/Echo") }.to raise_error(ArgumentError, /configured service/)
  end

  it "ignores malformed Accept and JSON body params while preserving ordinary controller parsing" do
    response = rpc(codec: :json, "HTTP_ACCEPT" => "invalid; /;\"", "HTTP_X_CASE" => "params")
    expect(response.status).to eq(200)
    response = rpc("/example.Scalar/Echo", codec: :json, body: '"scalar"', "HTTP_ACCEPT" => "text/javascript")
    expect(response.status).to eq(200)
    expect(JSON.parse(response.body)).to eq("scalar")
    response = rpc("/example.Array/Echo", codec: :json, body: '["array", 42]')
    expect(response.status).to eq(200)
    expect(JSON.parse(response.body)).to eq(["array", 42])
    ordinary = Rack::MockRequest.new(app).post("/ordinary", "HTTP_HOST" => "www.example.com",
                                                            "CONTENT_TYPE" => "application/json",
                                                            input: '{"name":"normal"}')
    expect(JSON.parse(ordinary.body)).to include("name" => "normal")
  end

  it "dispatches gzip JSON scalar through native parser suppression" do
    response = rpc("/example.Scalar/Echo", codec: :json, body: Zlib.gzip('"scalar"'),
                                           "HTTP_CONTENT_ENCODING" => "gzip", "HTTP_X_GZIP" => "yes",
                                           "HTTP_ACCEPT_ENCODING" => "gzip")
    expect(response.status).to eq(200)
    expect(response["content-encoding"]).to eq(["gzip"])
    expect(JSON.parse(Zlib.gunzip(response.body))).to eq("scalar")
  end

  it "keeps real Base forgery protection and ordinary Base actions" do
    expect(BaseEchoController.allow_forgery_protection).to be(true)
    expect(app.config.action_controller.default_protect_from_forgery).to be(true)
    response = rpc("/base/example.v1.EchoService/Echo")
    expect(JSON.parse(response.body)["code"]).to eq("internal")
    expect(response.status).to eq(500) # Rails' 422 has no unambiguous Connect inverse.
    expect { Rack::MockRequest.new(app).post("/base/ordinary", "HTTP_HOST" => "www.example.com") }
      .to raise_error(ActionController::InvalidAuthenticityToken)
    ordinary = Rack::MockRequest.new(app).get("/base/ordinary?name=normal", "HTTP_HOST" => "www.example.com")
    expect(JSON.parse(ordinary.body)["name"]).to eq("normal")
    expect(ordinary["set-cookie"]).to include(a_string_including("ordinary=yes"))
  end

  it "applies exact wire content types and preserves host headers, repeated cookies and Rack 3 metadata" do
    %i[proto json].each do |codec|
      lint = Rack::Lint.new(app)
      input = binding.encode(type.new(name: "Jim"), type: type, codec: codec)
      env = Rack::MockRequest.env_for("/example.v1.EchoService/Echo", method: "POST", input: input)
      env.merge!("CONTENT_TYPE" => "application/#{codec == :proto ? 'proto' : 'json'}",
                 "HTTP_HOST" => "www.example.com", "HTTP_X_CASE" => "cookie", "HTTP_X_REQUEST_ID" => "host-id")
      status, headers, body = lint.call(env)
      expect(status).to eq(200)
      expect(headers["content-type"]).to eq(env["CONTENT_TYPE"])
      expect(headers["x-leading"]).to eq(%w[one two])
      expect(headers["set-cookie"]).to eq(["first=1; path=/", "second=2; path=/"])
      expect(headers["content-security-policy"]).to eq("default-src 'none'")
      expect(headers["www-authenticate"]).to eq('Bearer realm="rpc"')
      expect(headers["x-content-type-options"]).to eq("nosniff")
      expect(headers["x-request-id"]).to eq("host-id")
      %w[content-encoding last-modified location].each { |key| expect(headers).not_to have_key(key) }
      bytes = +""
      body.each { |chunk| bytes << chunk }
      expect(headers["etag"]).not_to eq("stale")
      expect(headers["content-length"].to_i).to eq(bytes.bytesize) if headers.key?("content-length")
      body.close
    end
  end
  it "preserves real Rails Context execution without interpreting a token in the request env" do
    source = ConnectRPC::CancellationSource.new
    source.cancel
    trace = []
    response = rpc("/example.v1.EchoService/Echo", "connectrpc.test.trace" => trace,
                                                   "connectrpc.cancellation" => source.token)
    expect(response.status).to eq(200)
    handler = trace.find { |entry| entry.first == :handler }
    expect(handler).not_to be_nil
    context = handler[2]
    expect(context.cancellation_reason).to be_nil
    expect { context.check! }.not_to raise_error
  end
end
