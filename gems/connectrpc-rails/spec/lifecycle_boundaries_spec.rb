# frozen_string_literal: true

require "json"
require "open3"
require "timeout"
require "zlib"

RSpec.describe "Rails lifecycle boundaries" do
  let(:app) { RailsIntegration::APP }
  let(:binding) { ConnectRPC::Bindings::GoogleProtobuf.new }
  let(:type) { Example::V1::EchoMessage }

  def dispatch(input: "", path: "/example.v1.EchoService/Echo", **headers)
    env = Rack::MockRequest.env_for(path, method: "POST")
    env.merge!("CONTENT_TYPE" => "application/proto", "HTTP_HOST" => "www.example.com", "rack.input" => input)
    env.delete("CONTENT_LENGTH")
    env.merge!(headers)
    status, fields, body = app.call(env)
    bytes = +""
    body.each { |chunk| bytes << chunk }
    body.close if body.respond_to?(:close)
    [status, fields, bytes]
  end

  it "rejects preflight errors before callbacks or controller instrumentation" do
    trace = []
    events = []
    subscriber = ActiveSupport::Notifications.subscribe("process_action.action_controller") do |*args|
      events << args.last
    end
    cases = [
      [{ "CONTENT_TYPE" => "application/grpc" }, 415, nil],
      [{ "REQUEST_METHOD" => "GET" }, 405, nil],
      [{ "HTTP_CONTENT_ENCODING" => "gzip" }, 501, "unimplemented"],
      [{ "HTTP_CONNECT_PROTOCOL_VERSION" => "2" }, 400, "invalid_argument"],
      [{ "HTTP_CONNECT_TIMEOUT_MS" => "bad" }, 400, "invalid_argument"]
    ]
    cases.each do |headers, expected, code|
      status, _fields, bytes = dispatch(**headers, "connectrpc.test.trace" => trace)
      expect(status).to eq(expected)
      expect(JSON.parse(bytes)["code"]).to eq(code) if code
    end
    status, _fields, bytes = dispatch(input: StringIO.new("{"), "CONTENT_TYPE" => "application/json",
                                      "connectrpc.test.trace" => trace)
    expect(status).to eq(400)
    expect(JSON.parse(bytes)["code"]).to eq("invalid_argument")
    expect(trace).to be_empty
    expect(events).to be_empty
  ensure
    ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
  end

  it "rejects malformed and inflated-oversize gzip before native lifecycle or instrumentation" do
    trace = []
    events = []
    subscriber = ActiveSupport::Notifications.subscribe("process_action.action_controller") { |*args| events << args.last }
    [[Zlib.gzip("valid")[0...-1], "", 400, "invalid_argument"],
     [Zlib.gzip("x" * 1000), "gzip-inflated-limit", 429, "resource_exhausted"]].each do |body, scenario, status, code|
      result, headers, bytes = dispatch(input: StringIO.new(body), "HTTP_X_GZIP" => "yes", "HTTP_X_CASE" => scenario,
                                        "HTTP_CONTENT_ENCODING" => "gzip", "connectrpc.test.trace" => trace)
      expect(result).to eq(status)
      expect(headers).not_to have_key("content-encoding")
      expect(JSON.parse(bytes)["code"]).to eq(code)
    end
    expect(trace).to be_empty
    expect(events).to be_empty
  ensure
    ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
  end

  it "does not rerun native execution when compressed response exceeds its wire cap" do
    trace = []
    events = []
    subscriber = ActiveSupport::Notifications.subscribe("process_action.action_controller") { |*args| events << args.last }
    # Deterministic varied bytes ensure the encoded response exceeds 80 bytes.
    message = type.new(name: (32..126).map(&:chr).join)
    input = Zlib.gzip(binding.encode(message, type: type, codec: :proto))
    status, headers, bytes = dispatch(input: StringIO.new(input), "HTTP_X_GZIP" => "yes",
                                      "HTTP_X_CASE" => "gzip-wire-limit", "HTTP_CONTENT_ENCODING" => "gzip",
                                      "connectrpc.test.trace" => trace)
    expect(status).to eq(429)
    expect(JSON.parse(bytes)["code"]).to eq("resource_exhausted")
    expect(headers).not_to have_key("content-encoding")
    expect(trace.map(&:first)).to eq(%i[before around_before handler after around_ensure])
    expect(events.size).to eq(1)
    expect(events.first[:status]).to eq(200)
  ensure
    ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
  end

  it "bounds declared and unknown-length input without any eager Rails read or rewind" do
    lengths = []
    input = Object.new
    input.define_singleton_method(:read) do |length|
      lengths << length
      "x" * length
    end
    status, = dispatch(input: input, "CONTENT_LENGTH" => "99", "HTTP_X_CASE" => "request-limit")
    expect(status).to eq(429)
    expect(lengths).to be_empty
    status, = dispatch(input: input, "HTTP_TRANSFER_ENCODING" => "chunked", "HTTP_X_CASE" => "request-limit")
    expect(status).to eq(429)
    expect(lengths).to eq([9])

    # Valid body is read exactly once by bounded MessageIO, including its EOF read.
    bytes = binding.encode(type.new(name: "bounded"), type: type, codec: :proto)
    stream = StringIO.new(bytes)
    lengths.clear
    input.define_singleton_method(:read) do |length|
      lengths << length
      stream.read(length)
    end
    status, = dispatch(input: input, "HTTP_TRANSFER_ENCODING" => "chunked")
    expect(status).to eq(200)
    expect(stream.pos).to eq(bytes.bytesize)
    expect(lengths).to eq([16_384, 16_384])
  end

  it "bounds encoding after successful native execution and exposes that instrumentation boundary" do
    payloads = []
    subscriber = ActiveSupport::Notifications.subscribe("process_action.action_controller") do |*args|
      payloads << args.last
    end
    input = StringIO.new(binding.encode(type.new(name: "x" * 200), type: type, codec: :proto))
    status, _fields, bytes = dispatch(input: input, "HTTP_X_CASE" => "response-limit")
    expect(status).to eq(429)
    expect(JSON.parse(bytes)["code"]).to eq("resource_exhausted")
    expect(payloads.last[:status]).to eq(200)
    dispatch(input: StringIO.new(""), "HTTP_X_CASE" => "auth")
    expect(payloads.last[:exception].first).to eq("ConnectRPC::Error")
  ensure
    ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
  end

  it "checks cooperative deadline expiry after native lifecycle completion" do
    clock = 100.0
    allow(Process).to receive(:clock_gettime).and_call_original
    allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC).and_return(clock)
    expire = lambda do
      allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC).and_return(clock + 1)
    end
    trace = []
    status, _fields, bytes = dispatch(input: StringIO.new(""), "HTTP_CONNECT_TIMEOUT_MS" => "1",
                                      "HTTP_X_CASE" => "deadline",
                                      "connectrpc.test.expire" => expire, "connectrpc.test.trace" => trace)
    expect(status).to eq(504)
    expect(JSON.parse(bytes)["code"]).to eq("deadline_exceeded")
    expect(trace.map(&:first)).to eq(%i[before around_before handler after around_ensure])
  end

  it "dispatches a Base typed request with a real CSRF session/token without changing global security" do
    response = Rack::MockRequest.new(app).get("/base/ordinary", "HTTP_HOST" => "www.example.com")
    token = JSON.parse(response.body).fetch("token")
    cookies = Array(response["set-cookie"]).map { |cookie| cookie.split(";", 2).first }.join("; ")
    %i[proto json].product(%i[identity gzip]).each do |codec, encoding|
      bytes = binding.encode(type.new(name: "Base"), type: type, codec: codec)
      bytes = Zlib.gzip(bytes) if encoding == :gzip
      content_type = "application/#{codec == :proto ? 'proto' : 'json'}"
      status, fields, body = dispatch(path: "/base/example.v1.EchoService/Echo", input: StringIO.new(bytes),
                                      "CONTENT_TYPE" => content_type,
                                      "HTTP_COOKIE" => cookies, "HTTP_X_CSRF_TOKEN" => token, "HTTP_X_GZIP" => "yes",
                                      "HTTP_CONTENT_ENCODING" => encoding.to_s)
      expect(status).to eq(200)
      expect(fields["content-type"]).to eq(content_type)
      expect(fields["x-base"]).to eq(["before"])
      expect(fields["trailer-x-base"]).to eq(["after"])
      expect(fields["content-encoding"]).to eq(["gzip"]) if encoding == :gzip
      body = Zlib.gunzip(body) if encoding == :gzip
      expect(binding.decode(body, type: type, codec: codec).name).to eq("Base")
    end
  end

  it "keeps concurrent typed requests, contexts and metadata isolated with observable synchronization" do
    sync = { entered: Queue.new, release: Queue.new }
    threads = %w[first second].map do |name|
      Thread.new do
        input = StringIO.new(binding.encode(type.new(name: name), type: type, codec: :proto))
        dispatch(input: input, "HTTP_X_CASE" => "concurrent", "connectrpc.test.sync" => sync)
      end
    end
    entered = Timeout.timeout(10) { [sync[:entered].pop, sync[:entered].pop] }
    expect(entered.map(&:first).sort).to eq(%w[first second])
    expect(entered.map { |entry| entry.last.object_id }.uniq.size).to eq(2)
    2.times { sync[:release].push(true) }
    results = threads.map { |thread| Timeout.timeout(10) { thread.value } }
    expect(results.map { |status, _headers, _bytes| status }).to eq([200, 200])
    expect(results.map do |_status, _headers, bytes|
      binding.decode(bytes, type: type, codec: :proto).name
    end).to eq(%w[first second])
    expect(results.map { |_status, headers, _bytes| headers["x-leading"] }).to eq([%w[one two], %w[one two]])
  ensure
    2.times { sync[:release].push(true) } if sync
    threads&.each { |thread| thread.join(10) || thread.kill }
  end

  it "eager-loads real controller paths and re-resolves reloadable controllers without reloading generated code" do
    app.eager_load!
    controller = EchoController
    generated = Example::V1::EchoService::ECHO
    file = File.join(RailsIntegration::ROOT, "app/controllers/echo_controller.rb")
    original = File.read(file)
    File.write(file, original.sub('context.response_trailers.add("x-trailing", "three")', 'message.name = "reloaded"'))
    app.reloader.reload!
    status, _headers, bytes = dispatch(input: StringIO.new(""))
    expect(status).to eq(200)
    expect(binding.decode(bytes, type: type, codec: :proto).name).to eq("reloaded")
    expect(EchoController).not_to equal(controller)
    expect(Example::V1::EchoService::ECHO).to equal(generated)
  ensure
    if original
      File.write(file, original)
      app.reloader.reload!
    end
  end

  it "requires the optional adapter without eagerly loading Base/API or Google" do
    code = <<~CODE
      require "connectrpc/rails"
      abort "Base eagerly loaded" unless ActionController.autoload?(:Base)
      abort "API eagerly loaded" unless ActionController.autoload?(:API)
      abort "Google eagerly loaded" if defined?(Google::Protobuf)
      puts "lazy require passed"
    CODE
    output, error, status = Open3.capture3(RbConfig.ruby, "-e", code)
    expect(status.success?).to be(true), error
    expect(output).to include("lazy require passed")
  end
end
