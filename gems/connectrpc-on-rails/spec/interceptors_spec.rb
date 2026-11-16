# frozen_string_literal: true

require "json"

RSpec.describe "Rails server interceptors" do
  let(:app) { RailsIntegration::APP }
  let(:binding) { ConnectRPC::Bindings::GoogleProtobuf.new }
  let(:type) { Example::V1::EchoMessage }
  let(:trace) { [] }

  # Records entry and exit into the callbacks' trace. The optional block runs in
  # place of a plain continuation call when a spec needs another shape.
  def interceptor(&around)
    log = trace
    Object.new.tap do |layer|
      layer.define_singleton_method(:server_unary) do |call, &continuation|
        log.push([:interceptor_in, call])
        result = around ? around.call(call, continuation) : continuation.call
        log.push([:interceptor_out])
        result
      end
    end
  end

  def rpc(path = "/intercepted/example.v1.EchoService/Echo", interceptors: [], **headers)
    body = binding.encode(type.new(name: "Jim"), type: type, codec: :proto)
    Rack::MockRequest.new(app).post(path, { "CONTENT_TYPE" => "application/proto", "HTTP_HOST" => "www.example.com",
                                            "connectrpc.test.trace" => trace,
                                            "connectrpc.test.interceptors" => interceptors,
                                            input: body }.merge(headers))
  end

  def error_code(response) = JSON.parse(response.body).fetch("code")

  def controller_events
    events = []
    subscriber = ActiveSupport::Notifications.subscribe("process_action.action_controller") do |*args|
      events << args.last
    end
    yield events
  ensure
    ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
  end

  def rails_reports
    reports = []
    subscriber = Object.new
    subscriber.define_singleton_method(:report) { |error, **options| reports << [error, options] }
    Rails.error.subscribe(subscriber)
    yield reports
  ensure
    Rails.error.unsubscribe(subscriber) if subscriber
  end

  def base_session
    response = Rack::MockRequest.new(app).get("/base/ordinary", "HTTP_HOST" => "www.example.com")
    cookies = Array(response["set-cookie"]).map { |cookie| cookie.split(";", 2).first }.join("; ")
    { "HTTP_COOKIE" => cookies, "HTTP_X_CSRF_TOKEN" => JSON.parse(response.body).fetch("token") }
  end

  it "runs outside the whole Rails process_action chain and shares the handler's Context" do
    controller_events do |events|
      response = rpc(interceptors: [interceptor])
      expect(response.status).to eq(200)
      expect(binding.decode(response.body, type: type, codec: :proto).name).to eq("Jim")
      expect(trace.map(&:first)).to eq(
        %i[interceptor_in before around_before handler after around_ensure interceptor_out]
      )
      call = trace.first.last
      handler = trace.find { |entry| entry.first == :handler }
      expect(call).to be_a(ConnectRPC::ServerUnaryCall)
      expect(call).to have_attributes(protocol: :connect, method: Example::V1::EchoService::ECHO)
      expect(call.context).to equal(handler[2])
      expect(events.last).to include(controller: "InterceptedEchoController", action: "connect", status: 200)
    end
  end

  it "rejects before callbacks, rescue_from, Rails instrumentation and Rails.error" do
    rejecting = interceptor do |_call, _continuation|
      raise ConnectRPC::Error.new(:permission_denied, "Interceptor rejected")
    end
    raising = interceptor { |_call, _continuation| raise ArgumentError, "interceptor bug" }
    outcomes = []
    observer = Object.new
    observer.define_singleton_method(:processing_finished) { |outcome| outcomes << outcome }
    subscriber = Object.new
    subscriber.define_singleton_method(:start) { |_| observer }
    allow_any_instance_of(InterceptedEchoController).to receive(:connect_instrumentation).and_return(
      ConnectRPC::Instrumentation.new(subscribers: [subscriber])
    )
    rails_reports do |reports|
      controller_events do |events|
        response = rpc(interceptors: [rejecting])
        expect(response.status).to eq(403)
        expect(JSON.parse(response.body)).to include("code" => "permission_denied", "message" => "Interceptor rejected")
        expect(trace.map(&:first)).to eq(%i[interceptor_in])
        trace.clear
        # EchoController's rescue_from maps ArgumentError to invalid_argument "mapped safely".
        response = rpc(interceptors: [raising])
        expect(response.status).to eq(500)
        expect(JSON.parse(response.body)).to include("code" => "internal")
        expect(response.body).not_to include("mapped safely", "interceptor bug")
        expect(trace.map(&:first)).to eq(%i[interceptor_in])
        expect(events).to be_empty
      end
      expect(reports.select { |_error, options| options[:source] == "connectrpc.rails" }).to be_empty
    end
    expect(outcomes.last.error).to be_a(ArgumentError).and(have_attributes(message: "interceptor bug"))
  end

  it "still requires CSRF and authentication callbacks for success" do
    finishing = interceptor do |call, continuation|
      result = continuation.call
      call.context.response_metadata.add("x-intercepted", "yes")
      result
    end
    response = rpc("/intercepted/base/example.v1.EchoService/Echo", interceptors: [finishing], **base_session)
    expect(response.status).to eq(200)
    expect(response["x-base"]).to eq(["before"])
    expect(response["trailer-x-base"]).to eq(["after"])
    expect(response["x-intercepted"]).to eq(["yes"])
    expect(trace.map(&:first)).to eq(%i[interceptor_in interceptor_out])
    trace.clear
    response = rpc("/intercepted/base/example.v1.EchoService/Echo", interceptors: [finishing])
    expect(response.status).to eq(500)
    expect(error_code(response)).to eq("internal")
    expect(trace.map(&:first)).to eq(%i[interceptor_in])
    trace.clear
    response = rpc(interceptors: [finishing], "HTTP_X_CASE" => "auth")
    expect(JSON.parse(response.body)).to include("code" => "unauthenticated", "message" => "Sign in")
    expect(trace.map(&:first)).to eq(%i[interceptor_in before])
  end

  it "fails a server interceptor that returns without yielding before any callback or handler" do
    skipping = interceptor { |_call, _continuation| type.new(name: "never sent") }
    response = rpc(interceptors: [skipping])
    expect(response.status).to eq(500)
    expect(error_code(response)).to eq("internal")
    expect(response.body).not_to include("never sent")
    expect(trace.map(&:first)).to eq(%i[interceptor_in interceptor_out])
  end

  it "keeps a Rails rejection when an interceptor rescues it and returns a message" do
    rescued = []
    swallowing = interceptor do |_call, continuation|
      continuation.call
    rescue ConnectRPC::Error => e
      rescued << e
      type.new(name: "fallback")
    end
    full = %i[interceptor_in before around_before handler after around_ensure interceptor_out]
    cases = [
      ["/intercepted/example.v1.EchoService/Echo", { "HTTP_X_CASE" => "raw", "HTTP_X_STATUS" => "401" },
       %i[interceptor_in before interceptor_out], [401, "unauthenticated", "Unauthenticated"]],
      ["/intercepted/base/example.v1.EchoService/Echo", {}, %i[interceptor_in interceptor_out],
       [500, "internal", "Internal error"]],
      ["/intercepted/example.v1.EchoService/Echo", { "HTTP_X_CASE" => "after-halt" }, full,
       [403, "permission_denied", "Permission denied"]]
    ]
    cases.each do |path, headers, steps, rejection|
      status, code, message = rejection
      rescued.clear
      trace.clear
      response = rpc(path, interceptors: [swallowing], **headers)
      expect(trace.map(&:first)).to eq(steps), path
      expect(rescued.size).to eq(1), path
      expect(rescued.first).to be_an_instance_of(ConnectRPC::Error)
        .and(have_attributes(code: code, message: message))
      expect(response.status).to eq(status)
      expect(JSON.parse(response.body)).to include("code" => code, "message" => message)
    end
  end

  it "reports a controller exception once and hands interceptors the normalized Error" do
    seen = []
    observing = interceptor do |_call, continuation|
      continuation.call
    rescue StandardError => e
      seen << e
      raise
    end
    rails_reports do |reports|
      response = rpc(interceptors: [observing], "HTTP_X_CASE" => "bug")
      expect(JSON.parse(response.body)).to include("code" => "internal", "message" => "Internal error")
      expect(response.body).not_to include("secret bug")
      rails = reports.select { |_error, options| options[:source] == "connectrpc.rails" }
      expect(rails.size).to eq(1)
      expect(rails.first.first).to be_a(RuntimeError).and(have_attributes(message: "secret bug"))
    end
    expect(trace.map(&:first)).to eq(%i[interceptor_in before around_before handler around_ensure])
    expect(seen.size).to eq(1)
    expect(seen.first).to be_an_instance_of(ConnectRPC::Error)
      .and(have_attributes(code: "internal", message: "Internal error", cause: nil))
  end
end
