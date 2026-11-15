# frozen_string_literal: true

require "json"
require "stringio"

RSpec.describe "Rails error reporting" do
  let(:app) { RailsIntegration::APP }
  let(:output) { StringIO.new }

  around do |example|
    original = [EchoController, BaseEchoController].to_h { |controller| [controller, controller.logger] }
    original.each_key { |controller| controller.logger = Logger.new(output) }
    example.run
  ensure
    original&.each { |controller, logger| controller.logger = logger }
  end

  def rpc(target, **headers)
    Rack::MockRequest.new(app).post(target, { "CONTENT_TYPE" => "application/proto",
                                              "HTTP_HOST" => "www.example.com", input: "" }.merge(headers))
  end

  def with_reports
    reports = []
    subscriber = Object.new
    subscriber.define_singleton_method(:report) { |error, **options| reports << [error, options] }
    Rails.error.subscribe(subscriber)
    yield reports
  ensure
    Rails.error.unsubscribe(subscriber) if subscriber
  end

  it "keeps native rate-limit and CSRF rejections out of the unhandled error reporter" do
    with_reports do |reports|
      expect(rpc("/rate/example.v1.EchoService/Echo").status).to eq(429)
      expect(rpc("/base/example.v1.EchoService/Echo").status).to eq(500)
      expect(reports).to be_empty
      expect(output.string).not_to include("[connectrpc.rails]")
    end
  end

  it "classifies request preparation failures through the application's configured Rails responses" do
    original = ActionDispatch::ExceptionWrapper.rescue_responses
    ActionDispatch::ExceptionWrapper.rescue_responses = original.merge(
      "ActionController::BadRequest" => :service_unavailable
    )
    with_reports do |reports|
      response = rpc("/example.v1.EchoService/Echo?a=%E0%A4%A")
      expect(response.status).to eq(503)
      expect(JSON.parse(response.body)["code"]).to eq("unavailable")
      expect(reports).to be_empty
      expect(output.string).not_to include("[connectrpc.rails]")
    end
  ensure
    ActionDispatch::ExceptionWrapper.rescue_responses = original if original
  end

  it "logs an unexpected original exception and backtrace once without requiring an error subscriber" do
    response = rpc("/example.v1.EchoService/Echo", "HTTP_X_CASE" => "bug")
    expect(JSON.parse(response.body)).to include("code" => "internal", "message" => "Internal error")
    expect(output.string).to include("RuntimeError", "rpc_echo")
    expect(output.string.scan("secret bug").size).to eq(1)
    expect(output.string.scan("[connectrpc.rails]").size).to eq(1)
    expect(response.body).not_to include("secret bug")
  end
end
