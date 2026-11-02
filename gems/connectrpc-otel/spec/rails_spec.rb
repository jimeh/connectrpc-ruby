# frozen_string_literal: true

require "connectrpc/rails"
ENV["CONNECTRPC_ROOT"] = ROOT
require_relative "../../connectrpc-rails/spec/support/application"

RSpec.describe "Native Rails RPC spans" do
  let(:telemetry) { tracing }
  let(:provider) { telemetry[0] }
  let(:exporter) { telemetry[1] }
  let(:bus) { telemetry[2] }

  it "activates callbacks and rescue_from, preserves original escaping failures, and leaves ordinary actions alone" do
    controller = EchoController
    outcomes = []
    recorder = Object.new
    recorder.define_singleton_method(:start) do |_call|
      observer = Object.new
      observer.define_singleton_method(:processing_finished) { |outcome| outcomes << outcome }
      observer
    end
    _provider, recorded_exporter, instrumentation = tracing(subscribers: [recorder])
    seen = []
    controller.class_eval do
      define_method(:connect_instrumentation) { instrumentation }
      private :connect_instrumentation
    end
    callback = -> { seen << OpenTelemetry::Trace.current_span.context }
    controller.before_action(callback, only: :connect)
    mock = Rack::MockRequest.new(RailsIntegration::APP)
    message = AsyncHelpers::MESSAGE.new(name: "rails")
    [nil, "argument", "bug"].each do |scenario|
      response = mock.post(AsyncHelpers::ECHO::ECHO.path,
                           "CONTENT_TYPE" => "application/proto", "HTTP_X_CASE" => scenario,
                           "HTTP_HOST" => "www.example.com", input: message.to_proto)
      expect(response.status).to eq(if scenario.nil?
                                      200
                                    else
                                      scenario == "argument" ? 400 : 500
                                    end)
      span = rpc_spans(recorded_exporter).last
      expect(seen.last.span_id).to eq(span.span_id)
      expected_code = { nil => "OK", "argument" => "invalid_argument", "bug" => "internal" }.fetch(scenario)
      expect(span.attributes["rpc.response.status_code"]).to eq(expected_code)
      expect(span.status.code).to eq(scenario == "bug" ? OpenTelemetry::Trace::Status::ERROR : OpenTelemetry::Trace::Status::UNSET)
      expect(Array(span.events)).to be_empty
      expect(span.attributes.to_s).not_to include("secret bug", "secret exception")
      expect(current_span.context).not_to be_valid
      original = outcomes.last.error
      if scenario == "bug"
        expect(original).to be_a(RuntimeError)
        expect(original.message).to eq("secret bug")
        expect(outcomes.last.normalized_error.code).to eq("internal")
      elsif scenario == "argument"
        expect(original).to be_a(ConnectRPC::Error)
        expect(original.code).to eq("invalid_argument")
      else
        expect(original).to be_nil
      end
    end
    expect(mock.post("/ordinary", "HTTP_HOST" => "www.example.com").status).to eq(200)
    expect(rpc_spans(recorded_exporter).size).to eq(3)
    expect(controller.action_methods).not_to include("connect_instrumentation")
  ensure
    controller&.skip_before_action(callback, only: :connect) if callback
    controller&.class_eval { remove_method :connect_instrumentation }
  end
end

RSpec.configure do |config|
  config.after(:suite) { FileUtils.remove_entry(RailsIntegration::ROOT) if File.directory?(RailsIntegration::ROOT) }
end
