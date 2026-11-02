# frozen_string_literal: true

require "simplecov"
ROOT = File.expand_path("../../..", __dir__)
SimpleCov.root(ROOT)
SimpleCov.coverage_dir("coverage/otel/#{RUBY_VERSION}")
SimpleCov.start do
  enable_coverage :branch
  merging false
  skip "/spec/"
  skip "/scripts/"
  cover "lib/**/*.rb"
  cover "gems/*/lib/**/*.rb"
end

require "connectrpc/otel"
require "connectrpc/testing"
require "connectrpc/rack"
require "connectrpc/falcon"
require "connectrpc/bindings/google_protobuf"
require "opentelemetry/sdk"
require "opentelemetry/sdk/trace/export/in_memory_span_exporter"
require "tmpdir"
$LOAD_PATH.unshift(File.join(ROOT, "spec/fixtures/generation/echo"))
require "echo_connect"
require_relative "../../connectrpc-async/spec/support/helpers"
require_relative "../../connectrpc-async/spec/support/hosting_helpers"
require_relative "../../connectrpc-async/spec/support/frame_gate"
require_relative "../../connectrpc-falcon/spec/support/helpers"

module TraceHelpers
  def tracing(request_metadata: [], response_metadata: [], subscribers: [])
    exporter = OpenTelemetry::SDK::Trace::Export::InMemorySpanExporter.new
    provider = OpenTelemetry::SDK::Trace::TracerProvider.new
    provider.add_span_processor(OpenTelemetry::SDK::Trace::Export::SimpleSpanProcessor.new(exporter))
    subscriber = ConnectRPC::OTel::Subscriber.new(tracer_provider: provider, request_metadata: request_metadata,
                                                  response_metadata: response_metadata)
    [provider, exporter, ConnectRPC::Instrumentation.new(subscribers: [subscriber, *subscribers])]
  end

  def rpc_spans(exporter)
    exporter.finished_spans.select { |span| span.instrumentation_scope.name == "connectrpc-otel" }
  end

  def current_span = OpenTelemetry::Trace.current_span
end

RSpec.configure do |config|
  config.include AsyncHelpers
  config.include AsyncHostingHelpers
  config.include FalconHelpers
  config.include TraceHelpers
  config.order = :random
  config.disable_monkey_patching!
end
