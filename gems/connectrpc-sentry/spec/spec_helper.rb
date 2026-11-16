# frozen_string_literal: true

require "simplecov"
ROOT = File.expand_path("../../..", __dir__)
SimpleCov.root(ROOT)
SimpleCov.coverage_dir("coverage/sentry/#{RUBY_VERSION}")
SimpleCov.start do
  enable_coverage :branch
  merging false
  skip "/spec/"
  skip "/scripts/"
  cover "lib/**/*.rb"
  cover "gems/*/lib/**/*.rb"
end

require "connectrpc/sentry"
require "connectrpc/testing"
require "connectrpc/rack"
require "connectrpc/falcon"
require "connectrpc/bindings/google_protobuf"
require "rack/mock"
require "stringio"
require "tmpdir"
$LOAD_PATH.unshift(File.join(ROOT, "spec/fixtures/generation/echo"))
require "echo_connect"
require_relative "../../connectrpc-async/spec/support/helpers"
require_relative "../../connectrpc-async/spec/support/hosting_helpers"
require_relative "../../connectrpc-async/spec/support/frame_gate"
require_relative "../../connectrpc-async/spec/support/connection_peer"
require_relative "../../connectrpc-async/spec/support/threaded_helpers"
require_relative "../../connectrpc-falcon/spec/support/helpers"

ECHO = Example::V1::EchoService
MESSAGE = Example::V1::EchoMessage

# A real SDK per example: DummyTransport, no worker threads and a .invalid DSN,
# so events stay in memory and nothing reaches the network.
module SentryHelpers
  DSN = "https://public@sentry.invalid/42"

  def sentry(isolation: :thread, sample_rate: 1.0, **settings)
    ::Sentry.close if ::Sentry.initialized?
    log = @sentry_log = StringIO.new
    ::Sentry.init do |config|
      config.dsn = DSN
      config.transport.transport_class = ::Sentry::DummyTransport
      config.background_worker_threads = 0
      config.auto_session_tracking = false
      config.send_client_reports = false
      config.sdk_logger = ::Sentry::Logger.new(log)
      config.traces_sample_rate = sample_rate
      config.hub_isolation_level = isolation
      settings.each { |key, value| config.public_send(:"#{key}=", value) }
      yield config if block_given?
    end
    @sentry_transport = ::Sentry.get_current_client.transport
  end

  def sentry_log = @sentry_log.string
  def sentry_events = @sentry_transport.events.dup
  def transactions = sentry_events.grep(::Sentry::TransactionEvent)
  def error_events = sentry_events.grep(::Sentry::ErrorEvent)
  def trace_of(event) = event.contexts.fetch(:trace)

  def rpc_transactions(operation = nil)
    transactions.select do |event|
      current = trace_of(event)[:op].to_s
      operation ? current == operation : current.start_with?("rpc.")
    end
  end

  def scope_span = ::Sentry.get_current_scope.get_span
  def mechanism(event) = event.to_h.dig(:exception, :values).last.fetch(:mechanism)

  # Failures the core lifecycle reports from subscribers; each example expects none unless it clears them.
  def lifecycle_errors = (@lifecycle_errors ||= [])

  def bus(*subscribers)
    subscribers = [ConnectRPC::Sentry::Subscriber.new] if subscribers.empty?
    ConnectRPC::Instrumentation.new(subscribers: subscribers, on_error: ->(error) { lifecycle_errors << error })
  end

  def start_caller(name = "caller")
    transaction = ::Sentry.start_transaction(name: name, op: "test")
    ::Sentry.get_current_scope.set_span(transaction)
    transaction
  end

  def echo_server(instrumentation:, echo: ->(message, _) { message }, fail: nil, **)
    fail ||= ->(*) { raise ConnectRPC::Error.new(:invalid_argument, "deliberate") }
    ConnectRPC::Server.new(services: [ECHO::SERVICE], binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                           handlers: ECHO.handlers(echo: echo, fail: fail), instrumentation: instrumentation, **)
  end

  def in_process(server, instrumentation: nil, base_url: "https://example.invalid")
    core = ConnectRPC::Client.new(base_url: base_url, binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                                  transport: ConnectRPC::Testing::Transport.new(server: server),
                                  instrumentation: instrumentation)
    ECHO::Client.new(client: core)
  end

  def rack_post(app, name, headers: {}, env: {})
    request = Rack::MockRequest.new(app)
    request.post(ECHO::ECHO.path, { "CONTENT_TYPE" => "application/proto", input: MESSAGE.new(name: name).to_proto }
                 .merge(headers).merge(env))
  end

  # Span ids of every recorded span, transactions included, for duplicate checks.
  def recorded_span_ids
    transactions.flat_map { |event| [trace_of(event)[:span_id], *event.spans.map { |span| span[:span_id] }] }
  end
end

RSpec.configure do |config|
  config.include AsyncHelpers
  config.include AsyncHostingHelpers
  config.include ThreadedHelpers
  config.include FalconHelpers
  config.include SentryHelpers
  config.order = :random
  config.disable_monkey_patching!
  config.before { sentry }
  config.after do
    expect(lifecycle_errors).to be_empty
  ensure
    Sentry.close if Sentry.initialized?
  end
end
