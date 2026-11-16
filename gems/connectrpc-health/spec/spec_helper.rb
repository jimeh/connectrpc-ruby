# frozen_string_literal: true

require "simplecov"
ROOT = File.expand_path("../../..", __dir__)
SimpleCov.root(ROOT)
SimpleCov.coverage_dir("coverage/health/#{RUBY_VERSION}")
SimpleCov.start do
  enable_coverage :branch
  merging false
  skip "/spec/"
  skip "/scripts/"
  cover "lib/**/*.rb"
  cover "gems/*/lib/**/*.rb"
end

require "connectrpc/health"
require "connectrpc/testing"
require "connectrpc/rack"
require "connectrpc/falcon"
require "connectrpc/bindings/google_protobuf"
require "connectrpc/transports/net_http"
$LOAD_PATH.unshift(File.join(ROOT, "spec/fixtures/generation/echo"))
require "echo_connect"
require_relative "../../connectrpc-async/spec/support/helpers"
require_relative "../../connectrpc-async/spec/support/hosting_helpers"
require_relative "../../connectrpc-falcon/spec/support/helpers"

module HealthHelpers
  HEALTH = Grpc::Health::V1::Health
  REQUEST = Grpc::Health::V1::HealthCheckRequest
  BINDING = ConnectRPC::Bindings::GoogleProtobuf.new

  # Records each sent status and optionally parks the sender on a gate after
  # recording. A gate value that is an Exception is raised from the send.
  class RecordingWriter
    attr_reader :sent

    def initialize(gate: nil)
      @sent = Thread::Queue.new
      @gate = gate
    end

    def send_message(message)
      @sent << message.status
      released = @gate&.pop
      raise released if released.is_a?(Exception)

      nil
    end

    def next_status(timeout: 5)
      status = @sent.pop(timeout: timeout)
      raise "Timed out waiting for a sent status" if status.nil?

      status
    end
  end

  # Runs a Watch handler on its own thread; #outcome joins it with a bound.
  class WatchCall
    attr_reader :thread

    def initialize(registry, service, writer, context)
      @thread = Thread.new do
        Thread.current.report_on_exception = false
        registry.handlers.fetch(HEALTH::WATCH.path).call(REQUEST.new(service: service), writer, context)
        :ok
      rescue StandardError => e
        e
      end
    end

    def outcome(timeout: 5)
      raise "Watch did not finish" unless @thread.join(timeout)

      @thread.value
    end
  end

  def watch_call(registry, service, writer, context = ConnectRPC::Context.new)
    WatchCall.new(registry, service, writer, context)
  end

  def check(registry, service)
    registry.handlers.fetch(HEALTH::CHECK.path).call(REQUEST.new(service: service), ConnectRPC::Context.new).status
  end

  # Bounded polling for state with no direct signal, reporting the last value.
  def eventually(message, within: 5)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + within
    until (last = yield)
      raise "Timed out waiting for #{message}; last observed: #{last.inspect}" if
        Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

      Thread.pass
    end
    last
  end

  def health_server(registry, protocol: :auto, **)
    ConnectRPC::Server.new(services: [AsyncHelpers::ECHO::SERVICE, ConnectRPC::Health::SERVICE], binding: BINDING,
                           handlers: AsyncHelpers::ECHO.handlers(echo: ->(message, _) { message },
                                                                 fail: ->(message, _) { message })
                                                       .merge(registry.handlers),
                           protocol: protocol, status_codec: BINDING, **)
  end

  def health_client(transport, origin, protocol: :connect, **)
    core = ConnectRPC::Client.new(base_url: origin, binding: BINDING, transport: transport, protocol: protocol, **)
    HEALTH::Client.new(client: core)
  end

  def canceled = raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("canceled") }
end

RSpec.configure do |config|
  config.include AsyncHelpers
  config.include AsyncHostingHelpers
  config.include FalconHelpers
  config.include HealthHelpers
  config.order = :random
  config.disable_monkey_patching!
end
