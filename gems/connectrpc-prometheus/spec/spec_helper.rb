# frozen_string_literal: true

require "simplecov"
ROOT = File.expand_path("../../..", __dir__)
SimpleCov.root(ROOT)
SimpleCov.coverage_dir("coverage/prometheus/#{RUBY_VERSION}")
SimpleCov.start do
  enable_coverage :branch
  merging false
  skip "/spec/"
  skip "/scripts/"
  cover "lib/**/*.rb"
  cover "gems/*/lib/**/*.rb"
end

require "connectrpc/prometheus"
require "connectrpc/testing"
require "connectrpc/rack"
require "connectrpc/falcon"
require "connectrpc/bindings/google_protobuf"
require "prometheus/client/formats/text"
require "prometheus/client/data_stores/direct_file_store"
require "stringio"
require "tmpdir"
$LOAD_PATH.unshift(File.join(ROOT, "spec/fixtures/generation/echo"))
require "echo_connect"
require_relative "../../connectrpc-async/spec/support/helpers"
require_relative "../../connectrpc-async/spec/support/hosting_helpers"
require_relative "../../connectrpc-async/spec/support/connection_peer"
require_relative "../../connectrpc-falcon/spec/support/helpers"

module MetricsHelpers
  ECHO_LABELS = { grpc_type: "unary", grpc_service: "example.v1.EchoService", grpc_method: "Echo",
                  rpc_protocol: "connect" }.freeze

  def registry = @registry ||= Prometheus::Client::Registry.new
  def metrics = @metrics ||= ConnectRPC::Prometheus::Metrics.new(registry: registry)
  def bus = @bus ||= ConnectRPC::Instrumentation.new(subscribers: [metrics.subscriber])
  def exposition = Prometheus::Client::Formats::Text.marshal(registry)

  # Aggregated across processes; DirectFileStore#get reads only the calling process's file.
  def value(name, labels = {})
    registry.get(name).values.fetch(labels, 0.0)
  end

  # Counter sums across matching label sets; histograms report their observation count.
  def total(name, **labels)
    metric = registry.get(name)
    metric.values.sum do |set, sample|
      next 0 unless labels <= set

      sample.is_a?(Hash) ? sample.fetch("+Inf") : sample
    end
  end

  def in_flight(role = :server)
    started = total(:"grpc_#{role}_started_total")
    rejected = role == :server ? total(:grpc_server_rejected_total) : 0
    started - total(:"grpc_#{role}_handled_total") - rejected
  end
end

RSpec.configure do |config|
  config.include AsyncHelpers
  config.include AsyncHostingHelpers
  config.include ConnectionPeer
  config.include FalconHelpers
  config.include MetricsHelpers
  config.order = :random
  config.disable_monkey_patching!
end
