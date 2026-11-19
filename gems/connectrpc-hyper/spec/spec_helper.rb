# frozen_string_literal: true

require "simplecov"
ROOT = File.expand_path("../../..", __dir__)
SimpleCov.root(ROOT)
SimpleCov.coverage_dir("coverage/hyper/#{RUBY_VERSION}")
SimpleCov.start do
  enable_coverage :branch
  merging false
  skip "/spec/"
  skip "/scripts/"
  cover "lib/**/*.rb"
  cover "gems/*/lib/**/*.rb"
end

require "connectrpc/hyper"
require "connectrpc/bindings/google_protobuf"
require "json"
require "open3"
require "openssl"
require "rbconfig"
require "socket"
require "timeout"
require "tmpdir"
$LOAD_PATH.unshift(File.join(ROOT, "interop/grpc/ruby"))
require "peer_connect"

MSG = Connectrpc::Grpcinterop::V1::Message
PEER = Connectrpc::Grpcinterop::V1::Peer

require_relative "support/peer"
require_relative "support/helpers"
require_relative "support/tiny_http"

RSpec.configure do |config|
  config.include HyperHelpers
  config.disable_monkey_patching!
  config.order = :random
  Kernel.srand config.seed
  config.after(:suite) { Peer.instance&.close }
end
