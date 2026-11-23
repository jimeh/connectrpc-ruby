# frozen_string_literal: true

require "simplecov"
SimpleCov.root(File.expand_path("../../..", __dir__))
SimpleCov.coverage_dir("coverage/async/#{RUBY_VERSION}")
SimpleCov.start do
  enable_coverage :branch
  merging false
  skip "/spec/"
  skip "/scripts/"
  cover "gems/connectrpc-async/lib/**/*.rb"
end

require "connectrpc/async"
require "connectrpc/bindings/google_protobuf"
require "tmpdir"
$LOAD_PATH.unshift(File.expand_path("../../../spec/fixtures/generation/echo", __dir__))
require "echo_connect"
require_relative "support/helpers"
require_relative "support/hosting_helpers"
require_relative "support/tls_observed_host"
require_relative "support/frame_gate"
require_relative "support/rejected_media_body"
require_relative "support/connection_peer"
require_relative "support/threaded_helpers"

# Ruby warns once per process on first IO::Buffer use, which io-stream's
# buffered reads trigger. Warn here, so the warning never lands in whichever
# example's captured stderr runs first (#132).
IO::Buffer.new(1).free

# Transports read the proxy environment at construction. Every example starts
# with it cleared, so a developer's proxy settings never reroute direct dials;
# proxy specs set what they need while a transport takes its snapshot.
PROXY_ENV = %w[HTTP_PROXY http_proxy HTTPS_PROXY https_proxy NO_PROXY no_proxy REQUEST_METHOD].freeze

RSpec.configure do |config|
  config.include AsyncHelpers
  config.include AsyncHostingHelpers
  config.include ConnectionPeer
  config.include ThreadedHelpers
  config.order = :random
  config.disable_monkey_patching!
  config.around do |example|
    saved = PROXY_ENV.to_h { |key| [key, ENV.fetch(key, nil)] }
    PROXY_ENV.each { |key| ENV.delete(key) }
    example.run
  ensure
    saved.each { |key, value| value.nil? ? ENV.delete(key) : ENV[key] = value }
  end
end
