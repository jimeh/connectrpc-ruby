# frozen_string_literal: true

require "simplecov"
ROOT = File.expand_path("../../..", __dir__)
SimpleCov.root(ROOT)
SimpleCov.coverage_dir("coverage/yabeda/#{RUBY_VERSION}")
SimpleCov.start do
  enable_coverage :branch
  merging false
  skip "/spec/"
  skip "/scripts/"
  cover "lib/**/*.rb"
  cover "gems/*/lib/**/*.rb"
end

require "connectrpc/yabeda"
require "connectrpc/testing"
require "connectrpc/bindings/google_protobuf"
require "yabeda/testing"
$LOAD_PATH.unshift(File.join(ROOT, "spec/fixtures/generation/echo"))
require "echo_connect"

module YabedaHelpers
  def adapter = Yabeda::TestAdapter.instance

  def echo(instrumentation, message: "hello", fail: false)
    binding = ConnectRPC::Bindings::GoogleProtobuf.new
    service = Example::V1::EchoService
    handlers = service.handlers(echo: ->(request, _) { request },
                                fail: ->(*) { raise ConnectRPC::Error.new(:not_found, "missing") })
    server = ConnectRPC::Server.new(services: [service::SERVICE], binding: binding, handlers: handlers,
                                    instrumentation: instrumentation)
    core = ConnectRPC::Client.new(base_url: "https://example.invalid", binding: binding,
                                  transport: ConnectRPC::Testing::Transport.new(server: server),
                                  instrumentation: instrumentation)
    typed = service::Client.new(client: core)
    request = Example::V1::EchoMessage.new(name: message)
    fail ? typed.fail(request) : typed.echo(request)
  end
end

RSpec.configure do |config|
  config.include YabedaHelpers
  config.order = :random
  config.disable_monkey_patching!
  # Yabeda is process-global; every example starts unconfigured with the test adapter.
  config.before do
    Yabeda.reset!
    Yabeda.register_adapter(:test, Yabeda::TestAdapter.instance)
    Yabeda::TestAdapter.instance.reset!
  end
end
