# frozen_string_literal: true

# NetHTTP client: core's default, dependency-free transport. It makes Connect
# unary calls over HTTP/1.1, with POST or opt-in GET. It has no client
# certificate option, so it cannot call a server started with --mtls.
require_relative "../demo/cli"
require_relative "../demo/calls"

options = DemoExample::CLI.parse("nethttp", ARGV)
client = ConnectRPC::Client.new(base_url: options.url, binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                                codec: options.codec, use_get: options.get, encoding: options.encoding)
exit DemoExample::Calls.run(Demo::V1::DemoService::Client.new(client: client), options)
