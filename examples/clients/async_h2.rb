# frozen_string_literal: true

# Async HTTP/2 client: ConnectRPC::Async::Transport over h2c prior knowledge, or
# over mutual TLS for an https:// server. It supports Connect and native gRPC,
# including all four RPC shapes.
require "async"
require "connectrpc/async"
require_relative "../demo/cli"
require_relative "../demo/calls"
require_relative "../demo/mtls"

options = DemoExample::CLI.parse("async-h2", ARGV)
# The application owns the reactor; create, use and close the transport inside it.
succeeded = Sync do
  # An https:// URL means the server runs with --mtls: trust the demo CA and
  # present the demo client certificate. Plaintext URLs take no TLS options.
  tls = options.mtls? ? DemoExample::MTLS.client_tls : {}
  transport = ConnectRPC::Async::Transport.new(origin: options.origin, **tls)
  client = ConnectRPC::Client.new(base_url: options.url, binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                                  transport: transport, protocol: options.protocol, codec: options.codec,
                                  use_get: options.get, encoding: options.encoding)
  DemoExample::Calls.run(Demo::V1::DemoService::Client.new(client: client), options)
ensure
  transport&.close
end
exit succeeded
