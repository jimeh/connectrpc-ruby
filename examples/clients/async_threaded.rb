# frozen_string_literal: true

# Async threaded client: ConnectRPC::Async::ThreadedTransport over h2c prior
# knowledge, or over mutual TLS for an https:// server. It needs no application
# reactor and supports Connect unary calls only, including GET.
require "connectrpc/async"
require_relative "../demo/cli"
require_relative "../demo/calls"
require_relative "../demo/mtls"

options = DemoExample::CLI.parse("async-threaded", ARGV)
# An https:// URL means the server runs with --mtls: trust the demo CA and
# present the demo client certificate. Plaintext URLs take no TLS options.
tls = options.mtls? ? DemoExample::MTLS.client_tls : {}
# Build one transport at boot and share it. Its owner thread and connection start on the first call.
transport = ConnectRPC::Async::ThreadedTransport.new(origin: options.origin, **tls)
client = ConnectRPC::Client.new(base_url: options.url, binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                                transport: transport, codec: options.codec, use_get: options.get,
                                encoding: options.encoding)
begin
  # Any plain thread can call, for example a Puma or Sidekiq worker thread.
  succeeded = Thread.new { DemoExample::Calls.run(Demo::V1::DemoService::Client.new(client: client), options) }.value
ensure
  # Drain in-flight calls before exit, as a server's shutdown hook would.
  transport.close
end
exit succeeded
