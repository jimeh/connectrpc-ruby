# frozen_string_literal: true

# Async HTTP/1.1 client: ConnectRPC::Async::HTTP1Transport. It makes Connect
# unary, server-streaming and client-streaming calls; HTTP/1.1 has no bidi or gRPC.
require "async"
require "connectrpc/async"
require_relative "../demo/cli"
require_relative "../demo/calls"

options = DemoExample::CLI.parse("async-http1", ARGV)
succeeded = Sync do
  transport = ConnectRPC::Async::HTTP1Transport.new(origin: options.origin)
  client = ConnectRPC::Client.new(base_url: options.url, binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                                  transport: transport, codec: options.codec, use_get: options.get,
                                  encoding: options.encoding)
  DemoExample::Calls.run(Demo::V1::DemoService::Client.new(client: client), options)
ensure
  transport&.close
end
exit succeeded
