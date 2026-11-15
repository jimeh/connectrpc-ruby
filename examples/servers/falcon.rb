# frozen_string_literal: true

# Falcon server: ConnectRPC::Falcon::Server on one plaintext endpoint. HTTP/1.1
# clients get Connect unary and SS/CS streaming; HTTP/2 clients also get bidi and
# native gRPC.
#
# Production deployments normally use `falcon host` with the service DSL, which
# adds process workers and restarts; see gems/connectrpc-falcon/examples/falcon.rb.
# This file runs one in-process server so it can bind PORT=0 and print its URL.
require "async"
require "connectrpc/falcon"
require_relative "../demo/launch"
require_relative "../demo/handlers"

DemoExample::Launch.serve("falcon") do |launch|
  server = DemoExample.connect_server(served_by: "falcon", protocol: :auto)
  Sync do
    # Bind first so the actual port is known. The caller owns this bound endpoint.
    endpoint = ConnectRPC::Falcon::Endpoint.parse("http://127.0.0.1:#{launch.port}").bound
    host = ConnectRPC::Falcon::Server.new(server: server, endpoint: endpoint)
    host.run
    launch.ready("http://127.0.0.1:#{endpoint.sockets.first.local_address.ip_port}")
    launch.wait_for_shutdown
  ensure
    host&.close(grace: 5)
    endpoint&.close
  end
end
