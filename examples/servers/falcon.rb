# frozen_string_literal: true

# Falcon server: ConnectRPC::Falcon::Server on one endpoint, plaintext by default
# or TLS that requires a client certificate with --mtls. HTTP/1.1 clients get
# Connect unary and SS/CS streaming; HTTP/2 clients also get bidi and native gRPC.
#
# Production deployments normally use `falcon host` with the service DSL, which
# adds process workers and restarts; see gems/connectrpc-falcon/examples/falcon.rb.
# This file runs one in-process server so it can bind PORT=0 and print its URL.
require "async"
require "connectrpc/falcon"
require_relative "../demo/launch"
require_relative "../demo/handlers"
require_relative "../demo/mtls"

DemoExample::Launch.serve("falcon") do |launch|
  host = nil
  # Inside a handler, host.peer_certificate returns the client certificate the
  # server verified for that call. It is nil without --mtls.
  server = DemoExample.connect_server(served_by: "falcon", protocol: :auto,
                                      client_certificate: -> { host.peer_certificate })
  # With --mtls the endpoint is https:// and the server requires a client
  # certificate signed by the demo CA. The provider rereads the certificate files
  # every 5 seconds (the default is 60), so renewed certificates take effect
  # without a restart (see demo/mtls.rb). The URL scheme and TLS options must agree.
  tls = {}
  if launch.mtls?
    provider = DemoExample::MTLS.host_tls_provider { |version| puts "Read TLS certificates (version #{version})" }
    # The host calls on_tls_error when it rejects what the provider returned, and keeps its last good certificates.
    rejected = ->(event) { puts "Rejected TLS certificates (#{event[:category]})" }
    tls = { tls_provider: provider, refresh_interval: 5, on_tls_error: rejected }
  end
  Sync do
    # Bind first so the actual port is known. The caller owns this bound endpoint.
    endpoint = ConnectRPC::Falcon::Endpoint.parse("#{launch.scheme}://127.0.0.1:#{launch.port}").bound
    host = ConnectRPC::Falcon::Server.new(server: server, endpoint: endpoint, **tls)
    host.run
    launch.ready("#{launch.scheme}://127.0.0.1:#{endpoint.sockets.first.local_address.ip_port}")
    launch.wait_for_shutdown
  ensure
    host&.close(grace: 5)
    endpoint&.close
  end
end
