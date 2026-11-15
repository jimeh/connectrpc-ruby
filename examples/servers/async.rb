# frozen_string_literal: true

# Async server: the standalone ConnectRPC::Async::Host. It serves Connect and
# native gRPC, all four RPC shapes, over strict HTTP/2: h2c prior knowledge by
# default, or TLS that requires a client certificate with --mtls.
require "async"
require "connectrpc/async"
require_relative "../demo/launch"
require_relative "../demo/handlers"
require_relative "../demo/mtls"

DemoExample::Launch.serve("async") do |launch|
  host = nil
  # Inside a handler, host.peer_certificate returns the client certificate the
  # host verified for that call. It is nil without --mtls.
  server = DemoExample.connect_server(served_by: "async", protocol: :auto,
                                      client_certificate: -> { host.peer_certificate })
  # With --mtls the host serves TLS and requires a client certificate signed by
  # the demo CA. The provider rereads the certificate files every 5 seconds (the
  # default is 60), so renewed certificates take effect without a restart (see
  # demo/mtls.rb). Without TLS options the host serves h2c.
  tls = {}
  if launch.mtls?
    provider = DemoExample::MTLS.host_tls_provider { |version| puts "Loaded TLS certificates (version #{version})" }
    tls = { tls_provider: provider, refresh_interval: 5 }
  end
  # The application owns the reactor; the host runs inside it.
  Sync do |task|
    host = ConnectRPC::Async::Host.new(server: server, port: launch.port, **tls).start
    launch.ready("#{launch.scheme}://127.0.0.1:#{host.port}")
    shutdown = task.async do
      launch.wait_for_shutdown
      host.close(grace: 5)
    end
    host.wait # Returns once close stops the listeners; raises if a listener fails.
  ensure
    host&.close(grace: 5) # Joins an in-progress close and its drain outcome.
    shutdown&.stop
  end
end
