# frozen_string_literal: true

# Async server: the standalone ConnectRPC::Async::Host. It serves Connect and
# native gRPC, all four RPC shapes, over strict HTTP/2 (h2c prior knowledge here).
require "async"
require "connectrpc/async"
require_relative "../demo/launch"
require_relative "../demo/handlers"

DemoExample::Launch.serve("async") do |launch|
  server = DemoExample.connect_server(served_by: "async", protocol: :auto)
  # The application owns the reactor; the host runs inside it.
  Sync do |task|
    host = ConnectRPC::Async::Host.new(server: server, port: launch.port).start
    launch.ready("http://127.0.0.1:#{host.port}")
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
