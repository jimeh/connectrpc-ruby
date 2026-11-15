# frozen_string_literal: true

# Rack server: ConnectRPC::Rack middleware hosted by WEBrick. Portable Rack
# hosting serves Connect unary calls over HTTP/1.1.
require_relative "../demo/launch"
require_relative "../demo/handlers"
require_relative "../localhost" # Loads connectrpc/rack and a WEBrick host.

DemoExample::Launch.serve("rack") do |launch|
  server = DemoExample.connect_server(served_by: "rack")
  # Any Rack server can host this app; `run ConnectRPC::Rack.new(server: server)` works in config.ru.
  host = EchoExample::Localhost.new(ConnectRPC::Rack.new(server: server), port: launch.port)
  launch.ready(host.url)
  launch.wait_for_shutdown
ensure
  host&.close
end
