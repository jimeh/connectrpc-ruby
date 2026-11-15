# frozen_string_literal: true

# Rack server: ConnectRPC::Rack middleware hosted by WEBrick. Portable Rack
# hosting serves Connect unary calls over HTTP/1.1.
require_relative "../demo/launch"
require_relative "../demo/handlers"
# EchoExample::Localhost (examples/localhost.rb) is this repository's small
# WEBrick wrapper. It loads connectrpc/rack, binds, waits until WEBrick is
# ready and drains each request body. Any Rack server can host the same app.
require_relative "../localhost"

DemoExample::Launch.serve("rack") do |launch|
  server = DemoExample.connect_server(served_by: "rack")
  # The Rack app. In a config.ru this is `run ConnectRPC::Rack.new(server: server)`.
  host = EchoExample::Localhost.new(ConnectRPC::Rack.new(server: server), port: launch.port)
  launch.ready(host.url)
  launch.wait_for_shutdown
ensure
  host&.close
end
