# frozen_string_literal: true

require "connectrpc/rack"
require "rackup/handler/webrick"

module EchoExample
  # A test/example host, not a ConnectRPC production server. Each instance owns
  # its listener and workers; readiness and shutdown have bounded waits.
  class Localhost
    attr_reader :url

    def initialize(app)
      ready = Queue.new
      @server = Rackup::Handler::WEBrick::Server.new(
        app, BindAddress: "127.0.0.1", Port: 0, AccessLog: [], Logger: WEBrick::Log.new(File::NULL),
             StartCallback: -> { ready << true }
      )
      @url = "http://127.0.0.1:#{@server.listeners.first.addr[1]}"
      @thread = Thread.new { @server.start }
      raise "Localhost server failed to start" unless ready.pop(timeout: 5)
    rescue StandardError
      close
      raise
    end

    def close
      @server&.shutdown
      raise "Localhost server failed to stop" if @thread && !@thread.join(5)
    end
  end
end
