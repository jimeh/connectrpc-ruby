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
        draining_app(app),
        BindAddress: "127.0.0.1", Port: 0, AccessLog: [], Logger: WEBrick::Log.new(File::NULL),
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

    private

    def draining_app(app)
      lambda do |env|
        # Fixture apps return buffered response bodies and finish reading during
        # call. Capture Rackup's input before an app replaces it with telemetry.
        input = env.fetch("rack.input")
        result = begin
          app.call(env)
        rescue StandardError
          drain_input(input)
          raise
        end
        drain_input(input)
        result
      end
    end

    def drain_input(input)
      # Resume Rackup's original Fiber to EOF. WEBrick's later body fixup cannot
      # safely restart chunk parsing when that Fiber suspended inside a chunk.
      buffer = +"".b
      nil while input.read(16_384, buffer)
    rescue WEBrick::HTTPStatus::Error
      # As with WEBrick's fixup, a bad unread remainder must not replace the
      # app's response or prevent Rackup from closing its body.
      nil
    end
  end
end
