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
      @server = Server.new(
        app,
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

    # Keep each request's input cleanup bound to its own WEBrick response.
    class Server < WEBrick::HTTPServer
      def initialize(app, config)
        super(config)
        @app = app
      end

      def service(request, response)
        Rackup::Handler::WEBrick.new(self, draining_app(response)).service(request, response)
      end

      private

      def draining_app(response)
        lambda do |env|
          # Fixture apps return buffered response bodies and finish reading during
          # call. Capture Rackup's input before an app replaces it with telemetry.
          input = env.fetch("rack.input")
          result = begin
            @app.call(env)
          rescue StandardError
            drain_input(input, response)
            raise
          end
          drain_input(input, response)
          result
        end
      end

      def drain_input(input, response)
        # Resume Rackup's original Fiber to EOF. WEBrick's later body fixup cannot
        # safely restart chunk parsing when that Fiber suspended inside a chunk.
        buffer = +"".b
        nil while input.read(16_384, buffer)
      rescue StandardError
        # Preserve the app response and body close, but prevent WEBrick's fixup
        # from restarting failed framing and admitting a following request.
        response.keep_alive = false
      end
    end
    private_constant :Server
  end
end
