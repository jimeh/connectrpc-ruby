# frozen_string_literal: true

require_relative "support"

module DemoExample
  # Demo plumbing, not ConnectRPC usage: the server lifecycle shared by every
  # example server. It reads PORT, prints the ready line, writes the state file
  # clients use to find the server, and turns Ctrl-C or TERM into a shutdown.
  class Launch
    def self.serve(name, &) = new(name).serve(&)

    attr_reader :port

    def initialize(name)
      @name = name
      @port = Integer(ENV.fetch("PORT", DEFAULT_PORT))
    end

    def serve
      $stdout.sync = true
      # Signal handlers may only do minimal work, so they write to a pipe that
      # wait_for_shutdown reads. Inside an Async reactor that read yields.
      @signals, writer = IO.pipe
      %w[INT TERM].each { |signal| trap(signal) { writer.write_nonblock("#{signal}\n", exception: false) } }
      yield self
      puts "#{@name} server stopped"
    rescue Errno::EADDRINUSE
      abort "Port #{port} is already in use. Set PORT to another port (PORT=0 picks a free one)."
    ensure
      State.remove
    end

    def ready(url)
      State.write(server: @name, url: url)
      puts "#{@name} server listening on #{url} (Ctrl-C to stop)"
    end

    def wait_for_shutdown
      puts "Received SIG#{@signals.gets.chomp}, shutting down"
    end
  end
end
