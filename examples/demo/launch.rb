# frozen_string_literal: true

require_relative "certs"

module DemoExample
  # Demo plumbing, not ConnectRPC usage: the server lifecycle shared by every
  # example server. It reads PORT and the --mtls flag, issues the demo
  # certificates for --mtls, prints the ready line, writes the state file clients
  # use to find the server, and turns Ctrl-C or TERM into a shutdown.
  class Launch
    def self.serve(name, &) = new(name, ARGV).serve(&)

    attr_reader :port

    def initialize(name, argv)
      @name = name
      @port = DemoExample.port(ENV.fetch("PORT", DEFAULT_PORT.to_s), 0..65_535) ||
              abort("PORT=#{ENV.fetch('PORT')} is not a port number from 0 to 65535 (PORT=0 picks a free one).")
      @mtls = mtls_flag(argv)
    end

    def mtls? = @mtls
    def scheme = mtls? ? "https" : "http"

    def serve
      $stdout.sync = true
      prepare_certificates if mtls?
      # Signal handlers may only do minimal work, so they write to a pipe that
      # wait_for_shutdown reads. Inside an Async reactor that read yields.
      @signals, writer = IO.pipe
      %w[INT TERM].each { |signal| trap(signal) { writer.write_nonblock("#{signal}\n", exception: false) } }
      yield self
      puts "#{@name} server stopped"
    rescue Errno::EADDRINUSE, Errno::EACCES => e
      abort bind_failure(e)
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

    private

    def bind_failure(error)
      reason = if error.is_a?(Errno::EACCES)
                 "needs privileges this process lacks. Set PORT to 1024 or above"
               else
                 "is already in use. Set PORT to another port"
               end
      "Port #{port} #{reason} (PORT=0 picks a free one)."
    end

    def prepare_certificates
      verb = Certs.prepare! == :issued ? "Issued" : "Using"
      puts "#{verb} demo certificates in #{Certs.display_dir}"
    end

    def mtls_flag(argv)
      return false if argv.empty?

      abort "Usage: mise run example:server -- #{@name} [--mtls]" unless argv == ["--mtls"]
      return true if MTLS_SERVERS.include?(@name)

      warn "Refused: #{@name} does not serve mutual TLS in these examples; " \
           "only #{MTLS_SERVERS.join(' and ')} accept --mtls.\nSee #{README} for the supported pairings."
      exit 2
    end
  end
end
