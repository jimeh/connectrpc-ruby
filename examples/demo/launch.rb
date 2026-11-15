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
      @port = Integer(ENV.fetch("PORT", DEFAULT_PORT))
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

    private

    def prepare_certificates
      if Certs.usable?
        puts "Using demo certificates in #{Certs.display_dir}"
      else
        Certs.issue_all
        puts "Issued demo certificates in #{Certs.display_dir}"
      end
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
