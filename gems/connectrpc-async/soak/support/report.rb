# frozen_string_literal: true

require "fileutils"
require "json"
require "open3"
require "time"

module Soak
  # Appends one JSON line per event to `campaign.jsonl`, opening the file for
  # each write so that a hang or kill keeps every earlier line. The supervisor
  # (scripts/soak.rb) renders the Markdown summary from these lines.
  class Report
    ROOT = File.expand_path("../../../..", __dir__)

    attr_reader :path

    def self.default_directory(package)
      File.join(ROOT, "vendor", package, "soak", RUBY_VERSION)
    end

    def initialize(directory)
      FileUtils.mkdir_p(directory)
      @path = File.join(directory, "campaign.jsonl")
      @mutex = Mutex.new
    end

    def write(type, **fields)
      line = JSON.generate({ type: type, at: Time.now.utc.iso8601(3), **fields })
      @mutex.synchronize { append(line) }
    end

    def environment(package:, seed:, rounds:)
      write(:environment, package: package, revision: revision, ruby: RUBY_DESCRIPTION,
                          openssl: OpenSSL::OPENSSL_LIBRARY_VERSION, openssl_gem: OpenSSL::VERSION,
                          platform: RUBY_PLATFORM, gems: Gem.loaded_specs.transform_values { |spec| spec.version.to_s },
                          seed: seed, rounds: rounds, only: ENV.fetch("CONNECTRPC_SOAK_ONLY", nil),
                          deadline: ENV.fetch("CONNECTRPC_SOAK_DEADLINE", nil),
                          cleanup: ENV.fetch("CONNECTRPC_SOAK_CLEANUP", nil), pid: Process.pid)
    end

    # Thread backtraces, every registered reactor's task tree and descriptor
    # targets. Runs on a thread started by the deadline trap, so it reads other
    # reactors best-effort without their cooperation.
    def self.diagnostics
      threads = Thread.list.map do |thread|
        { name: thread.name || thread.inspect, status: thread.status.to_s, backtrace: thread.backtrace&.first(40) }
      end
      reactors = Diagnostics.reactors.map { |name, scheduler| { name: name, tree: Sampler.hierarchy(scheduler) } }
      { threads: threads, reactors: reactors, fds: Sampler.fd_links.transform_keys(&:to_s) }
    end

    private

    def append(line)
      File.open(@path, "a") do |file|
        file.write("#{line}\n")
        file.flush
      end
    end

    def revision
      output, status = Open3.capture2("git", "rev-parse", "HEAD", chdir: ROOT, err: File::NULL)
      status.success? ? output.strip : "unknown"
    rescue SystemCallError
      "unknown"
    end
  end

  # Reactors the harness owns, for task trees in failure and deadline reports.
  module Diagnostics
    @reactors = {}
    @lock = Mutex.new

    class << self
      def register(name, scheduler) = @lock.synchronize { @reactors[name] = scheduler }
      def unregister(name) = @lock.synchronize { @reactors.delete(name) }

      # Read without the lock: a trap thread must not wait behind a holder.
      def reactors = @reactors.dup
    end
  end
end
