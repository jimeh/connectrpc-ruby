# frozen_string_literal: true

require "fileutils"
require "json"
require "rbconfig"
require "time"

# Supervises each named package's long-running lifecycle campaign (`async`,
# `falcon` or both, in order) as a child process in its own process group. At
# the run deadline it sends TERM, whose trap in the child records thread
# backtraces, Async task trees and descriptor targets; after the cleanup
# budget it kills the group. It then renders the Markdown summary from the
# campaign's JSON lines, so a hung or killed run still gets one. A failed
# package does not stop the next; the exit status is non-zero if any failed.
# Arguments after the package names go to RSpec for every package.
module Soak
  module Supervisor
    ROOT = File.expand_path("..", __dir__)
    PACKAGES = %w[async falcon].freeze
    Outcome = Struct.new(:status, :process, :elapsed) do
      def fields
        { status: status, exit_status: process.exitstatus, signal: process.termsig, elapsed: elapsed.round(3) }
      end
    end

    module_function

    def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    def setting(name, default) = Float(ENV.fetch("CONNECTRPC_SOAK_#{name}", default))

    def main(*arguments)
      packages = packages(arguments)
      settings = self.settings
      statuses = packages.to_h { |package| [package, run(package, settings, arguments.drop(packages.size))] }
      report(statuses) if packages.size > 1
      exit(statuses.values.all?("passed") ? 0 : 1)
    end

    # The leading package names, each at most once.
    def packages(arguments)
      packages = arguments.take_while { |argument| PACKAGES.include?(argument) }
      return packages unless packages.empty? || packages.uniq.size != packages.size

      abort "Usage: ruby scripts/soak.rb #{PACKAGES.join('|')}... [rspec arguments]"
    end

    def report(statuses)
      puts "Lifecycle campaigns: #{statuses.map { |package, status| "#{package} #{status}" }.join(', ')}"
    end

    def run(package, settings, arguments)
      directory = prepare(package)
      outcome = supervise(package, directory, settings, arguments)
      record(directory, type: "supervisor", package: package, **settings, **outcome.fields)
      puts "Lifecycle campaign #{package} #{outcome.status}; report #{Summary.new(directory).write}"
      outcome.status
    end

    # The run deadline and cleanup budget in seconds, and the seed every child
    # uses. Each package gets its own run deadline.
    def settings
      seed = Integer(ENV.fetch("CONNECTRPC_SOAK_SEED") { Random.new_seed % (2**31) })
      { deadline: setting("DEADLINE", 900), cleanup: setting("CLEANUP", 60), seed: seed }
    end

    # A fresh report directory for this package and Ruby.
    def prepare(package)
      directory = File.join(ROOT, "vendor", package, "soak", RUBY_VERSION)
      FileUtils.mkdir_p(directory)
      FileUtils.rm_f(%w[campaign.jsonl summary.md].map { |name| File.join(directory, name) })
      directory
    end

    def supervise(package, directory, settings, arguments)
      environment = { "CONNECTRPC_SOAK_REPORT" => directory, "CONNECTRPC_SOAK_SEED" => settings[:seed].to_s }
      command = [RbConfig.ruby, File.join(ROOT, "gems/connectrpc-#{package}/scripts/run.rb"), "soak", *arguments]
      started = monotonic
      pid = Process.spawn(environment, *command, pgroup: true, in: File::NULL)
      waiter = Process.detach(pid)
      status = wait(pid, waiter, settings)
      Outcome.new(status, waiter.value, monotonic - started)
    ensure
      signal("KILL", -pid) if pid
    end

    # "passed", "failed", "deadline" (exited within the cleanup budget) or
    # "killed" (did not exit within it).
    def wait(pid, waiter, settings)
      return waiter.value.success? ? "passed" : "failed" if waiter.join(settings[:deadline])

      warn "Lifecycle campaign passed its #{settings[:deadline]} s deadline; requesting diagnostics"
      signal("TERM", pid)
      return "deadline" if waiter.join(settings[:cleanup])

      warn "Lifecycle campaign outlived its #{settings[:cleanup]} s cleanup budget; killing it"
      signal("KILL", -pid)
      waiter.join
      "killed"
    end

    def signal(name, target)
      Process.kill(name, target)
    rescue Errno::ESRCH, Errno::EPERM
      nil
    end

    # Appends the supervisor's record on a line of its own, even when a
    # killed child left its last record unterminated.
    def record(directory, **fields)
      path = File.join(directory, "campaign.jsonl")
      File.open(path, "a+") do |file|
        file.puts if file.size.positive? && file.pread(1, file.size - 1) != "\n"
        file.puts(JSON.generate({ **fields, at: Time.now.utc.iso8601(3) }))
      end
    end
  end

  # Markdown summary of one campaign's JSON lines.
  class Summary
    def initialize(directory)
      @directory = directory
      path = File.join(directory, "campaign.jsonl")
      @lines = File.exist?(path) ? File.readlines(path).filter_map { |line| parse(line) } : []
    end

    def write
      path = File.join(@directory, "summary.md")
      File.write(path, [header, environment, scenarios, workloads, failures, deadline].compact.join("\n\n") << "\n")
      path
    end

    private

    def parse(line)
      JSON.parse(line)
    rescue JSON::ParserError
      nil
    end

    def of(type) = @lines.select { |line| line["type"] == type }
    def last(type) = of(type).last || {}

    def header
      run = last("supervisor")
      "# Lifecycle campaign: #{run['package']} on Ruby #{RUBY_VERSION}\n\n" \
        "Result: **#{run['status']}** (#{run['signal'] ? "signal #{run['signal']}" : "exit #{run['exit_status']}"}) " \
        "after #{run['elapsed']} s. " \
        "Seed #{run['seed']}, deadline #{run['deadline']} s, cleanup budget #{run['cleanup']} s. " \
        "Reproduce with `CONNECTRPC_SOAK_SEED=#{run['seed']} mise run #{run['package']}:soak`."
    end

    def environment
      env = last("environment")
      return "## Environment\n\nThe campaign wrote no environment record." if env.empty?

      gems = env.fetch("gems", {}).sort.map { |name, version| "#{name} #{version}" }.join(", ")
      "## Environment\n\n- Revision: #{env['revision']}\n- Ruby: #{env['ruby']}\n" \
        "- OpenSSL: #{env['openssl']} (gem #{env['openssl_gem']})\n- Platform: #{env['platform']}\n" \
        "- Rounds per scenario: #{env['rounds']}; scenario filter: #{env['only'].inspect}\n" \
        "- Resolved gems: #{gems}"
    end

    def scenarios
      "## Scenarios\n\n| Scenario | Status | Warmups | Rounds | Elapsed (s) | Outcomes | Peaks (peak/cap) | " \
        "RSS (KiB) | Heap live slots |\n| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n" \
        "#{of('scenario').map { |scenario| scenario_row(scenario) }.join("\n")}"
    end

    def scenario_row(scenario)
      peaks = scenario["peaks"].map { |name, value| "#{name} #{value['peak']}/#{value['cap']}" }.join("; ")
      outcomes = scenario["outcomes"].map { |name, count| "#{name} #{count}" }.join(", ")
      rounds = "#{scenario['rounds_completed']}/#{scenario['rounds']}"
      row(*scenario.values_at("scenario", "status", "warmups"), rounds, scenario["elapsed"], outcomes, peaks,
          *scenario.values_at("rss_kb", "heap_live_slots"))
    end

    def row(*cells) = "| #{cells.join(' | ')} |"

    def workloads
      rows = of("scenario").map do |scenario|
        info = scenario["info"].dup
        fixed = %w[workload concurrency payload_sizes].map { |key| info.delete(key) }
        "| #{scenario['scenario']} | #{fixed[0]} | #{fixed[1]} | #{fixed[2]&.join(', ')} | " \
          "#{info.map { |key, value| "#{key}: #{value}" }.join('; ')} |"
      end
      "## Workloads\n\n| Scenario | Workload | Concurrency | Payload sizes (bytes) | Settings |\n" \
        "| --- | --- | --- | --- | --- |\n#{rows.join("\n")}"
    end

    def failures
      failed = of("scenario").select { |scenario| scenario["failure"] }
      return if failed.empty?

      "## Failures\n\n#{failed.map { |scenario| failure(scenario) }.join("\n\n")}"
    end

    def failure(scenario)
      failure = scenario["failure"]
      "### #{scenario['scenario']}\n\n```text\n#{failure['error']}: #{failure['message']}\n" \
        "#{Array(failure['backtrace']).join("\n")}\n```\n\n#{details(failure['diagnostics'])}"
    end

    def deadline
      records = of("diagnostics")
      return if records.empty?

      sections = records.map { |record| "Running scenario: #{record['scenario'].inspect}\n\n#{details(record)}" }
      "## Deadline diagnostics\n\n#{sections.join("\n\n")}"
    end

    def details(diagnostics)
      return "" unless diagnostics

      trees = diagnostics["reactors"].map { |reactor| "#{reactor['name']}\n#{reactor['tree']}" }
      [block("Threads", thread_lines(diagnostics["threads"])), block("Async task trees", trees),
       block("Descriptors", diagnostics["fds"].map { |fd, link| "#{fd} #{link}" })].join
    end

    def thread_lines(threads)
      threads.map { |thread| "#{thread['name']} (#{thread['status']})\n  #{Array(thread['backtrace']).join("\n  ")}" }
    end

    def block(title, lines)
      "<details><summary>#{title}</summary>\n\n```text\n#{lines.join("\n")}\n```\n\n</details>\n"
    end
  end
end

Soak::Supervisor.main(*ARGV) if $PROGRAM_NAME == __FILE__
