# frozen_string_literal: true

require "io/wait"
require "rbconfig"
require_relative "dependency_canary"

# Drives one package's pins-lifted canary lane from its launcher, outside
# Bundler: the scripts/dependency_canary.rb report, then the upstream contract
# spec, then the rest of the suite. Each runs as a `bundle exec` child in its own
# process group under a wall-clock deadline enforced here, outside Async, so a
# hang is still classified and the next phase still runs.
module CanaryDriver
  CONTRACT_SPEC = "spec/upstream_contract_spec.rb"
  PHASES = {
    "contract" => ["rspec", CONTRACT_SPEC],
    "suite" => ["rspec", "--exclude-pattern", "**/#{File.basename(CONTRACT_SPEC)}"]
  }.freeze
  # Seconds; generous next to normal runs, so only a hang reaches them.
  DEADLINES = { "report" => 180, "contract" => 300, "suite" => 1_800 }.freeze
  ANSI = /\e\[[0-9;]*m/
  RESULTS = {
    passed: "passed", timeout: "timeout (hang)", contract_guard: "contract guard failure",
    boot_failure: "boot failure", example_failures: "example failures", failed: "failed"
  }.freeze
  WORKED_AROUND = "a failed \"worked around\" example can mean upstream fixed that behavior; delete its workaround"

  Phase = Struct.new(:name, :result, :details)

  # Runs a command in its own process group, streaming and keeping its merged
  # output. On expiry the whole group gets TERM, then KILL after a grace period.
  module Bounded
    KILL_GRACE = 5

    Outcome = Struct.new(:output, :exit_status, :timed_out)

    module_function

    def monotonic = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)

    def run(command, seconds:, out: $stdout)
      reader, writer = IO.pipe
      pid = ::Process.spawn(*command, pgroup: true, in: File::NULL, out: writer, err: writer)
      writer.close
      waiter = ::Process.detach(pid)
      deadline = monotonic + seconds
      output = stream(reader, deadline, out)
      Outcome.new(output, *finish(pid, waiter, deadline))
    ensure
      reader&.close
      # Also reaps anything the command left running in its group.
      signal_group("KILL", pid) if pid
    end

    def stream(reader, deadline, out)
      output = +""
      while (remaining = deadline - monotonic).positive? && reader.wait_readable(remaining)
        chunk = reader.read_nonblock(65_536, exception: false)
        break if chunk.nil?
        next if chunk == :wait_readable

        out.write(chunk)
        out.flush
        output << chunk
      end
      output
    end

    # The exit status and whether the deadline expired first.
    def finish(pid, waiter, deadline)
      finished = waiter.join([deadline - monotonic, 0].max)
      unless finished
        signal_group("TERM", pid)
        signal_group("KILL", pid) unless waiter.join(KILL_GRACE)
      end
      [waiter.value.exitstatus, !finished]
    end

    def signal_group(signal, pid)
      ::Process.kill(signal, -pid)
    rescue Errno::ESRCH, Errno::EPERM
      nil
    end
  end

  module_function

  # Classifies one rspec phase from its exit status, deadline and output.
  def classify(name, outcome)
    return Phase.new(name, :timeout, ["no exit within its deadline; a hang"]) if outcome.timed_out
    return Phase.new(name, :passed, []) if outcome.exit_status&.zero?

    output = outcome.output.gsub(ANSI, "")
    if output.match?(/occurred outside of examples|An error occurred while loading/)
      load_failure(name, output)
    else
      example_failure(name, output, outcome.exit_status)
    end
  end

  # A load-time UpstreamContractError names each broken contract; any other load error is a boot failure.
  def load_failure(name, output)
    guard = output[/UpstreamContractError:\n(.*?)^\s*Verified with/m, 1]
    return Phase.new(name, :contract_guard, guard.scan(/^\s*- (.+)$/).flatten.uniq) if guard

    # RSpec prints a load error as `Class:` with the message indented on the next line.
    errors = output.scan(/^([A-Z][\w:]*(?:Error|Exception)):\n\s+(.+)$/).map { |error| error.join(": ") }.uniq
    Phase.new(name, :boot_failure, errors.empty? ? ["error outside of examples; see the log"] : errors.first(5))
  end

  def example_failure(name, output, exit_status)
    failed = output.scan(%r{^rspec (\./\S+) # (.+)$}).map { |location, description| "#{location} #{description.strip}" }
    if failed.empty?
      return Phase.new(name, :failed, ["exit status #{exit_status.inspect} without a recognized rspec failure"])
    end

    failed << WORKED_AROUND if failed.any? { |line| line.include?("worked around") }
    Phase.new(name, :example_failures, failed)
  end

  # The report is not rspec: it passes, times out, or fails with its last line.
  def report_phase(outcome)
    return Phase.new("report", :timeout, ["no exit within its deadline"]) if outcome.timed_out
    return Phase.new("report", :passed, []) if outcome.exit_status&.zero?

    Phase.new("report", :failed, outcome.output.lines.map(&:strip).reject(&:empty?).last(1))
  end

  def summary_markdown(package, phases)
    rows = phases.map do |phase|
      "| #{phase.name} | #{RESULTS.fetch(phase.result)} | #{phase.details.join('<br>').gsub('|', '\|')} |"
    end
    ["### Pins-lifted canary result: #{package}", "", "| Phase | Result | Details |", "| --- | --- | --- |",
     *rows, ""].join("\n")
  end

  # Prints and appends the summary; true only when every phase passed.
  def summary(package, phases)
    puts "", "Pins-lifted canary result for #{package}:"
    phases.each do |phase|
      puts("  #{phase.name}: #{RESULTS.fetch(phase.result)}", phase.details.map { |detail| "    #{detail}" })
    end
    DependencyCanary.append_summary(summary_markdown(package, phases))
    phases.all? { |phase| phase.result == :passed }
  end

  def phase(name, bundle, *command)
    puts "", "== Pins-lifted canary #{name} phase (#{DEADLINES.fetch(name)}s deadline)"
    $stdout.flush
    Bounded.run([*bundle, *command], seconds: DEADLINES.fetch(name))
  end

  # Runs every phase from the launcher's environment and working directory.
  def run!(package)
    raise "Run with #{DependencyCanary::ENVIRONMENT}=1 through the package launcher" unless DependencyCanary.enabled?

    DependencyCanary.reject_floors!
    bundle = [RbConfig.ruby, "-S", "bundle", "_#{ENV.fetch('BUNDLE_VERSION')}_", "exec"]
    phases = [report_phase(phase("report", bundle, "ruby", File.join(__dir__, "dependency_canary.rb")))]
    PHASES.each { |name, command| phases << classify(name, phase(name, bundle, *command)) }
    summary(package, phases)
  end
end

if $PROGRAM_NAME == __FILE__
  begin
    exit(CanaryDriver.run!(File.basename(Dir.pwd)))
  rescue RuntimeError => e
    abort e.message
  end
end
