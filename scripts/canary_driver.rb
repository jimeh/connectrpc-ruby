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
  TIMED_OUT = "did not finish within its deadline; a hang"
  HELD_OPEN = "a leftover process kept the output open after the command exited; its process group was killed"
  # RSpec's report of an error raised in a hook outside any example. Each block
  # ends at its unindented backtrace; example failure backtraces are indented.
  HOOK_ERROR = /^An error occurred in (an? `[^`]+` hook)\.\n(.*?)(?=^# |^An error occurred |^Failures:|\z)/m

  Phase = Struct.new(:name, :result, :details)

  # Runs a command in its own process group, streaming and keeping its merged
  # output. On expiry the whole group gets TERM, then KILL after a grace period.
  # Output ends at EOF, at the deadline, or DRAIN_GRACE after the leader exits;
  # a descendant still holding it then is a leftover, and the group is killed.
  module Bounded
    KILL_GRACE = 5
    DRAIN_GRACE = 3
    # How often a read wait checks whether the leader has exited.
    POLL = 0.25

    Outcome = Struct.new(:output, :exit_status, :timed_out, :held_open)

    module_function

    def monotonic = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)

    def run(command, seconds:, out: $stdout)
      reader, writer = IO.pipe
      pid = ::Process.spawn(*command, pgroup: true, in: File::NULL, out: writer, err: writer)
      writer.close
      waiter = ::Process.detach(pid)
      deadline = monotonic + seconds
      output, ending = stream(reader, waiter, deadline, out)
      Outcome.new(output, *finish(pid, waiter, deadline, ending))
    ensure
      reader&.close
      # Also reaps anything the command left running in its group.
      signal_group("KILL", pid) if pid
    end

    # The output and why reading stopped: :eof, :deadline or :held_open.
    def stream(reader, waiter, deadline, out)
      output = +""
      drained_by = nil
      loop do
        drained_by ||= monotonic + DRAIN_GRACE unless waiter.alive?
        stop, ending = stop_at(deadline, drained_by)
        return [output, ending] unless (remaining = stop - monotonic).positive?
        return [output, :eof] if reached_eof?(reader, [remaining, POLL].min, out, output)
      end
    end

    # When reading must stop and why: the deadline, or DRAIN_GRACE after the leader exited.
    def stop_at(deadline, drained_by)
      drained_by && drained_by < deadline ? [drained_by, :held_open] : [deadline, :deadline]
    end

    # Copies what arrives within `seconds` to `out` and `output`; true at EOF.
    def reached_eof?(reader, seconds, out, output)
      return false unless reader.wait_readable(seconds)

      chunk = reader.read_nonblock(65_536, exception: false)
      return chunk.nil? unless chunk.is_a?(String)

      out.write(chunk)
      out.flush
      output << chunk
      false
    end

    # The exit status, whether the deadline expired first (for the leader or its
    # output), and whether a leftover process held the output after the leader exited.
    def finish(pid, waiter, deadline, ending)
      finished = waiter.join([deadline - monotonic, 0].max)
      unless finished
        signal_group("TERM", pid)
        signal_group("KILL", pid) unless waiter.join(KILL_GRACE)
      end
      [waiter.value.exitstatus, !finished || ending == :deadline, ending == :held_open]
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
    return Phase.new(name, :timeout, [TIMED_OUT]) if outcome.timed_out

    phase = if outcome.exit_status&.zero?
              Phase.new(name, :passed, [])
            else
              rspec_failure(name, outcome.output.gsub(ANSI, ""), outcome.exit_status)
            end
    leftover(phase, outcome)
  end

  # A load error stops every example; anything else is reported per example.
  def rspec_failure(name, output, exit_status)
    if output.match?(/An error occurred while loading|^0 examples, /)
      load_failure(name, output)
    else
      example_failure(name, output, exit_status)
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

  # Failed example ids, then any error RSpec raised outside examples, such as in an after hook.
  def example_failure(name, output, exit_status)
    failed = output.scan(%r{^rspec (\./\S+) # (.+)$}).map { |location, description| "#{location} #{description.strip}" }
    failed << WORKED_AROUND if failed.any? { |line| line.include?("worked around") }
    outside = hook_errors(output)
    return Phase.new(name, :example_failures, failed + outside) unless failed.empty?
    return Phase.new(name, :failed, outside) unless outside.empty?

    Phase.new(name, :failed, ["exit status #{exit_status.inspect} without a recognized rspec failure"])
  end

  def hook_errors(output)
    output.scan(HOOK_ERROR).map { |hook, block| "error in #{hook}: #{hook_error(block)}" }.uniq
  end

  # `Class: message`, or the first line of a failed expectation, which has no class header.
  def hook_error(block)
    error, message = block.match(/^([A-Z][\w:]*):\n +([^\n]+)/)&.captures
    return "#{error}: #{message}" if error

    block.sub(%r{\AFailure/Error:[^\n]*\n}, "").lines.map(&:strip).find { |line| !line.empty? } || "see the log"
  end

  # A phase cannot pass while a leftover process held its output after it exited.
  def leftover(phase, outcome)
    return phase unless outcome.held_open

    phase.result = :failed if phase.result == :passed
    phase.details << HELD_OPEN
    phase
  end

  # The report is not rspec: it passes, times out, or fails with its last line.
  def report_phase(outcome)
    return Phase.new("report", :timeout, [TIMED_OUT]) if outcome.timed_out

    phase = if outcome.exit_status&.zero?
              Phase.new("report", :passed, [])
            else
              Phase.new("report", :failed, outcome.output.lines.map(&:strip).reject(&:empty?).last(1))
            end
    leftover(phase, outcome)
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
