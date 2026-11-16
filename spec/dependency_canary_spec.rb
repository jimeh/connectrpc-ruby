# frozen_string_literal: true

require "stringio"
require "tmpdir"
require_relative "../scripts/canary_driver"

RSpec.describe DependencyCanary do
  def requirement(*parts) = Gem::Requirement.new(*parts)
  def version(text) = Gem::Version.new(text)

  def with_environment(values)
    previous = values.keys.to_h { |key| [key, ENV.fetch(key, nil)] }
    values.each { |key, value| ENV[key] = value }
    yield
  ensure
    previous.each { |key, value| ENV[key] = value }
  end

  def canary(&) = with_environment(described_class::ENVIRONMENT => "1", DependencyFloors::ENVIRONMENT => nil, &)

  it "lifts only a single patch pin, below 1 for 0.x and below the next major otherwise" do
    expect(described_class.lift(requirement("~> 0.105.0"))).to eq(requirement(">= 0.105.0", "< 1"))
    expect(described_class.lift(requirement("~> 2.46.0"))).to eq(requirement(">= 2.46.0", "< 3"))
    [requirement("~> 0.105"), requirement(">= 0.72.1", "< 1"), requirement(">= 3.3"), requirement("= 1.2.3"),
     requirement("~> 1.2.3", "< 1.2.9"), requirement("~> 1.2.3.4"), requirement("~> 1.2.0.pre")].each do |kept|
      expect(described_class.lift(kept)).to be_nil, kept.to_s
    end
  end

  it "places the ignored wrapper and canary-only bundle state beside the normal ones" do
    expect(described_class.wrapper_path("/repo/gems/connectrpc-async/Gemfile"))
      .to eq("/repo/gems/connectrpc-async/Gemfile.canary")
    expect { described_class.wrapper_path("/repo/gemfiles/rails_8.0.gemfile") }
      .to raise_error(/Cannot derive a canary wrapper name/)
    with_environment(described_class::ENVIRONMENT => nil) do
      expect(described_class.bundle_settings("/repo/vendor/async"))
        .to eq("BUNDLE_PATH" => "/repo/vendor/async/bundle", "BUNDLE_USER_HOME" => "/repo/vendor/async/bundler")
      expect(described_class.gemfile("/repo/Gemfile")).to eq("/repo/Gemfile")
    end
    canary do
      expect(described_class.bundle_settings("/repo/vendor/async")).to eq(
        "BUNDLE_PATH" => "/repo/vendor/async/canary/bundle", "BUNDLE_USER_HOME" => "/repo/vendor/async/canary/bundler"
      )
    end
  end

  it "rejects floors mode and artifact-building launcher commands in canary mode" do
    with_environment(described_class::ENVIRONMENT => "1", DependencyFloors::ENVIRONMENT => "1") do
      expect { described_class.gemfile("/repo/Gemfile") }.to raise_error(/cannot be combined/)
      expect { described_class.evaluate(Bundler::Dsl.new, "/repo/Gemfile") }.to raise_error(/cannot be combined/)
      expect { described_class.allow!("test") }.to raise_error(/cannot be combined/)
    end
    canary do
      %w[setup exec test canary].each { |command| expect { described_class.allow!(command) }.not_to raise_error }
      %w[build package interop check].each do |command|
        expect { described_class.allow!(command) }.to raise_error(/#{command} is not part of the pins-lifted canary/)
      end
    end
    with_environment(described_class::ENVIRONMENT => nil) do
      expect { described_class.allow!("build") }.not_to raise_error
    end
  end

  it "evaluates a Gemfile whose local gemspec patch pins load lifted and recorded, leaving others alone" do
    Dir.mktmpdir("connectrpc-canary-") do |directory|
      package = "connectrpc-canary-#{File.basename(directory)}"
      File.write(File.join(directory, "#{package}.gemspec"), <<~RUBY)
        Gem::Specification.new do |spec|
          spec.name = #{package.dump}
          spec.version = "0.1.0"
          spec.summary = "fixture"
          spec.authors = ["fixture"]
          spec.add_dependency "async-http", "~> 0.105.0"
          spec.add_dependency "async", ">= 2.46.0", "< 3"
          spec.add_dependency "connectrpc", "~> 0.1.0"
          spec.add_development_dependency "rake", "~> 13.4.2"
        end
      RUBY
      File.write(File.join(directory, "other.gemspec"), <<~RUBY)
        Gem::Specification.new do |spec|
          spec.name = "other"
          spec.version = "1.0.0"
          spec.summary = "fixture"
          spec.authors = ["fixture"]
          spec.add_dependency "rack", "~> 3.2.7"
        end
      RUBY
      gemfile = File.join(directory, "Gemfile")
      File.write(gemfile, <<~RUBY)
        source "https://rubygems.org"
        gemspec name: #{package.dump}
        gemspec name: "other"
      RUBY
      dsl = Bundler::Dsl.new
      canary { described_class.evaluate(dsl, gemfile) }
      specs = dsl.instance_variable_get(:@gemspecs).to_h { |spec| [spec.name, spec] }
      dependencies = specs.fetch(package).dependencies.to_h { |entry| [entry.name, entry.requirement] }

      expect(dependencies).to eq("async-http" => requirement(">= 0.105.0", "< 1"),
                                 "async" => requirement(">= 2.46.0", "< 3"),
                                 "connectrpc" => requirement("~> 0.1.0"), "rake" => requirement("~> 13.4.2"))
      expect(specs.fetch("other").dependencies.map(&:requirement)).to eq([requirement("~> 3.2.7")])
      lift = described_class.lifts.fetch([package, "async-http"])
      expect(lift.to_a).to eq([package, "async-http", requirement("~> 0.105.0"), requirement(">= 0.105.0", "< 1")])
      expect(described_class.lifts.keys.select { |key| key.first == package }).to eq([[package, "async-http"]])
    end
  end

  it "keeps every declaration when canary mode is off, even after the lift is installed" do
    with_environment(described_class::ENVIRONMENT => nil) do
      expect(described_class.lift_declaration("connectrpc-async", "async-http", ["~> 0.105.0"])).to be_nil
    end
  end

  describe "report" do
    let(:lift) do
      described_class::Lift.new("connectrpc-async", "async-http", requirement("~> 0.105.0"),
                                requirement(">= 0.105.0", "< 1"))
    end
    let(:locked) { { %w[connectrpc-async async-http] => requirement("< 1", ">= 0.105.0") } }
    let(:resolved) do
      { "async-http" => version("0.106.1"), "async" => version("2.46.0"), "rack" => version("3.2.7") }
    end

    it "says whether each lifted dependency resolved beyond its pin and lists the resolved socketry stack" do
      entries = described_class::Report.entries([lift], resolved, locked)
      expect(described_class::Report.lines(entries, resolved)).to eq(
        ["lifted connectrpc-async async-http ~> 0.105.0 -> >= 0.105.0, < 1: resolved 0.106.1, beyond the pin",
         "Newest eligible socketry stack after the 3-day cooldown:", "  async 2.46.0", "  async-http 0.106.1"]
      )
      within = described_class::Report.entries([lift], resolved.merge("async-http" => version("0.105.3")), locked)
      expect(within.first.last).to eq("within the pin")
      markdown = described_class::Report.markdown("connectrpc-async", entries, resolved)
      expect(markdown).to include("| connectrpc-async | async-http | `~> 0.105.0` | `>= 0.105.0, < 1` | 0.106.1 | " \
                                  "beyond the pin |", "async 2.46.0, async-http 0.106.1")
    end

    it "fails when nothing was lifted, a lifted gem is missing or the canary lock drifted" do
      expect { described_class::Report.entries([], resolved, locked) }.to raise_error(/retire or retarget/)
      expect { described_class::Report.entries([lift], resolved.except("async-http"), locked) }
        .to raise_error(/Lifted dependency async-http is missing/)
      expect { described_class::Report.entries([lift], resolved, {}) }
        .to raise_error(/Canary lock drift: connectrpc-async records async-http nothing, expected >= 0.105.0, < 1/)
      drifted = { %w[connectrpc-async async-http] => requirement("~> 0.105.0") }
      expect { described_class::Report.entries([lift], resolved, drifted) }.to raise_error(/Canary lock drift/)
    end

    it "appends Markdown to the step summary only when GitHub provides one" do
      Dir.mktmpdir("connectrpc-canary-summary-") do |directory|
        path = File.join(directory, "summary.md")
        with_environment("GITHUB_STEP_SUMMARY" => path) do
          described_class.append_summary("first")
          described_class.append_summary("second")
        end
        expect(File.read(path)).to eq("first\nsecond\n")
        with_environment("GITHUB_STEP_SUMMARY" => nil) { described_class.append_summary("ignored") }
        expect(File.read(path)).to eq("first\nsecond\n")
      end
    end
  end
end

RSpec.describe CanaryDriver do
  describe ".classify" do
    def outcome(output, exit_status: 1, timed_out: false)
      described_class::Bounded::Outcome.new(output, exit_status, timed_out)
    end

    let(:guard) do
      <<~TEXT
        An error occurred while loading ./spec/spec_helper.
        Failure/Error: require "connectrpc/async"

        ConnectRPC::Async::UpstreamContractError:
          connectrpc-async 0.1.2 is incompatible with the installed socketry gems:
            - async-http 0.106.0: Async::HTTP::Protocol::HTTP2::Output#start now takes (parent: ?, task: ?)
            - async 2.47.0: Task finalization did not call #consume
          Verified with async 2.46.0, async-http 0.105.0, protocol-http2 0.29.2. See https://example.invalid/68.
        # ./spec/spec_helper.rb:14:in '<top (required)>'
        No examples found.

        0 examples, 0 failures, 1 error occurred outside of examples
      TEXT
    end

    it "passes a successful phase and names a hang when the deadline expired" do
      expect(described_class.classify("suite", outcome("2 examples, 0 failures", exit_status: 0)).result)
        .to eq(:passed)
      timeout = described_class.classify("suite", outcome("partial", exit_status: nil, timed_out: true))
      expect(timeout.to_a).to eq(["suite", :timeout, ["no exit within its deadline; a hang"]])
    end

    it "names each broken contract when the guard fails require" do
      phase = described_class.classify("contract", outcome(guard))
      expect(phase.result).to eq(:contract_guard)
      expect(phase.details).to eq(["async-http 0.106.0: Async::HTTP::Protocol::HTTP2::Output#start now takes " \
                                   "(parent: ?, task: ?)", "async 2.47.0: Task finalization did not call #consume"])
    end

    it "reports another load error as a boot failure" do
      output = <<~TEXT
        \e[31mAn error occurred while loading ./spec/spec_helper.\e[0m
        Failure/Error: require "connectrpc/async"

        LoadError:
          cannot load such file -- async/http/protocol/http2/output
        # ./spec/spec_helper.rb:14:in '<top (required)>'

        0 examples, 0 failures, 1 error occurred outside of examples
      TEXT
      phase = described_class.classify("suite", outcome(output))
      expect(phase.to_a).to eq(["suite", :boot_failure,
                                ["LoadError: cannot load such file -- async/http/protocol/http2/output"]])
    end

    it "lists failed examples and notes that a failed worked-around example can mean an upstream fix" do
      output = <<~TEXT
        Failures:

          1) connectrpc-async upstream contract worked around protocol-http2 consume_local_window lets the window go negative
             Failure/Error: expect(window).to be_negative
               ConnectRPC::Async::UpstreamContractError mentioned in an example failure

        Failed examples:

        rspec ./spec/upstream_contract_spec.rb:463 # connectrpc-async upstream contract worked around protocol-http2 consume_local_window lets the window go negative
        rspec ./spec/host_spec.rb:12 # Host serves h2c
      TEXT
      phase = described_class.classify("contract", outcome(output))
      expect(phase.result).to eq(:example_failures)
      expect(phase.details).to eq(
        ["./spec/upstream_contract_spec.rb:463 connectrpc-async upstream contract worked around protocol-http2 " \
         "consume_local_window lets the window go negative", "./spec/host_spec.rb:12 Host serves h2c",
         "a failed \"worked around\" example can mean upstream fixed that behavior; delete its workaround"]
      )
      plain = described_class.classify("suite", outcome("Failed examples:\n\nrspec ./spec/host_spec.rb:12 # Host"))
      expect(plain.details).to eq(["./spec/host_spec.rb:12 Host"])
    end

    it "keeps an unrecognized failure distinct from the named classes" do
      phase = described_class.classify("suite", outcome("Segmentation fault", exit_status: 139))
      expect(phase.to_a).to eq(["suite", :failed, ["exit status 139 without a recognized rspec failure"]])
    end

    it "reports the report phase as passed, a hang, or failed with its last line" do
      expect(described_class.report_phase(outcome("lifted ...", exit_status: 0)).result).to eq(:passed)
      expect(described_class.report_phase(outcome("", exit_status: nil, timed_out: true)).result).to eq(:timeout)
      failed = described_class.report_phase(outcome("Bundler loaded\nNo patch pin was lifted; retire it\n\n"))
      expect(failed.to_a).to eq(["report", :failed, ["No patch pin was lifted; retire it"]])
    end

    it "summarizes every phase as a Markdown table and passes only when all passed" do
      phases = [described_class::Phase.new("report", :passed, []),
                described_class.classify("contract", outcome(guard)),
                described_class::Phase.new("suite", :timeout, ["a | b"])]
      markdown = described_class.summary_markdown("connectrpc-async", phases)
      expect(markdown).to include("| report | passed |  |", "| contract | contract guard failure | async-http 0.106.0:",
                                  "<br>async 2.47.0:", "| suite | timeout (hang) | a \\| b |")
      result = nil
      expect { result = described_class.summary("connectrpc-async", phases) }
        .to output(/contract: contract guard failure\n    async-http 0.106.0/).to_stdout
      expect(result).to be(false)
      expect { result = described_class.summary("connectrpc-async", phases.first(1)) }.to output.to_stdout
      expect(result).to be(true)
    end
  end

  describe "Bounded.run" do
    it "streams and keeps the output and exit status of a command that finishes in time" do
      out = StringIO.new
      script = "$stdout.sync = true; puts :first; warn :second; exit 3"
      outcome = described_class::Bounded.run([RbConfig.ruby, "-e", script], seconds: 30, out: out)
      expect(outcome.to_a).to eq(["first\nsecond\n", 3, false])
      expect(out.string).to eq(outcome.output)
    end

    def gone?(pid)
      Process.kill(0, pid)
      File.read("/proc/#{pid}/stat").split[2] == "Z" if File.exist?("/proc/#{pid}/stat")
    rescue Errno::ESRCH, Errno::ENOENT
      true
    end

    it "kills the whole process group of a command that outlives its deadline" do
      Dir.mktmpdir("connectrpc-canary-deadline-") do |directory|
        marker = File.join(directory, "child.pid")
        # A parent that hangs on a child which hangs too, after both report that they started.
        child_script = "File.write(#{marker.dump}, Process.pid.to_s); sleep"
        script = <<~RUBY
          $stdout.sync = true
          child = Process.spawn(#{RbConfig.ruby.dump}, "-e", #{child_script.dump})
          deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 20
          sleep 0.01 until File.exist?(#{marker.dump}) || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
          puts :ready
          Process.wait(child)
        RUBY
        started = described_class::Bounded.monotonic
        outcome = described_class::Bounded.run([RbConfig.ruby, "-e", script], seconds: 5, out: StringIO.new)
        expect(described_class::Bounded.monotonic - started).to be < 5 + described_class::Bounded::KILL_GRACE + 5
        expect(outcome.output).to eq("ready\n"), "the command did not start its child before the deadline"
        expect(outcome.timed_out).to be(true)
        expect(described_class.classify("suite", outcome).result).to eq(:timeout)
        child = Integer(File.read(marker))
        # The orphaned child is reaped asynchronously after the group KILL.
        deadline = described_class::Bounded.monotonic + 10
        sleep 0.05 until gone?(child) || described_class::Bounded.monotonic > deadline
        expect(gone?(child)).to be(true), "child #{child} survived the group kill"
      end
    end
  end
end
