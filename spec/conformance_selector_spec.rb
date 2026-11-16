# frozen_string_literal: true

require "fileutils"
require "json"
require "open3"
require "rbconfig"
require "tmpdir"
require_relative "../conformance/native_ledger"

RSpec.describe "Conformance selectors" do
  let(:root) { File.expand_path("..", __dir__) }

  around do |example|
    Dir.mktmpdir("connectrpc-conformance-selector-") do |directory|
      @directory = directory
      example.run
    end
  end

  def log_path = File.join(@directory, "runner.jsonl")

  def invocations
    File.exist?(log_path) ? File.readlines(log_path).map { |line| JSON.parse(line) } : []
  end

  # Stands in for the external runner: it reports the pinned version and records
  # every other invocation, then fails without producing telemetry.
  def install_fake_runner
    runner = File.join(@directory, "connectconformance")
    File.write(runner, <<~RUBY)
      #!#{RbConfig.ruby}
      require "json"
      if ARGV == ["--version"]
        puts "connectconformance v1.0.6"
        exit 0
      end
      File.open(#{log_path.inspect}, "a") { |log| log.puts(JSON.generate(ARGV)) }
      exit 1
    RUBY
    File.chmod(0o755, runner)
  end

  def run_script(script, *)
    install_fake_runner
    environment = { "PATH" => "#{@directory}:#{ENV.fetch('PATH')}" }
    output = File.join(@directory, "stdout")
    error = File.join(@directory, "stderr")
    command = [RbConfig.ruby, File.join(root, "scripts", script), *]
    pid = Bundler.with_unbundled_env { Process.spawn(environment, *command, out: output, err: error) }
    _pid, status = Process.wait2(pid)
    # Remove only this process's artifacts; the scripts name them by PID.
    FileUtils.rm_rf(Dir.glob(File.join(root, "vendor/conformance/results", "*-#{pid}-*")))
    [File.read(output), File.read(error), status]
  end

  describe "native_conformance.rb" do
    it "rejects an unknown selector before invoking the runner" do
      _output, error, status = run_script("native_conformance.rb", "async", "server", "NoSuchCase")
      expect(invocations).to be_empty
      expect(status.success?).to be(false)
      expect(error).to include("Selector must be an exact configured test name: NoSuchCase")
    end

    it "runs only the profile whose inventory contains the exact selector" do
      inventories = Conformance::NativeLedger::INVENTORIES
      name = (inventories.fetch("gzip").fetch("server") - inventories.fetch("identity").fetch("server")).first
      run_script("native_conformance.rb", "async", "server", name)
      expect(invocations.size).to eq(1)
      profile = File.join(root, "conformance/profile-native-gzip.yaml")
      expect(invocations.first.each_cons(2).to_a).to include(["--mode", "server"], ["--run", name], ["--conf", profile])
    end
  end

  describe "web_conformance.rb" do
    it "rejects an unknown selector before invoking the runner" do
      _output, error, status = run_script("web_conformance.rb", "async", "NoSuchCase")
      expect(invocations).to be_empty
      expect(status.success?).to be(false)
      expect(error).to include("Selector must be an exact configured test name")
    end

    it "invokes the runner with an exact Web selector" do
      name = JSON.parse(File.read(File.join(root, "conformance/cases-web-async.json"))).fetch("server").first
      run_script("web_conformance.rb", "async", name)
      expect(invocations.size).to eq(1)
      expect(invocations.first.each_cons(2).to_a).to include(["--mode", "server"], ["--run", name])
    end
  end
end
