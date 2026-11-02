# frozen_string_literal: true

require "fileutils"
require "json"
require "open3"
require "rbconfig"
require "time"
require_relative "../conformance/ledger"

root = File.expand_path("..", __dir__)
unless ARGV.size <= 2 && (ARGV.empty? || %w[client server].include?(ARGV.first))
  raise "Usage: conformance.rb [client|server] [exact test name]"
end

version, status = Open3.capture2("connectconformance", "--version")
unless status.success? && version.strip == "connectconformance v1.0.6"
  raise "Expected official runner v1.0.6: #{version}"
end

directions = ARGV.first ? [ARGV.first] : %w[client server]
results = []
directions.each do |direction|
  Conformance::Ledger.new(direction: direction, selected: ARGV[1])
  artifacts = File.join(root, "vendor", "conformance", "results",
                        "#{Time.now.utc.strftime('%Y%m%dT%H%M%S')}-#{Process.pid}-#{RUBY_VERSION}-#{direction}")
  FileUtils.mkdir_p(artifacts)
  telemetry = File.join(artifacts, "telemetry.jsonl")
  command = ["connectconformance", "--mode", direction, "--conf", File.join(root, "conformance", "profile.yaml"),
             "--vv"]
  command.push("--known-failing", "@#{Conformance::Ledger::MANIFEST}") if direction == "client"
  command.push("--run", ARGV[1]) if ARGV[1]
  command.push("--", RbConfig.ruby, "-I#{File.join(root, 'lib')}",
               File.join(root, "conformance", "#{direction}.rb"))
  raw, process = Open3.capture2e({ "CONFORMANCE_TELEMETRY" => telemetry }, *command)
  File.write(File.join(artifacts, "runner.log"), raw)
  puts raw
  puts "Conformance #{direction} artifacts: #{artifacts}"
  ledger = Conformance::Ledger.new(direction: direction, selected: ARGV[1])
  begin
    events = File.readlines(telemetry).map { |line| JSON.parse(line) }
    summary = ledger.parse(raw, events, process.exitstatus)
    File.write(File.join(artifacts, "summary.json"), "#{JSON.pretty_generate(summary)}\n")
    puts "#{direction}: #{summary.fetch(:executed)} executed, #{summary.fetch(:passed)} passed, " \
         "#{summary.fetch(:failed)} failed, #{summary.fetch(:unsupported_instructions)} " \
         "unsupported caller instructions " \
         "(still executed), #{summary.fetch(:expected_failed)} failed as expected"
    results << process.success?
  rescue StandardError => e
    File.write(File.join(artifacts, "summary.json"),
               "#{JSON.pretty_generate({ accounting_valid: false, error: e.message })}\n")
    warn e.message
    results << false
  end
end
exit(results.all? ? 0 : 1)
