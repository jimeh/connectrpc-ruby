# frozen_string_literal: true

require "fileutils"
require "json"
require "open3"
require "rbconfig"
require "time"
require_relative "../conformance/ledger"

root = File.expand_path("..", __dir__)
profile = ARGV.first&.start_with?("--profile=") ? ARGV.shift.delete_prefix("--profile=") : nil
profiles = profile ? [profile] : %w[identity gzip]
raise "Unknown profile" unless profiles.all? { |entry| Conformance::Ledger::PROFILES.key?(entry) }
unless ARGV.size <= 2 && (ARGV.empty? || %w[client server].include?(ARGV.first))
  raise "Usage: conformance.rb [client|server] [exact test name]"
end

version, status = Open3.capture2("connectconformance", "--version")
unless status.success? && version.strip == "connectconformance v1.0.6"
  raise "Expected official runner v1.0.6: #{version}"
end

directions = ARGV.first ? [ARGV.first] : %w[client server]
if ARGV[1] && profile.nil?
  profiles.select! { |entry| Conformance::Ledger::PROFILES.fetch(entry).fetch(ARGV.first).include?(ARGV[1]) }
  raise "Selector must be an exact configured test name" if profiles.empty?
end
results = []
profiles.product(directions).each do |profile_name, direction|
  Conformance::Ledger.new(direction: direction, selected: ARGV[1], profile: profile_name)
  stamp = "#{Time.now.utc.strftime('%Y%m%dT%H%M%S')}-#{Process.pid}-#{RUBY_VERSION}-#{profile_name}-#{direction}"
  artifacts = File.join(root, "vendor", "conformance", "results",
                        stamp)
  FileUtils.mkdir_p(artifacts)
  telemetry = File.join(artifacts, "telemetry.jsonl")
  profile_file = profile_name == "identity" ? "profile.yaml" : "profile-gzip.yaml"
  command = ["connectconformance", "--mode", direction, "--conf", File.join(root, "conformance", profile_file),
             "--vv"]
  command.push("--run", ARGV[1]) if ARGV[1]
  command.push("--", RbConfig.ruby, "-I#{File.join(root, 'lib')}",
               File.join(root, "conformance", "#{direction}.rb"))
  raw, process = Open3.capture2e({ "CONFORMANCE_TELEMETRY" => telemetry }, *command)
  File.write(File.join(artifacts, "runner.log"), raw)
  puts raw
  puts "Conformance #{profile_name} #{direction} artifacts: #{artifacts}"
  ledger = Conformance::Ledger.new(direction: direction, selected: ARGV[1], profile: profile_name)
  begin
    events = File.readlines(telemetry).map { |line| JSON.parse(line) }
    summary = ledger.parse(raw, events, process.exitstatus)
    File.write(File.join(artifacts, "summary.json"), "#{JSON.pretty_generate(summary)}\n")
    puts "#{direction}: #{summary.fetch(:executed)} executed, #{summary.fetch(:passed)} passed, " \
         "#{summary.fetch(:failed)} failed, #{summary.fetch(:expected_failed)} expected failures"
    results << process.success?
  rescue StandardError => e
    File.write(File.join(artifacts, "summary.json"),
               "#{JSON.pretty_generate({ accounting_valid: false, error: e.message })}\n")
    warn e.message
    results << false
  end
end
exit(results.all? ? 0 : 1)
