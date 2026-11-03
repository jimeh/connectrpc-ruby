# frozen_string_literal: true

require "fileutils"
require "json"
require "open3"
require "rbconfig"
require_relative "../conformance/native_ledger"
require_relative "../conformance/grpc_ledger"

grpc = ENV["CONNECTRPC_CONFORMANCE_GRPC"] == "1"

root = File.expand_path("..", __dir__)
host = ARGV.shift || "async"
directions = ARGV.empty? ? %w[client server] : [ARGV.shift]
selector = ARGV.shift
raise "Usage: native_conformance.rb async|falcon [client|server [exact-name]]" unless
  %w[async falcon].include?(host) && directions.all? do |direction|
    %w[client server].include?(direction)
  end && ARGV.empty?
if host == "falcon" && directions.include?("client")
  raise "Native client profile must run once through its actual Async transport"
end

version, status = Open3.capture2("connectconformance", "--version")
raise "Wrong conformance runner" unless status.success? && version.strip == "connectconformance v1.0.6"

def native_command(root, profile, direction, selector, grpc)
  command = ["connectconformance", "--mode", direction, "--conf",
             File.join(root, "conformance/profile-#{grpc ? 'grpc' : 'native'}-#{profile}.yaml"), "--vv"]
  command.push("--run", selector) if selector
  command.push("--", RbConfig.ruby, "-I#{File.join(root, 'lib')}",
               File.join(root, "conformance/native_#{direction}.rb"))
  command
end

def native_telemetry(artifacts)
  FileUtils.mkdir_p(artifacts)
  File.join(artifacts, "telemetry.jsonl")
end

def native_environment(telemetry, host, artifacts)
  { "CONFORMANCE_TELEMETRY" => telemetry, "CONNECTRPC_NATIVE_HOST" => host,
    "CONFORMANCE_DIAGNOSTICS" => File.join(artifacts, "adapter-stderr.log") }
end

success = true
%w[identity gzip].product(directions).each do |profile, direction| # rubocop:disable Metrics/BlockLength
  inventory = (grpc ? Conformance::GRPCLedger : Conformance::NativeLedger)::INVENTORIES.fetch(profile).fetch(direction)
  next if selector && !inventory.include?(selector)

  ledger = (grpc ? Conformance::GRPCLedger : Conformance::NativeLedger).new(profile: profile, direction: direction,
                                                                            selected: selector)
  stamp = "#{Time.now.utc.strftime('%Y%m%dT%H%M%S')}-#{Process.pid}-#{RUBY_VERSION}-" \
          "#{host}-#{grpc ? 'grpc' : 'native'}-#{profile}-#{direction}"
  artifacts = File.join(root, "vendor/conformance/results", stamp)
  telemetry = native_telemetry(artifacts)
  command = native_command(root, profile, direction, selector, grpc)
  raw, process = Open3.capture2e(native_environment(telemetry, host, artifacts), *command)
  File.write(File.join(artifacts, "runner.log"), raw)
  puts raw
  begin
    events = File.readlines(telemetry).map { |line| JSON.parse(line) }
    summary = ledger.parse(raw, events, process.exitstatus)
    File.write(File.join(artifacts, "summary.json"), "#{JSON.pretty_generate(summary)}\n")
    puts "Native #{host}/#{profile}/#{direction}: #{summary.fetch(:executed)} executed; " \
         "#{summary.fetch(:passed)} passed; " \
         "#{summary.fetch(:failed)} failed; artifacts #{artifacts}"
    success &&= process.success?
  rescue StandardError => e
    File.write(File.join(artifacts, "summary.json"),
               "#{JSON.pretty_generate(accounting_valid: false, error: e.message)}\n")
    warn e.message
    success = false
  end
end
exit(success ? 0 : 1)
