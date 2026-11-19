# frozen_string_literal: true

require "fileutils"
require "json"
require "open3"
require "rbconfig"
require_relative "../conformance/native_ledger"
require_relative "../conformance/grpc_ledger"

http1 = ENV["CONNECTRPC_CONFORMANCE_HTTP1"] == "1"
require_relative "../conformance/http1_ledger" if http1

http1_client = ENV["CONNECTRPC_CONFORMANCE_HTTP1_CLIENT"] == "1"
require_relative "../conformance/native_http1_ledger" if http1_client

grpc = ENV["CONNECTRPC_CONFORMANCE_GRPC"] == "1"

threaded = ENV["CONNECTRPC_CONFORMANCE_THREADED_CLIENT"] == "1"
require_relative "../conformance/native_threaded_ledger" if threaded

# PerCallTransport runs the threaded client inventories on one execution path.
per_call = ENV.fetch("CONNECTRPC_CONFORMANCE_PER_CALL_CLIENT", nil)
raise "Per-call path must be threaded or inline" unless [nil, "threaded", "inline"].include?(per_call)

require_relative "../conformance/native_per_call_ledger" if per_call

root = File.expand_path("..", __dir__)
host = ARGV.shift || "async"
directions = ARGV.empty? ? %w[client server] : [ARGV.shift]
selector = ARGV.shift
raise "Usage: native_conformance.rb async|falcon [client|server [exact-name]]" unless
  %w[async falcon].include?(host) && directions.all? do |direction|
    %w[client server].include?(direction)
  end && ARGV.empty?
if http1_client && (host != "async" || directions != ["client"] || grpc || http1)
  raise "Native HTTP1 client profiles require Async client-only Connect mode"
end
if (threaded || per_call) && (host != "async" || directions != ["client"] || http1 || http1_client)
  raise "Threaded and per-call client profiles require Async client-only Connect or gRPC mode"
end
raise "Select either the threaded or the per-call client" if threaded && per_call
if http1 && (host != "falcon" || directions != ["server"] || grpc)
  raise "HTTP1 profiles require Falcon server-only Connect mode"
end
if host == "falcon" && directions.include?("client")
  raise "Native client profile must run once through its actual Async transport"
end

ledger_type = if per_call
                Conformance::NativePerCallLedger
              elsif threaded
                Conformance::NativeThreadedLedger
              elsif http1_client
                Conformance::NativeHTTP1Ledger
              elsif http1
                Conformance::HTTP1Ledger
              else
                (grpc ? Conformance::GRPCLedger : Conformance::NativeLedger)
              end
profiles = %w[identity gzip]
profiles = profiles.map { |profile| "#{grpc ? 'grpc' : 'connect'}-#{profile}" } if threaded || per_call
runs = profiles.product(directions).select do |profile, direction|
  !selector || ledger_type::INVENTORIES.fetch(profile).fetch(direction).include?(selector)
end
raise "Selector must be an exact configured test name: #{selector}" if runs.empty?

version, status = Open3.capture2("connectconformance", "--version")
raise "Wrong conformance runner" unless status.success? && version.strip == "connectconformance v1.0.6"

protocol = if per_call
             "per-call"
           elsif threaded
             "threaded"
           elsif http1_client
             "native-http1-client"
           elsif http1
             "http1"
           else
             (grpc ? "grpc" : "native")
           end

def native_command(root, profile, direction, selector, protocol)
  # PerCallTransport carries exactly the threaded client's feature profile.
  configuration = protocol == "per-call" ? "threaded" : protocol
  command = ["connectconformance", "--mode", direction, "--conf",
             File.join(root, "conformance/profile-#{configuration}-#{profile}.yaml"), "--vv"]
  command.push("--run", selector) if selector
  adapter = { "native-http1-client" => "native_http1_client", "threaded" => "native_threaded_client",
              "per-call" => "native_per_call_client" }.fetch(protocol, "native_#{direction}")
  command.push("--", RbConfig.ruby, "-I#{File.join(root, 'lib')}",
               File.join(root, "conformance/#{adapter}.rb"))
  command
end

def native_telemetry(artifacts)
  FileUtils.mkdir_p(artifacts)
  File.join(artifacts, "telemetry.jsonl")
end

def native_environment(telemetry, host, artifacts, per_call)
  { "CONFORMANCE_TELEMETRY" => telemetry, "CONNECTRPC_NATIVE_HOST" => host,
    "CONFORMANCE_DIAGNOSTICS" => File.join(artifacts, "adapter-stderr.log"),
    "CONNECTRPC_CONFORMANCE_PER_CALL_PATH" => per_call }
end

# CI keeps no artifacts, so a failed accounting shows the adapter's stderr.
def report_adapter_stderr(artifacts)
  path = File.join(artifacts, "adapter-stderr.log")
  lines = File.exist?(path) ? File.readlines(path) : []
  return if lines.empty?

  warn "Adapter stderr (last #{[lines.size, 400].min} of #{lines.size} lines, #{path}):"
  warn lines.last(400).join
end

# Names one run's artifact directory.
def run_stamp(host, protocol, per_call, profile, direction)
  "#{Time.now.utc.strftime('%Y%m%dT%H%M%S')}-#{Process.pid}-#{RUBY_VERSION}-" \
    "#{host}-#{protocol}#{"-#{per_call}" if per_call}-#{profile}-#{direction}"
end

success = true
runs.each do |profile, direction|
  path = per_call ? { path: per_call } : {}
  ledger = ledger_type.new(profile: profile, direction: direction, selected: selector, **path)
  artifacts = File.join(root, "vendor/conformance/results", run_stamp(host, protocol, per_call, profile, direction))
  telemetry = native_telemetry(artifacts)
  command = native_command(root, profile, direction, selector, protocol)
  raw, process = Open3.capture2e(native_environment(telemetry, host, artifacts, per_call), *command)
  File.write(File.join(artifacts, "runner.log"), raw)
  puts raw
  begin
    events = File.readlines(telemetry).map { |line| JSON.parse(line) }
    summary = ledger.parse(raw, events, process.exitstatus)
    File.write(File.join(artifacts, "summary.json"), "#{JSON.pretty_generate(summary)}\n")
    puts "Native #{host}/#{"per-call/#{per_call}/" if per_call}#{profile}/#{direction}: " \
         "#{summary.fetch(:executed)} executed; " \
         "#{summary.fetch(:passed)} passed; " \
         "#{summary.fetch(:failed)} failed; artifacts #{artifacts}"
    success &&= process.success?
  rescue StandardError => e
    File.write(File.join(artifacts, "summary.json"),
               "#{JSON.pretty_generate(accounting_valid: false, error: e.message)}\n")
    warn e.message
    report_adapter_stderr(artifacts)
    success = false
  end
end
exit(success ? 0 : 1)
