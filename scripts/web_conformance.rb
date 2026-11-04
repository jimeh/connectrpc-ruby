# frozen_string_literal: true

require "fileutils"
require "json"
require "open3"
require "rbconfig"
require_relative "../conformance/web_ledger"

root = File.expand_path("..", __dir__)
host = ARGV.shift || "async"
selector = ARGV.shift
raise "Usage: web_conformance.rb async|falcon|rack [exact-name]" unless %w[async falcon
                                                                           rack].include?(host) && ARGV.empty?

version, status = Open3.capture2("connectconformance", "--version")
raise "Wrong conformance runner" unless status.success? && version.strip == "connectconformance v1.0.6"

stamp = "#{Time.now.utc.strftime('%Y%m%dT%H%M%S')}-#{Process.pid}-#{RUBY_VERSION}-#{host}-web"
directory = File.join(root, "vendor/conformance/results", stamp)
FileUtils.mkdir_p(directory)
environment = { "CONNECTRPC_CONFORMANCE_WEB" => "1", "CONNECTRPC_NATIVE_HOST" => host,
                "CONFORMANCE_TELEMETRY" => File.join(directory, "telemetry.jsonl"),
                "CONFORMANCE_DIAGNOSTICS" => File.join(directory, "adapter-stderr.log") }
command = ["connectconformance", "--mode", "server", "--conf", File.join(root, "conformance/profile-web-#{host}.yaml"),
           "--vv"]
command.push("--run", selector) if selector
command.push("--", RbConfig.ruby, "-I#{File.join(root, 'lib')}", File.join(root, "conformance/native_server.rb"))
raw = +""
process = nil
File.open(File.join(directory, "runner.log"), "w") do |log|
  Open3.popen2e(environment, *command) do |input, output, worker|
    input.close
    output.each_line do |line|
      raw << line
      log.write(line)
      log.flush
    end
    process = worker.value
  end
end
puts raw
events = File.readlines(environment.fetch("CONFORMANCE_TELEMETRY")).map { |line| JSON.parse(line) }
summary = Conformance::WebLedger.new(host: host, selected: selector).parse(raw, events, process.exitstatus)
File.write(File.join(directory, "summary.json"), "#{JSON.pretty_generate(summary)}\n")
puts "Web #{host}: #{summary.fetch(:executed)} executed; #{summary.fetch(:passed)} passed; artifacts #{directory}"
exit(process.success? && summary.fetch(:failed).zero? ? 0 : 1)
