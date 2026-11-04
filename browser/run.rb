# frozen_string_literal: true

require "json"
require "open3"
require "rbconfig"
require "io/wait"
require_relative "../scripts/browser"

root = BrowserTools::ROOT
cache = BrowserTools::CACHE
log = File.open(File.join(cache, "ruby-fixture-#{RUBY_VERSION}.log"), "w") # rubocop:disable Style/FileOpen -- closed in process-owner ensure
Open3.popen2(RbConfig.ruby, File.join(root, "browser/fixture.rb"), err: log) do |input, output, worker| # rubocop:disable Metrics/BlockLength
  ready = output.wait_readable(30)
  raise "Ruby browser fixture did not become ready; see #{log.path}" unless ready

  line = output.gets
  raise "Ruby browser fixture exited before readiness; see #{log.path}" unless line

  config = JSON.parse(line)
  reference_input, reference_output, reference_worker = Open3.popen2(File.join(cache, "reference"), err: log)
  raise "Independent text reference did not become ready" unless reference_output.wait_readable(10)

  config["reference"] = reference_output.gets&.strip
  raise "Independent text reference exited" unless config["reference"]&.start_with?("http://127.0.0.1:")

  BrowserTools.run(File.join(cache, "reference"), config.fetch("h1"))
  config["chromium"] = File.join(cache, "chromium/chrome-headless-shell-linux64/chrome-headless-shell")
  config_file = File.join(cache, "runtime.json")
  File.write(config_file, JSON.pretty_generate(config))
  FileUtils.mkdir_p(File.join(cache, "tmp"))
  BrowserTools.run({ "BROWSER_FIXTURE" => config_file, "NODE_PATH" => File.join(cache, "node_modules"),
                     "TMPDIR" => File.join(cache, "tmp") },
                   BrowserTools.node, File.join(cache, "node_modules/@playwright/test/cli.js"), "test",
                   "--config=#{File.join(root, 'browser/playwright.config.cjs')}", *ARGV, chdir: root)
ensure
  reference_input&.close
  if reference_worker
    raise "Independent text reference shutdown timed out" unless reference_worker.join(10)
    raise "Independent text reference failed" unless reference_worker.value.success?
  end
  begin
    input.puts("stop") if worker&.alive?
  rescue Errno::EPIPE
    # A failed startup can close stdin before the wait thread notices exit.
    nil
  end
  input.close
  if worker
    raise "Ruby browser fixture shutdown timed out" unless worker.join(10)
    raise "Ruby browser fixture failed: #{worker.value.inspect}; see #{log.path}" unless worker.value.success?
  end
  puts output.read if output
  log.close
end
