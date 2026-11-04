# frozen_string_literal: true

# Fail closed if Mise or runner PATH replaces an action-provided runtime.
require "etc"
require "json"
require "open3"
require "rbconfig"
gem "bundler", "= 4.0.23"
require "bundler"

root = File.expand_path("..", __dir__)
check = ->(condition, message) { abort "CI runtime guard: #{message}" unless condition }
tool_cache = File.realpath(ENV.fetch("RUNNER_TOOL_CACHE"))
under_cache = ->(file) { File.realpath(file).start_with?("#{tool_cache}/") }
which = lambda do |name|
  ENV.fetch("PATH").split(File::PATH_SEPARATOR).map { |entry| File.join(entry, name) }
     .find { |entry| File.file?(entry) && File.executable?(entry) } || abort("Missing #{name} on PATH")
end
ruby = which.call("ruby")
check.call(ENV.fetch("CI_EXPECTED_RUBY_VERSION") == RUBY_VERSION, "unexpected Ruby #{RUBY_VERSION}")
check.call(under_cache.call(ruby) && under_cache.call(RbConfig.ruby), "Ruby is outside RUNNER_TOOL_CACHE")
check.call(File.realpath(ruby) == File.realpath(RbConfig.ruby), "PATH Ruby differs from running Ruby")
check.call(Bundler::VERSION == "4.0.23", "unexpected Bundler #{Bundler::VERSION}")
{
  "BUNDLE_IGNORE_CONFIG" => "true", "BUNDLE_FROZEN" => "true",
  "BUNDLE_DISABLE_SHARED_GEMS" => "true", "BUNDLE_VERSION" => "4.0.23",
  "BUNDLE_PATH" => File.join(root, "vendor/bundle")
}.each { |key, value| check.call(ENV.fetch(key) == value, "unexpected #{key}") }
check.call(Bundler.settings[:path] == File.join(root, "vendor/bundle"), "Bundler path differs from cached path")
puts "Ruby #{RUBY_VERSION}: #{File.realpath(ruby)}; Bundler #{Bundler::VERSION}"
puts "Bundle path: #{Bundler.bundle_path} (cache root: #{Bundler.settings[:path]})"
processors, status = Open3.capture2("nproc")
check.call(status.success?, "nproc failed")
puts "CPUs: #{Etc.nprocessors}; nproc: #{processors.strip}; OS/arch: #{RbConfig::CONFIG.fetch('host_os')}/#{RbConfig::CONFIG.fetch('host_cpu')}"
puts "Runner: #{ENV.fetch('RUNNER_OS', 'unknown')}/#{ENV.fetch('RUNNER_ARCH', 'unknown')}; " \
     "image: #{ENV.fetch('ImageOS', 'unknown')} #{ENV.fetch('ImageVersion', 'unknown')}"

case ARGV
when []
  # Ruby-only jobs need no Go or task-local tool installation.
when ["interop"], ["browser-reference"]
  module_dir, cache_dir = ARGV == ["interop"] ? %w[interop interop] : %w[browser/reference browser/go]
  go = which.call("go")
  check.call(under_cache.call(go), "Go is outside RUNNER_TOOL_CACHE")
  output, status = Open3.capture2(go, "env", "-json", "GOVERSION", "GOROOT", "GOMODCACHE", "GOCACHE",
                                  "GOTOOLCHAIN", "GOWORK", "GOFLAGS", "GOMOD")
  check.call(status.success?, "go env failed")
  context = JSON.parse(output)
  check.call(context.fetch("GOVERSION") == "go1.27.3", "unexpected Go version")
  check.call(under_cache.call(context.fetch("GOROOT")), "GOROOT is outside RUNNER_TOOL_CACHE")
  check.call(Dir.pwd == File.join(root, module_dir), "#{ARGV.first} task has the wrong working directory")
  {
    "GOMODCACHE" => File.join(root, "vendor", cache_dir, "mod"),
    "GOCACHE" => File.join(root, "vendor", cache_dir, "build"),
    "GOTOOLCHAIN" => "local", "GOWORK" => "off", "GOFLAGS" => "-mod=readonly",
    "GOMOD" => File.join(root, module_dir, "go.mod")
  }.each { |key, value| check.call(context.fetch(key) == value, "unexpected Go #{key}") }
  puts "Go: #{File.realpath(go)}; #{JSON.generate(context)}"
else
  abort "Usage: ruby scripts/ci_runtime.rb [interop|browser-reference]"
end
