# frozen_string_literal: true

require "bundler"
require "fileutils"
require "rbconfig"
require "rubygems/package"
require "tmpdir"

require_relative "../../../scripts/release_train"

root = File.expand_path("../../..", __dir__)
train = ReleaseTrain.new(root)
version = train.version
output = File.join(root, "vendor/sentry/pkg")
core, testing, sentry = %w[connectrpc connectrpc-testing connectrpc-sentry].map do |name|
  Gem::Package.new(File.join(output, "#{name}-#{version}.gem"))
end
abort "Core runtime dependencies changed" unless core.spec.runtime_dependencies.empty?
expected = %w[LICENSE README.md lib/connectrpc/sentry.rb lib/connectrpc/sentry/subscriber.rb
              lib/connectrpc/sentry/version.rb sig/connectrpc-sentry.rbs sig/manifest.yaml].sort
abort "Sentry manifest changed" unless sentry.contents.sort == expected
expected_dependencies = { "connectrpc" => train.requirement, "sentry-ruby" => ">= 6.7.0, < 8" }
actual = sentry.spec.runtime_dependencies.to_h { |dep| [dep.name, dep.requirement.to_s] }
abort "Sentry runtime closure changed: #{actual}" unless actual == expected_dependencies
license = File.binread(File.join(root, "LICENSE"))
abort "Package license differs" unless File.binread(File.join(File.dirname(__dir__), "LICENSE")) == license
[core, sentry].each do |archive|
  abort "Push host missing" unless archive.spec.metadata["allowed_push_host"] == "https://rubygems.org"
  abort "MFA requirement lost" unless archive.spec.metadata["rubygems_mfa_required"] == "true"
  abort "MIT metadata missing" unless archive.spec.licenses == ["MIT"]
end
registry_specs = Bundler.load.specs.reject { |spec| spec.source.is_a?(Bundler::Source::Path) }
protobuf = registry_specs.find { |entry| entry.name == "google-protobuf" }
abort "Missing cached google-protobuf" unless protobuf && File.file?(protobuf.cache_file)
artifacts = registry_specs.filter_map { |spec| spec.cache_file if File.file?(spec.cache_file) }
Bundler.with_unbundled_env do
  Dir.mktmpdir("connectrpc-sentry-consumer-") do |temporary|
    working = File.join(temporary, "cwd")
    gem_home = File.join(temporary, "gems")
    FileUtils.mkdir_p(working)
    FileUtils.cp([core, testing, sentry].map { |archive| archive.gem.path } + artifacts, working)
    FileUtils.cp(File.join(__dir__, "integration.rb"), working)
    FileUtils.cp(Dir.glob(File.join(root, "spec/fixtures/generation/echo/*.rb")), working)
    environment = { "GEM_HOME" => gem_home, "GEM_PATH" => gem_home, "RUBYOPT" => nil, "RUBYLIB" => nil,
                    "RUBYGEMS_GEMDEPS" => nil }
    ENV.each_key { |key| environment[key] = nil if key.start_with?("BUNDLE_", "BUNDLER_") }
    run = ->(*args) { system(environment, RbConfig.ruby, *args, chdir: working, exception: true) }
    run.call("-S", "gem", "install", "--norc", "connectrpc-sentry-#{version}.gem", "--local", "--no-document")
    run.call("-e", <<~'CODE')
      require "connectrpc/sentry"
      abort "Optional integrations auto-required" if defined?(Rails) || defined?(Sentry::Rails) ||
                                                    defined?(Sentry::OpenTelemetry) || defined?(OpenTelemetry)
      abort "Other optional runtime loaded" if defined?(Rack::Builder) || defined?(Async) || defined?(Google::Protobuf)
      abort "SDK configured on require" if Sentry.initialized?
      expected = %w[connectrpc connectrpc-sentry sentry-ruby concurrent-ruby bigdecimal logger]
      extras = Gem::Specification.reject(&:default_gem?).map(&:name) - expected
      abort "Unexpected installed closure: #{extras}" unless extras.empty?
      %w[connectrpc connectrpc-sentry sentry-ruby].each do |name|
        abort "Source leak" unless Gem.loaded_specs.fetch(name).full_gem_path.start_with?(ENV.fetch("GEM_HOME") + "/")
      end
      puts "Sentry require without Rails from normal offline dependencies outside checkout passed " \
           "(sentry-ruby #{Sentry::VERSION})"
    CODE
    ["connectrpc-testing-#{version}.gem", File.basename(protobuf.cache_file)].each do |name|
      run.call("-S", "gem", "install", "--norc", name, "--local", "--no-document")
    end
    run.call("-I", working, "integration.rb")
    puts "Exact manifest, runtime closure, MIT, push host and fresh installed generated consumer passed"
  end
end
