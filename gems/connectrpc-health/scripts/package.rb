# frozen_string_literal: true

require "bundler"
require "fileutils"
require "rbconfig"
require "rubygems/package"
require "tmpdir"

require_relative "../../../scripts/release_train"

root = File.expand_path("../../..", __dir__)
package = File.expand_path("..", __dir__)
train = ReleaseTrain.new(root)
version = train.version
output = File.join(root, "vendor/health/pkg")
core, testing, health = %w[connectrpc connectrpc-testing connectrpc-health].map do |name|
  Gem::Package.new(File.join(output, "#{name}-#{version}.gem"))
end
abort "Core runtime dependencies changed" unless core.spec.runtime_dependencies.empty?
abort "Core gained health code" if core.contents.any? { |file| file.include?("health") }
expected = %w[LICENSE README.md lib/connectrpc/health.rb lib/connectrpc/health/registry.rb
              lib/connectrpc/health/version.rb lib/connectrpc/health/watcher.rb
              lib/grpc/health/v1/health_connect.rb lib/grpc/health/v1/health_pb.rb proto/LICENSE proto/README.md
              proto/grpc/health/v1/health.proto rbs/grpc_health_v1/grpc/health/v1/health_connect.rbs
              rbs/grpc_health_v1/grpc/health/v1/health_pb.rbs sig/connectrpc-health.rbs sig/manifest.yaml].sort
abort "Health manifest changed" unless health.contents.sort == expected
expected_dependencies = { "connectrpc" => train.requirement, "google-protobuf" => ">= 4.36.2, < 5" }
actual = health.spec.runtime_dependencies.to_h { |dep| [dep.name, dep.requirement.to_s] }
abort "Health runtime closure changed" unless actual == expected_dependencies
license = File.binread(File.join(root, "LICENSE"))
abort "Package license differs" unless File.binread(File.join(package, "LICENSE")) == license
abort "MIT metadata missing" unless core.spec.licenses == ["MIT"]
abort "MIT and Apache-2.0 metadata missing" unless health.spec.licenses == %w[MIT Apache-2.0]
[core, health].each do |archive|
  abort "Push host missing" unless archive.spec.metadata["allowed_push_host"] == "https://rubygems.org"
  abort "MFA requirement lost" unless archive.spec.metadata["rubygems_mfa_required"] == "true"
end
Dir.mktmpdir("connectrpc-health-archive-") do |extracted|
  health.extract_files(extracted)
  %w[lib/grpc/health/v1/health_pb.rb lib/grpc/health/v1/health_connect.rb
     rbs/grpc_health_v1/grpc/health/v1/health_pb.rbs rbs/grpc_health_v1/grpc/health/v1/health_connect.rbs
     proto/grpc/health/v1/health.proto proto/LICENSE].each do |file|
    abort "Archived #{file} differs from source" unless
      File.binread(File.join(extracted, file)) == File.binread(File.join(package, file))
  end
end
registry_specs = Bundler.load.specs.reject { |spec| spec.source.is_a?(Bundler::Source::Path) }
artifacts = registry_specs.filter_map { |spec| spec.cache_file if File.file?(spec.cache_file) }
abort "Missing cached google-protobuf" unless registry_specs.any? { |spec| spec.name == "google-protobuf" }
Bundler.with_unbundled_env do
  Dir.mktmpdir("connectrpc-health-consumer-") do |temporary|
    working = File.join(temporary, "cwd")
    gem_home = File.join(temporary, "gems")
    FileUtils.mkdir_p(working)
    FileUtils.cp([core, testing, health].map { |archive| archive.gem.path } + artifacts, working)
    FileUtils.cp(File.join(__dir__, "integration.rb"), working)
    environment = { "GEM_HOME" => gem_home, "GEM_PATH" => gem_home, "RUBYOPT" => nil, "RUBYLIB" => nil,
                    "RUBYGEMS_GEMDEPS" => nil }
    ENV.each_key { |key| environment[key] = nil if key.start_with?("BUNDLE_", "BUNDLER_") }
    run = ->(*args) { system(environment, RbConfig.ruby, *args, chdir: working, exception: true) }
    run.call("-S", "gem", "install", "--norc", "connectrpc-health-#{version}.gem", "--local", "--no-document")
    run.call("-e", <<~'CODE')
      require "connectrpc/health"
      abort "Other optional runtime loaded" if defined?(Rack) || defined?(Async)
      # google-protobuf brings its own runtime and native build dependencies.
      protobuf = Gem.loaded_specs.fetch("google-protobuf").runtime_dependencies.map(&:name)
      expected = %w[connectrpc connectrpc-health google-protobuf] + protobuf
      extras = Gem::Specification.reject(&:default_gem?).map(&:name) - expected
      abort "Unexpected installed closure: #{extras}" unless extras.empty?
      %w[connectrpc connectrpc-health google-protobuf].each do |name|
        abort "Source leak" unless Gem.loaded_specs.fetch(name).full_gem_path.start_with?(ENV.fetch("GEM_HOME") + "/")
      end
      puts "Health require from normal offline dependencies outside checkout passed"
    CODE
    run.call("-S", "gem", "install", "--norc", "connectrpc-testing-#{version}.gem", "--local", "--no-document")
    run.call("integration.rb")
    puts "Exact manifest, runtime closure, MIT + Apache-2.0, push host and fresh installed generated consumer passed"
  end
end
