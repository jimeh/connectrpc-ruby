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
output = File.join(root, "vendor/hyper/pkg")
core, hyper = %w[connectrpc connectrpc-hyper].map do |name|
  Gem::Package.new(File.join(output, "#{name}-#{version}.gem"))
end
abort "Core runtime dependencies changed" unless core.spec.runtime_dependencies.empty?
abort "Core gained hyper code" if core.contents.any? { |file| file.include?("hyper") }
sources = Dir.glob("ext/connectrpc_hyper/src/*.rs", base: package)
expected = (%w[LICENSE README.md THIRD_PARTY_NOTICES.md Cargo.toml Cargo.lock ext/connectrpc_hyper/Cargo.toml
               ext/connectrpc_hyper/extconf.rb lib/connectrpc/hyper.rb lib/connectrpc/hyper/errors.rb
               lib/connectrpc/hyper/exchange.rb lib/connectrpc/hyper/tls.rb lib/connectrpc/hyper/transport.rb
               lib/connectrpc/hyper/version.rb sig/connectrpc-hyper.rbs sig/manifest.yaml] + sources).sort
expected_sources = %w[body call conn failure gvl holds lib pool runtime tls unary].map do |name|
  "ext/connectrpc_hyper/src/#{name}.rs"
end
abort "Native source set changed: #{sources.sort}" unless sources.sort == expected_sources
abort "Hyper manifest changed: #{hyper.contents.sort - expected} #{expected - hyper.contents}" unless
  hyper.contents.sort == expected
abort "Source gem lost its extension" unless hyper.spec.extensions == ["ext/connectrpc_hyper/extconf.rb"]
abort "Source gem must not be a platform gem" unless hyper.spec.platform.to_s == "ruby"
expected_dependencies = { "connectrpc" => train.requirement, "rb_sys" => ">= 0.9.130, < 1" }
actual = hyper.spec.runtime_dependencies.to_h { |dep| [dep.name, dep.requirement.to_s] }
abort "Hyper runtime closure changed: #{actual}" unless actual == expected_dependencies
license = File.binread(File.join(root, "LICENSE"))
abort "Package license differs" unless File.binread(File.join(package, "LICENSE")) == license
[core, hyper].each do |archive|
  abort "Push host missing" unless archive.spec.metadata["allowed_push_host"] == "https://rubygems.org"
  abort "MFA requirement lost" unless archive.spec.metadata["rubygems_mfa_required"] == "true"
  abort "MIT metadata missing" unless archive.spec.licenses == ["MIT"]
end
system(RbConfig.ruby, File.join(package, "scripts/notices.rb"), "--check", exception: true)

registry_specs = Bundler.load.specs.reject { |spec| spec.source.is_a?(Bundler::Source::Path) }
closure = %w[rb_sys rake-compiler-dock]
artifacts = registry_specs.select { |spec| closure.include?(spec.name) }.map(&:cache_file)
abort "Missing cached rb_sys closure: #{artifacts}" unless artifacts.size == 2 && artifacts.all? { File.file?(it) }
# The locked crate graph is fetched once; the install below builds offline.
cargo_home = File.join(root, "vendor/hyper/cargo")
Bundler.with_unbundled_env do
  Dir.mktmpdir("connectrpc-hyper-archive-") do |extracted|
    hyper.extract_files(extracted)
    %w[Cargo.toml Cargo.lock ext/connectrpc_hyper/Cargo.toml THIRD_PARTY_NOTICES.md].each do |file|
      abort "Archived #{file} differs from source" unless
        File.binread(File.join(extracted, file)) == File.binread(File.join(package, file))
    end
    system({ "CARGO_HOME" => cargo_home }, "cargo", "fetch", "--locked", "--manifest-path",
           File.join(extracted, "Cargo.toml"), exception: true)
  end
  Dir.mktmpdir("connectrpc-hyper-consumer-") do |temporary|
    working = File.join(temporary, "cwd")
    gem_home = File.join(temporary, "gems")
    FileUtils.mkdir_p(working)
    FileUtils.cp([core.gem.path, hyper.gem.path] + artifacts, working)
    FileUtils.cp(File.join(__dir__, "smoke.rb"), working)
    environment = { "GEM_HOME" => gem_home, "GEM_PATH" => gem_home, "RUBYOPT" => nil, "RUBYLIB" => nil,
                    "RUBYGEMS_GEMDEPS" => nil, "CARGO_HOME" => cargo_home, "CARGO_NET_OFFLINE" => "true",
                    "CARGO_TARGET_DIR" => nil, "RB_SYS_CARGO_TARGET_DIR" => nil, "CONNECTRPC_HYPER_LIB_DIR" => nil }
    ENV.each_key { |key| environment[key] = nil if key.start_with?("BUNDLE_", "BUNDLER_") }
    run = ->(*args) { system(environment, RbConfig.ruby, *args, chdir: working, exception: true) }
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    run.call("-S", "gem", "install", "--norc", "connectrpc-hyper-#{version}.gem", "--local", "--no-document")
    built = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
    run.call("-e", <<~'CODE')
      require "connectrpc/hyper"
      abort "Other optional runtime loaded" if defined?(Rack) || defined?(Async) || defined?(Google::Protobuf)
      expected = %w[connectrpc connectrpc-hyper rb_sys rake-compiler-dock]
      extras = Gem::Specification.reject(&:default_gem?).map(&:name) - expected
      abort "Unexpected installed closure: #{extras}" unless extras.empty?
      %w[connectrpc connectrpc-hyper].each do |name|
        abort "Source leak" unless Gem.loaded_specs.fetch(name).full_gem_path.start_with?(ENV.fetch("GEM_HOME") + "/")
      end
      binary = Dir.glob(File.join(Gem.loaded_specs.fetch("connectrpc-hyper").full_gem_path,
                                  "lib/connectrpc/hyper/**/connectrpc_hyper.{so,bundle}"))
      abort "Compiled extension missing: #{binary}" if binary.empty?
      puts "Hyper source gem compiled offline from the locked crate cache and loaded outside the checkout"
    CODE
    run.call("smoke.rb")
    puts "Exact manifest, runtime closure, MIT and notices, push host, MFA, offline source build " \
         "(#{built.round}s) and installed smoke passed"
  end
end
