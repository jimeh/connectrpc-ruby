# frozen_string_literal: true

require "bundler"
require "rubygems/package"
require "tmpdir"
require_relative "harness"
require_relative "installed_api"
require_relative "archive"
require_relative "../scripts/package_build"

root = File.expand_path("..", __dir__)
output = File.join(root, "vendor/rbs/pkg", RUBY_VERSION)
archive = PackageBuild.build(directory: root, output: output)
SignatureArchive.check(archive, root: root)
package = Gem::Package.new(archive)
expected = %w[sig/connectrpc.rbs sig/manifest.yaml
              rbs/google_protobuf_4_36/runtime.rbs].sort
raise "Signature package manifest changed" unless package.contents.grep(%r{\A(?:sig|rbs)/}).sort == expected
raise "Runtime dependency added" unless package.spec.runtime_dependencies.empty?
raise "MIT license metadata missing" unless package.spec.license == "MIT"
raise "Push host missing" unless package.spec.metadata["allowed_push_host"] == "https://rubygems.org"

plugin_mode = File.stat(File.join(root, "exe/protoc-gen-connectrpc-ruby")).mode & 0o777
puts "Source plugin mode: #{format('%04o', plugin_mode)} (unchanged)"
artifacts = Bundler.load.specs.reject { |spec| spec.name == "bundler" }.map(&:cache_file)
raise "Missing offline tool archives" unless artifacts.all? { |file| File.file?(file) }

# Generated consumers need the exact google-protobuf development pin; RubyGems
# resolves its dependencies from the same ABI-scoped caches as installed packages.
protobuf = "google-protobuf:4.36.2"

Bundler.with_unbundled_env do
  Dir.mktmpdir("connectrpc-rbs-package-", ENV.fetch("CONNECTRPC_RBS_PACKAGE_TMPDIR", nil)) do |temporary|
    raise "Package consumer must be outside checkout" if File.realpath(temporary).start_with?("#{File.realpath(root)}/")

    notice = File.join(temporary, "notice")
    package.extract_files(notice, "LICENSE")
    unless File.binread(File.join(notice, "LICENSE")) == File.binread(File.join(root, "LICENSE"))
      raise "Packaged license differs from root LICENSE"
    end

    working = File.join(temporary, "consumer")
    home = File.join(temporary, "gems")
    FileUtils.mkdir_p(working)
    %w[consumers nominal runtime.rb harness.rb negative_check.rb].each do |name|
      FileUtils.cp_r(File.join(__dir__, name), working)
    end
    FileUtils.cp_r(File.join(root, "spec/fixtures/rbs/proto"), working)
    FileUtils.cp(artifacts, working)
    FileUtils.cp(InstalledAPI.cached_archives(root: root), working)
    environment = { "GEM_HOME" => home, "GEM_PATH" => home, "RUBYLIB" => nil, "RUBYOPT" => nil,
                    "RUBYGEMS_GEMDEPS" => nil }
    ENV.each_key { |name| environment[name] = nil if name.start_with?("BUNDLE_", "BUNDLER_") }
    environment["PATH"] = "#{File.join(home, 'bin')}#{File::PATH_SEPARATOR}#{ENV.fetch('PATH')}"
    previous = environment.to_h { |name, _value| [name, ENV.fetch(name, nil)] }
    begin
      environment.each { |name, value| value ? ENV[name] = value : ENV.delete(name) }
      RBSHarness.capture!([RbConfig.ruby, "-S", "gem", "install", "--norc", "--local", "--no-document",
                           archive, *artifacts, protobuf], working: working)
      installed = RBSHarness.capture!([RbConfig.ruby, "-e", <<~CODE], working: working).strip
        require 'connectrpc'
        spec = Gem.loaded_specs.fetch('connectrpc')
        abort 'Not normally installed' unless spec.full_gem_path.start_with?(ENV.fetch('GEM_HOME') + '/')
        abort 'Optional Google loaded' if defined?(Google)
        puts spec.full_gem_path
      CODE
      generated = File.join(working, "generated")
      FileUtils.mkdir_p(generated)
      plugin = File.join(home, "bin/protoc-gen-connectrpc-ruby")
      inputs = Dir.glob("*.proto", base: File.join(working, "proto")).sort
      RBSHarness.capture!(["protoc", "-I", File.join(working, "proto"), "--ruby_out=#{generated}",
                           "--rbs_out=#{generated}", "--connectrpc-ruby_out=rbs=true:#{generated}",
                           "--plugin=protoc-gen-connectrpc-ruby=#{plugin}", *inputs,
                           "google/protobuf/timestamp.proto", "google/protobuf/empty.proto"], working: working)
      puts RBSHarness.capture!([RbConfig.ruby, "-I", generated, "runtime.rb"], working: working)
      # The runner itself executes from this copy, so installed checks have no
      # checkout script/load paths and library "connectrpc" uses gem discovery.
      runner = <<~CODE
        require_relative 'harness'
        RBSHarness.check(working: Dir.pwd, bridge: #{File.join(installed, 'rbs/google_protobuf_4_36').dump},
                         generated: #{generated.dump})
      CODE
      File.write(File.join(working, "installed_check.rb"), runner)
      puts RBSHarness.capture!([RbConfig.ruby, "installed_check.rb"], working: working)
      InstalledAPI.check(root: root, working: working, output: output)
      puts "Installed executable #{plugin}; core/bridge discovered from #{installed}"
    ensure
      previous.each { |name, value| value ? ENV[name] = value : ENV.delete(name) }
      # Preserve complete batch output, including a failed installed API check,
      # before the external temporary consumer is removed.
      Dir.glob("{,installed-api/}*.{json,log}", base: working).each do |relative|
        target = File.join(root, "vendor/rbs/installed", RUBY_VERSION, relative)
        FileUtils.mkdir_p(File.dirname(target))
        FileUtils.cp(File.join(working, relative), target)
      end
    end
  end
end
puts "Offline installed gem/executable and strict positive/negative consumers passed (Ruby #{RUBY_VERSION})"
