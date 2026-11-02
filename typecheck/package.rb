# frozen_string_literal: true

require "bundler"
require "rubygems/package"
require "tmpdir"
require_relative "harness"

root = File.expand_path("..", __dir__)
specification = Gem::Specification.load(File.join(root, "connectrpc.gemspec"))
output = File.join(root, "vendor/rbs/pkg", RUBY_VERSION)
FileUtils.mkdir_p(output)
archive = File.join(output, specification.file_name)
Dir.chdir(root) { Gem::Package.build(specification, false, false, archive) }
package = Gem::Package.new(archive)
expected = %w[sig/connectrpc.rbs sig/manifest.yaml rbs/google_protobuf_4_36/binding.rbs
              rbs/google_protobuf_4_36/runtime.rbs].sort
raise "Signature package manifest changed" unless package.contents.grep(%r{\A(?:sig|rbs)/}).sort == expected
raise "Runtime dependency added" unless package.spec.runtime_dependencies.empty?
raise "MIT license metadata missing" unless package.spec.license == "MIT"
raise "Publication guard missing" unless package.spec.metadata["allowed_push_host"] == "https://gems.invalid"

plugin_mode = File.stat(File.join(root, "exe/protoc-gen-connectrpc-ruby")).mode & 0o777
puts "Source plugin mode: #{format('%04o', plugin_mode)} (unchanged)"
artifacts = Bundler.load.specs.reject { |spec| spec.name == "bundler" }.map(&:cache_file)
root_specs = Bundler::LockfileParser.new(File.read(File.join(root, "Gemfile.lock"))).specs
root_cache = File.join(root, "vendor/bundle/ruby", RbConfig::CONFIG.fetch("ruby_version"), "cache")
%w[rake bigdecimal google-protobuf].each do |name|
  version = root_specs.find { |spec| spec.name == name }.version
  candidates = Dir.glob(File.join(root_cache, "#{name}-#{version}*.gem"))
  artifact = candidates.find do |file|
    spec = Gem::Package.new(file).spec
    spec.name == name && spec.version == version && Gem::Platform.match_spec?(spec)
  end
  raise "Missing cached root bundle artifact #{name}" unless artifact

  artifacts << artifact
end
raise "Missing offline tool archives" unless artifacts.all? { |file| File.file?(file) }

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
    %w[consumers nominal runtime.rb harness.rb].each { |name| FileUtils.cp_r(File.join(__dir__, name), working) }
    FileUtils.cp_r(File.join(root, "spec/fixtures/rbs/proto"), working)
    FileUtils.cp(artifacts, working)
    environment = { "GEM_HOME" => home, "GEM_PATH" => home, "RUBYLIB" => nil, "RUBYOPT" => nil,
                    "RUBYGEMS_GEMDEPS" => nil }
    ENV.each_key { |name| environment[name] = nil if name.start_with?("BUNDLE_", "BUNDLER_") }
    environment["PATH"] = "#{File.join(home, 'bin')}#{File::PATH_SEPARATOR}#{ENV.fetch('PATH')}"
    previous = environment.to_h { |name, _value| [name, ENV.fetch(name, nil)] }
    begin
      environment.each { |name, value| value ? ENV[name] = value : ENV.delete(name) }
      RBSHarness.capture!([RbConfig.ruby, "-S", "gem", "install", "--norc", "--local", "--no-document",
                           archive, *artifacts], working: working)
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
      evidence = File.join(root, "vendor/rbs/installed", RUBY_VERSION)
      FileUtils.mkdir_p(evidence)
      FileUtils.cp(Dir.glob(File.join(working, "*.{json,log}")), evidence)
      puts "Installed executable #{plugin}; core/bridge discovered from #{installed}"
    ensure
      previous.each { |name, value| value ? ENV[name] = value : ENV.delete(name) }
    end
  end
end
puts "Offline installed gem/executable and strict positive/negative consumers passed (Ruby #{RUBY_VERSION})"
