# frozen_string_literal: true

require "bundler"
require "fileutils"
require "rbconfig"
require "rubygems/package"
require "stringio"
require "tmpdir"

root = File.expand_path("../../..", __dir__)
package_root = File.dirname(__dir__)
output = File.join(root, "vendor/testing/pkg")
core_archive = File.join(output, "connectrpc-0.1.0.gem")
testing_archive = File.join(output, "connectrpc-testing-0.1.0.gem")
core = Gem::Package.new(core_archive)
testing = Gem::Package.new(testing_archive)
expected_core = %w[
  LICENSE README.md docs/development.md docs/design.md docs/rbs.md exe/protoc-gen-connectrpc-ruby
  sig/connectrpc.rbs sig/manifest.yaml rbs/google_protobuf_4_36/runtime.rbs rbs/google_protobuf_4_36/binding.rbs
  lib/connectrpc/generator/rbs_renderer.rb
  lib/connectrpc.rb lib/connectrpc/version.rb lib/connectrpc/method.rb lib/connectrpc/metadata.rb
  lib/connectrpc/error.rb lib/connectrpc/deadline.rb lib/connectrpc/compression.rb lib/connectrpc/message_io.rb
  lib/connectrpc/protocol.rb lib/connectrpc/client.rb lib/connectrpc/server.rb lib/connectrpc/instrumentation.rb
  lib/connectrpc/bindings/google_protobuf.rb lib/connectrpc/transports/net_http.rb
  lib/connectrpc/generator.rb lib/connectrpc/generator/names.rb lib/connectrpc/generator/schema.rb
  lib/connectrpc/generator/renderer.rb lib/connectrpc/generator/runtime_names.rb
].sort
expected_testing = %w[LICENSE README.md lib/connectrpc/testing.rb lib/connectrpc/testing/transport.rb].sort
abort "Unexpected core manifest" unless core.contents.sort == expected_core
abort "Unexpected testing manifest" unless testing.contents.sort == expected_testing
abort "Core runtime dependencies changed" unless core.spec.runtime_dependencies.empty?
dependencies = testing.spec.runtime_dependencies
abort "Unexpected testing dependencies" unless dependencies.size == 1 && dependencies.first.name == "connectrpc" &&
                                               dependencies.first.requirement.to_s == "~> 0.1.0"
[core, testing].each do |archive|
  abort "Publication guard missing" unless archive.spec.metadata["allowed_push_host"] == "https://gems.invalid"
  abort "MIT metadata missing" unless archive.spec.licenses == ["MIT"]
end
license = File.binread(File.join(root, "LICENSE"))
abort "Testing license differs from root LICENSE" unless File.binread(File.join(package_root, "LICENSE")) == license
abort "Core executable metadata changed" unless core.spec.bindir == "exe" &&
                                                core.spec.executables == ["protoc-gen-connectrpc-ruby"]
# Check archive permissions as well as the gemspec executable metadata.
mode_failure = false
Gem::Package::TarReader.new(File.open(core_archive, "rb")) do |tar|
  tar.each do |entry|
    next unless entry.full_name == "data.tar.gz"

    Gem::Package::TarReader.new(Zlib::GzipReader.new(StringIO.new(entry.read))) do |data|
      data.each do |file|
        abort "Core archived MIT notice differs" if file.full_name == "LICENSE" && file.read != license

        next unless file.full_name == "exe/protoc-gen-connectrpc-ruby"

        mode_failure = file.header.mode & 0o777 != 0o755
        warn "Core package executable mode: #{(file.header.mode & 0o777).to_s(8)}; expected 755" if mode_failure
      end
    end
  end
end
artifacts = %w[rake bigdecimal google-protobuf].map do |name|
  cached = Bundler.load.specs.find { |spec| spec.name == name }&.cache_file
  abort "Missing cached #{name}" unless cached && File.file?(cached)

  cached
end
abort "Copyright missing" unless license.include?("Copyright (c) 2026 Jim Myhrberg")
Gem::Package::TarReader.new(File.open(testing_archive, "rb")) do |tar|
  tar.each do |entry|
    next unless entry.full_name == "data.tar.gz"

    Gem::Package::TarReader.new(Zlib::GzipReader.new(StringIO.new(entry.read))) do |data|
      data.each do |file|
        next unless file.full_name == "LICENSE"

        abort "Archived MIT notice differs" unless file.read == license
      end
    end
  end
end
Bundler.with_unbundled_env do
  Dir.mktmpdir("connectrpc-testing-package-") do |temporary|
    working = File.join(temporary, "cwd")
    gem_home = File.join(temporary, "gems")
    FileUtils.mkdir_p(working)
    FileUtils.cp([core_archive, testing_archive, *artifacts], working)
    FileUtils.cp(File.join(root, "LICENSE"), File.join(working, "expected-license"))
    FileUtils.cp(File.join(__dir__, "integration.rb"), working)
    FileUtils.cp(Dir.glob(File.join(root, "spec/fixtures/generation/echo/*.rb")), working)
    environment = { "GEM_HOME" => gem_home, "GEM_PATH" => gem_home, "RUBYOPT" => nil,
                    "RUBYLIB" => nil, "RUBYGEMS_GEMDEPS" => nil }
    ENV.each_key { |name| environment[name] = nil if name.start_with?("BUNDLE_", "BUNDLER_") }
    run = lambda do |*arguments|
      system(environment, RbConfig.ruby, *arguments, chdir: working, exception: true)
    end
    abort "Isolation home unexpectedly exists" if File.exist?(gem_home)
    # Install only testing. RubyGems must discover the adjacent core archive itself.
    run.call("-S", "gem", "install", "--norc", File.basename(testing_archive), "--local", "--no-document")
    run.call("-e", <<~'CODE')
      require "connectrpc/testing"
      abort "Optional runtime loaded" if defined?(Google::Protobuf) || defined?(Rack) || defined?(Rails) ||
                                         defined?(Async) || defined?(RSpec)
      expected = %w[connectrpc connectrpc-testing]
      expected.each do |name|
        spec = Gem.loaded_specs.fetch(name)
        abort "Loaded outside GEM_HOME" unless spec.full_gem_path.start_with?("#{ENV.fetch('GEM_HOME')}/")
        abort "Push guard missing" unless spec.metadata["allowed_push_host"] == "https://gems.invalid"
      end
      { ConnectRPC::Server => ["connectrpc", "lib/connectrpc/server.rb"],
        ConnectRPC::Testing::Transport => ["connectrpc-testing", "lib/connectrpc/testing/transport.rb"] }.each do |type, files|
        owner, relative = files
        installed_source = File.join(Gem.loaded_specs.fetch(owner).full_gem_path, relative)
        abort "Wrong installed source owner" unless File.realpath(type.instance_method(:call).source_location.first) ==
                                                   File.realpath(installed_source)
      end
      begin
        require "connectrpc/rack"
        abort "Core/testing unexpectedly supplies middleware"
      rescue LoadError => e
        abort "Wrong missing middleware path" unless e.path == "connectrpc/rack"
      end
      unexpected = Gem::Specification.reject(&:default_gem?).map(&:name) - expected
      abort "Unexpected installed gems: #{unexpected}" unless unexpected.empty?
      expected.each do |name|
        notice = File.binread(File.join(Gem.loaded_specs.fetch(name).full_gem_path, "LICENSE"))
        abort "Installed MIT notice differs" unless notice == File.binread("expected-license")
      end
      puts "Fresh isolated require passed; only testing and normally resolved core installed (Ruby #{RUBY_VERSION})"
    CODE
    artifacts.each do |artifact|
      run.call("-S", "gem", "install", "--norc", File.basename(artifact), "--local", "--no-document")
    end
    run.call("-I", working, "integration.rb")
    puts "Installed archive manifests, MIT notice, dependency and push guards passed"
  end
end
abort "Core package executable mode must be 0755" if mode_failure
