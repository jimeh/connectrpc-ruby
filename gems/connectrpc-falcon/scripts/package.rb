# frozen_string_literal: true

require "bundler"
require "fileutils"
require "rubygems/package"
require "rbconfig"
require "tmpdir"
root = File.expand_path("../../..", __dir__)
archives = %w[connectrpc connectrpc-async connectrpc-falcon].map do |name|
  File.join(root, "vendor/falcon/pkg/#{name}-0.1.0.gem")
end
packages = archives.map { |archive| Gem::Package.new(archive) }
expected = %w[LICENSE README.md examples/falcon.rb lib/connectrpc/falcon.rb sig/connectrpc-falcon.rbs sig/manifest.yaml
              rbs/async_service_0_25/service.rbs lib/connectrpc/falcon/endpoint.rb
              lib/connectrpc/falcon/environment.rb lib/connectrpc/falcon/http1.rb
              lib/connectrpc/falcon/rack_guard.rb lib/connectrpc/falcon/server.rb].sort
abort "Falcon manifest changed" unless packages.last.contents.sort == expected
expected_dependencies = {
  "connectrpc-async" => "~> 0.1.0", "falcon" => "~> 0.57.0", "protocol-rack" => "~> 0.23.0",
  "async-service" => "~> 0.25.0", "async-container" => "~> 0.38.0", "async-utilization" => "~> 0.5.0",
  "protocol-http1" => "~> 0.41.1"
}
actual = packages.last.spec.runtime_dependencies.to_h { |dependency| [dependency.name, dependency.requirement.to_s] }
abort "Falcon runtime dependencies changed" unless actual == expected_dependencies
packages.each do |package|
  unless package.spec.metadata["allowed_push_host"] == "https://gems.invalid" && package.spec.license == "MIT"
    abort "Publication guard missing"
  end
end
unless File.binread(File.expand_path("../LICENSE", __dir__)) == File.binread(File.join(root, "LICENSE"))
  abort "Falcon license differs from root LICENSE"
end
specifications = Bundler.load.specs.reject do |spec|
  %w[connectrpc connectrpc-async connectrpc-falcon bundler].include?(spec.name)
end
artifacts = specifications.map do |spec|
  abort "Missing offline archive #{spec.full_name}" unless File.file?(spec.cache_file)

  spec.cache_file
end
Bundler.with_unbundled_env do
  Dir.mktmpdir("connectrpc-falcon-package-") do |temporary|
    packages.each do |package|
      manifest = File.join(temporary, package.spec.name)
      package.extract_files(manifest, "LICENSE")
      unless File.binread(File.join(manifest, "LICENSE")) == File.binread(File.join(root, "LICENSE"))
        abort "Packaged MIT notice changed: #{package.spec.name}"
      end
    end

    working = File.join(temporary, "outside-checkout")
    FileUtils.mkdir_p(working)
    FileUtils.cp(artifacts + archives, working)
    FileUtils.cp(File.join(__dir__, "integration.rb"), working)
    FileUtils.cp(File.join(__dir__, "../examples/falcon.rb"), working)
    FileUtils.cp(File.join(root, "gems/connectrpc-async/spec/support/helpers.rb"), File.join(working, "helpers.rb"))
    FileUtils.cp_r(File.join(root, "spec/fixtures/generation/echo"), File.join(working, "generated"))
    environment = { "GEM_HOME" => File.join(temporary, "home"), "GEM_PATH" => File.join(temporary, "home"),
                    "RUBYOPT" => nil, "RUBYLIB" => nil, "RUBYGEMS_GEMDEPS" => nil,
                    "CONNECTRPC_FALCON_INSTALLED" => "1" }
    ENV.each_key { |name| environment[name] = nil if name.start_with?("BUNDLE_", "BUNDLER_") }
    commands = [
      [RbConfig.ruby, "-S", "gem", "install", "--norc", archives.first, "--local", "--no-document"],
      [RbConfig.ruby, "-e", 'require "connectrpc"; abort if defined?(ConnectRPC::Async) || defined?(Rack)'],
      [RbConfig.ruby, "-S", "gem", "install", "--norc", File.join(working, "json-3.1.0.gem"), "--local",
       "--no-document"],
      [RbConfig.ruby, "-S", "gem", "install", "--norc", archives.last, "--local", "--no-document"],
      [RbConfig.ruby, "-S", "gem", "install", "--norc", "google-protobuf", "--version", "4.36.2", "--local",
       "--no-document"],
      [RbConfig.ruby, "-S", "gem", "install", "--norc", "webrick", "--version", "1.9.2", "--local", "--no-document"],
      [RbConfig.ruby, "integration.rb", File.join(working, "falcon.rb")]
    ]
    commands.each { |command| system(environment, *command, chdir: working, exception: true) }
  end
end
puts "Exact Falcon manifest, normal offline dependencies, actual DSL/workers and publication guards passed"
