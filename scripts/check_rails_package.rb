# frozen_string_literal: true

require "bundler"
require "fileutils"
require "rubygems/package"
require "rbconfig"
require "tmpdir"

root = File.expand_path("..", __dir__)
archives = %w[connectrpc connectrpc-rails].map { |name| File.join(root, "vendor/rails/pkg/#{name}-0.1.0.gem") }
core, adapter = archives.map { |archive| Gem::Package.new(archive) }
core_files = %w[
  README.md docs/development.md docs/design.md lib/connectrpc.rb lib/connectrpc/version.rb
  exe/protoc-gen-connectrpc-ruby lib/connectrpc/generator.rb lib/connectrpc/generator/names.rb
  lib/connectrpc/generator/schema.rb lib/connectrpc/generator/renderer.rb lib/connectrpc/generator/runtime_names.rb
  lib/connectrpc/method.rb lib/connectrpc/metadata.rb lib/connectrpc/error.rb lib/connectrpc/deadline.rb
  lib/connectrpc/message_io.rb lib/connectrpc/protocol.rb lib/connectrpc/client.rb
  lib/connectrpc/server.rb lib/connectrpc/rack.rb
  lib/connectrpc/bindings/google_protobuf.rb lib/connectrpc/transports/net_http.rb
].sort
abort "Core manifest changed" unless core.contents.sort == core_files.sort && core.spec.runtime_dependencies.empty?
expected = %w[README.md lib/connectrpc/rails.rb lib/connectrpc/rails/controller.rb]
abort "Unexpected Rails manifest" unless adapter.contents.sort == expected.sort
expected_dependencies = {
  "connectrpc" => "~> 0.1.0", "actionpack" => ">= 8.0.5.1, < 8.2", "rack" => ">= 3.2.7, < 4"
}
actual = adapter.spec.runtime_dependencies.to_h { |dependency| [dependency.name, dependency.requirement.to_s] }
abort "Unexpected Rails dependencies: #{actual}" unless actual == expected_dependencies
[core, adapter].each do |package|
  unless package.spec.metadata["allowed_push_host"] == "https://gems.invalid" && package.spec.license == "Nonstandard"
    abort "Publication guard/license missing"
  end
end
specs = Bundler.load.specs.reject { |spec| %w[connectrpc connectrpc-rails bundler].include?(spec.name) }
artifacts = specs.map do |spec|
  abort "Missing offline archive #{spec.full_name}" unless File.file?(spec.cache_file)

  spec.cache_file
end
core_check = <<~CODE
  require "connectrpc"
  abort "Core loaded Rails" if defined?(Rails) || defined?(ActionController)
  puts "Isolated core without Rails passed"
CODE
Bundler.with_unbundled_env do
  Dir.mktmpdir("connectrpc-rails-package-", ENV.fetch("CONNECTRPC_RAILS_TMPDIR", nil)) do |temporary|
    working = File.join(temporary, "outside-checkout")
    FileUtils.mkdir_p(working)
    FileUtils.cp(artifacts + archives, working)
    FileUtils.cp(File.join(root, "scripts/installed_rails_integration.rb"), File.join(working, "integration.rb"))
    FileUtils.cp_r(File.join(root, "spec/fixtures/generation/echo"), File.join(working, "generated"))
    environment = { "GEM_HOME" => File.join(temporary, "home"), "GEM_PATH" => File.join(temporary, "home"),
                    "RUBYOPT" => nil, "RUBYLIB" => nil, "RUBYGEMS_GEMDEPS" => nil,
                    "CONNECTRPC_RAILS_VERSION" => ENV.fetch("CONNECTRPC_RAILS_VERSION") }
    ENV.each_key { |name| environment[name] = nil if name.start_with?("BUNDLE_", "BUNDLER_") }
    commands = [
      [RbConfig.ruby, "-S", "gem", "install", "--norc", archives.first, "--local", "--no-document"],
      [RbConfig.ruby, "-e", core_check],
      [RbConfig.ruby, "-S", "gem", "install", "--norc", archives.last, "--local", "--no-document"],
      [RbConfig.ruby, "-S", "gem", "install", "--norc", "railties", "--version", ENV.fetch("CONNECTRPC_RAILS_VERSION"),
       "--local", "--no-document"],
      [RbConfig.ruby, "-S", "gem", "install", "--norc", "google-protobuf", "--version", "4.36.2", "--local",
       "--no-document"],
      [RbConfig.ruby, "integration.rb"]
    ]
    commands.each do |command|
      system(environment, *command, chdir: working, exception: true)
    end
  end
end
puts "Exact core/Rails manifests, offline dependencies and publication guards passed"
puts "Installed Rails integration passed"
