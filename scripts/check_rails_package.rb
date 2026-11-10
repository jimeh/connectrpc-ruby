# frozen_string_literal: true

require "bundler"
require "fileutils"
require "rubygems/package"
require "rbconfig"
require "tmpdir"

require_relative "release_train"

root = File.expand_path("..", __dir__)
version = ReleaseTrain.new(root).version
local_requirement = ReleaseTrain.new(root).requirement
archives = %w[connectrpc connectrpc-rack connectrpc-rails].map do |name|
  File.join(root, "vendor/rails/pkg/#{name}-#{version}.gem")
end
core, middleware, adapter = archives.map { |archive| Gem::Package.new(archive) }
core_files = %w[
  LICENSE README.md docs/development.md docs/design.md docs/rbs.md docs/streaming.md lib/connectrpc.rb
  lib/connectrpc/version.rb
  sig/connectrpc.rbs sig/manifest.yaml rbs/google_protobuf_4_36/runtime.rbs
  lib/connectrpc/generator/rbs_renderer.rb lib/connectrpc/generator/streaming_methods.rb
  lib/connectrpc/streaming/method.rb lib/connectrpc/streaming/framing.rb lib/connectrpc/streaming/streams.rb
  lib/connectrpc/streaming/client.rb lib/connectrpc/streaming/server.rb
  lib/connectrpc/streaming/client_execution.rb lib/connectrpc/streaming/client_scope.rb
  lib/connectrpc/streaming/framing_io.rb lib/connectrpc/streaming/server_execution.rb
  lib/connectrpc/streaming/server_terminal.rb lib/connectrpc/streaming/session.rb
  lib/connectrpc/streaming/session_completion.rb lib/connectrpc/streaming/session_io.rb
  lib/connectrpc/streaming/session_messages.rb lib/connectrpc/streaming/terminal.rb
  lib/connectrpc/grpc.rb lib/connectrpc/grpc_client.rb lib/connectrpc/grpc_server.rb
  lib/connectrpc/grpc_web.rb lib/connectrpc/grpc_web_io.rb lib/connectrpc/grpc_web_cors.rb
  lib/connectrpc/grpc_web_server.rb
  lib/connectrpc/streaming/strategy.rb lib/connectrpc/bindings/descriptors/status.bin docs/grpc.md docs/grpc-web.md
  third_party/google/rpc/status.proto third_party/google/LICENSE third_party/protobuf-LICENSE third_party/README.md
  exe/protoc-gen-connectrpc-ruby lib/connectrpc/generator.rb lib/connectrpc/generator/names.rb
  lib/connectrpc/generator/schema.rb lib/connectrpc/generator/renderer.rb lib/connectrpc/generator/runtime_names.rb
  lib/connectrpc/method.rb lib/connectrpc/metadata.rb lib/connectrpc/error.rb lib/connectrpc/cancellation.rb
  lib/connectrpc/deadline.rb
  lib/connectrpc/compression.rb lib/connectrpc/message_io.rb
  lib/connectrpc/get_query.rb lib/connectrpc/protocol.rb lib/connectrpc/client.rb
  lib/connectrpc/server.rb lib/connectrpc/instrumentation.rb
  lib/connectrpc/bindings/google_protobuf.rb lib/connectrpc/transports/net_http.rb
].sort
abort "Core manifest changed" unless core.contents.sort == core_files.sort && core.spec.runtime_dependencies.empty?
expected = %w[LICENSE README.md lib/connectrpc/rails.rb sig/connectrpc-rails.rbs sig/manifest.yaml
              rbs/action_controller_8/controller.rbs
              lib/connectrpc/rails/controller.rb]
abort "Unexpected Rails manifest" unless adapter.contents.sort == expected.sort
expected_dependencies = {
  "connectrpc" => local_requirement, "actionpack" => ">= 8.0.5.1, < 8.2", "connectrpc-rack" => local_requirement
}
actual = adapter.spec.runtime_dependencies.to_h { |dependency| [dependency.name, dependency.requirement.to_s] }
abort "Unexpected Rails dependencies: #{actual}" unless actual == expected_dependencies
[core, middleware, adapter].each do |package|
  unless package.spec.metadata["allowed_push_host"] == "https://gems.invalid" &&
         package.spec.license == "MIT"
    abort "Publication guard/license missing"
  end
end
%w[connectrpc-rack connectrpc-rails].each do |name|
  unless File.binread(File.join(root, "gems", name, "LICENSE")) == File.binread(File.join(root, "LICENSE"))
    abort "Package-local license differs from root LICENSE: #{name}"
  end
end
Dir.mktmpdir("connectrpc-license-") do |temporary|
  [core, middleware, adapter].each do |package|
    destination = File.join(temporary, package.spec.name)
    package.extract_files(destination, "LICENSE")
    unless File.binread(File.join(destination, "LICENSE")) == File.binread(File.join(root, "LICENSE"))
      abort "Packaged license differs from root LICENSE: #{package.spec.name}"
    end
  end
end
abort "Rack manifest changed" unless middleware.contents.sort == %w[LICENSE README.md lib/connectrpc/rack.rb
                                                                    sig/connectrpc-rack.rbs sig/manifest.yaml]
rack_dependencies = middleware.spec.runtime_dependencies.to_h do |dependency|
  [dependency.name, dependency.requirement.to_s]
end
abort "Rack dependencies changed" unless rack_dependencies == { "connectrpc" => local_requirement, "rack" => ">= 3.2.7, < 4" }
specs = Bundler.load.specs.to_h { |spec| [spec.name, spec] }
pending = %w[connectrpc-rails railties google-protobuf]
required = {}
until pending.empty?
  name = pending.shift
  next if required.key?(name)

  spec = specs.fetch(name)
  required[name] = spec
  pending.concat(spec.runtime_dependencies.map(&:name))
end
artifacts = required.values.reject { |spec| spec.source.is_a?(Bundler::Source::Path) }.map do |spec|
  abort "Missing offline archive #{spec.full_name}" unless File.file?(spec.cache_file)

  spec.cache_file
end
core_check = <<~'CODE'
  require "connectrpc"
  abort "Core loaded optional hosting" if defined?(Rails) || defined?(ActionController) || defined?(Rack)
  core = Gem.loaded_specs.fetch("connectrpc")
  abort "Core outside isolated GEM_HOME" unless core.full_gem_path.start_with?("#{ENV.fetch('GEM_HOME')}/")
  abort "Core runtime dependencies" unless core.runtime_dependencies.empty?
  begin
    require "connectrpc/rack"
    abort "Core supplies middleware"
  rescue LoadError => e
    abort "Wrong missing middleware path" unless e.path == "connectrpc/rack"
  end
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
      [RbConfig.ruby, "-S", "gem", "install", "--norc", File.basename(archives.first), "--local", "--no-document"],
      [RbConfig.ruby, "-e", core_check],
      [RbConfig.ruby, "-S", "gem", "install", "--norc", File.basename(archives.last), "--local", "--no-document"],
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
