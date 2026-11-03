# frozen_string_literal: true

require "bundler"
require "fileutils"
require "rubygems/package"
require "rbconfig"
require "tmpdir"

root = File.expand_path("../../..", __dir__)
archives = %w[connectrpc connectrpc-rack].map { |name| File.join(root, "vendor/rack/pkg/#{name}-0.1.0.gem") }
core, middleware = archives.map { |archive| Gem::Package.new(archive) }
core_expected = %w[
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
  exe/protoc-gen-connectrpc-ruby lib/connectrpc/generator.rb lib/connectrpc/generator/names.rb
  lib/connectrpc/generator/schema.rb lib/connectrpc/generator/renderer.rb lib/connectrpc/generator/runtime_names.rb
  lib/connectrpc/method.rb lib/connectrpc/metadata.rb lib/connectrpc/error.rb lib/connectrpc/cancellation.rb
  lib/connectrpc/deadline.rb
  lib/connectrpc/compression.rb lib/connectrpc/message_io.rb
  lib/connectrpc/get_query.rb lib/connectrpc/protocol.rb lib/connectrpc/client.rb
  lib/connectrpc/server.rb lib/connectrpc/instrumentation.rb
  lib/connectrpc/bindings/google_protobuf.rb lib/connectrpc/transports/net_http.rb
].sort
unless core.contents.sort == core_expected && core.spec.runtime_dependencies.empty?
  abort "Core manifest/dependencies changed"
end
abort "Rack manifest changed" unless middleware.contents.sort == %w[LICENSE README.md lib/connectrpc/rack.rb
                                                                    sig/connectrpc-rack.rbs sig/manifest.yaml]
expected = { "connectrpc" => "~> 0.1.0", "rack" => ">= 3.2.7, < 4" }
actual = middleware.spec.runtime_dependencies.to_h { |dependency| [dependency.name, dependency.requirement.to_s] }
abort "Rack dependencies changed" unless actual == expected
[core, middleware].each do |package|
  unless package.spec.metadata["allowed_push_host"] == "https://gems.invalid" &&
         package.spec.license == "MIT" && package.spec.required_ruby_version.to_s == ">= 3.4"
    abort "Publication guard/license/Ruby requirement changed"
  end
end
unless File.binread(File.join(root, "gems/connectrpc-rack/LICENSE")) == File.binread(File.join(root, "LICENSE"))
  abort "Package-local license differs from root LICENSE"
end
Dir.mktmpdir("connectrpc-license-") do |temporary|
  [core, middleware].each do |package|
    destination = File.join(temporary, package.spec.name)
    package.extract_files(destination, "LICENSE")
    unless File.binread(File.join(destination, "LICENSE")) == File.binread(File.join(root, "LICENSE"))
      abort "Packaged license differs from root LICENSE: #{package.spec.name}"
    end
  end
end
# Only the runtime closure used by the installed caller, not lint/spec/JSON tools.
artifacts = %w[rake bigdecimal google-protobuf rack rackup webrick].map do |name|
  spec = Bundler.load.specs.find { |entry| entry.name == name }
  abort "Missing offline archive #{name}" unless spec && File.file?(spec.cache_file)

  spec.cache_file
end
Bundler.with_unbundled_env do
  Dir.mktmpdir("connectrpc-rack-package-") do |temporary|
    working = File.join(temporary, "outside-checkout")
    FileUtils.mkdir_p(working)
    FileUtils.cp(artifacts + archives, working)
    FileUtils.cp(File.join(root, "examples/localhost.rb"), File.join(working, "localhost.rb"))
    FileUtils.cp(File.join(root, "scripts/installed_integration.rb"), File.join(working, "integration.rb"))
    environment = { "GEM_HOME" => File.join(temporary, "home"), "GEM_PATH" => File.join(temporary, "home"),
                    "RUBYOPT" => nil, "RUBYLIB" => nil, "RUBYGEMS_GEMDEPS" => nil }
    ENV.each_key { |name| environment[name] = nil if name.start_with?("BUNDLE_", "BUNDLER_") }
    commands = [
      [RbConfig.ruby, "-S", "gem", "install", "--norc", File.basename(archives.first), "--local", "--no-document"],
      [RbConfig.ruby, "-e", <<~'RUBY']
        require "connectrpc"
        abort "Core loaded optional hosting" if defined?(Rack) || defined?(Rails)
        core = Gem.loaded_specs.fetch("connectrpc")
        abort "Core runtime dependencies" unless core.runtime_dependencies.empty?
        abort "Core outside isolated GEM_HOME" unless core.full_gem_path.start_with?("#{ENV.fetch('GEM_HOME')}/")
        begin
          require "connectrpc/rack"
          abort "Core supplies middleware"
        rescue LoadError => e
          abort "Wrong missing middleware path" unless e.path == "connectrpc/rack"
        end
        puts "Standalone core has no Rack dependency or middleware"
      RUBY
    ]
    commands << [RbConfig.ruby, "-S", "gem", "install", "--norc", File.basename(archives.last), "--local",
                 "--no-document"]
    %w[google-protobuf rackup webrick].each do |name|
      spec = artifacts.find { |artifact| File.basename(artifact).start_with?("#{name}-") }
      commands << [RbConfig.ruby, "-S", "gem", "install", "--norc", File.basename(spec), "--local", "--no-document"]
    end
    commands << [RbConfig.ruby, "integration.rb"]
    commands.each { |command| system(environment, *command, chdir: working, exception: true) }
  end
end
puts "Exact core/Rack manifests, normal offline runtime dependencies and installed calls passed"
