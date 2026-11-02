# frozen_string_literal: true

require "bundler"
require "fileutils"
require "rbconfig"
require "rubygems/package"
require "tmpdir"

root = File.expand_path("..", __dir__)
specification = Gem::Specification.load(File.join(root, "connectrpc.gemspec"))
archive = File.join(root, "pkg", specification.file_name)
package = Gem::Package.new(archive)
expected = %w[
  README.md docs/development.md docs/design.md lib/connectrpc.rb lib/connectrpc/version.rb
  lib/connectrpc/method.rb lib/connectrpc/metadata.rb lib/connectrpc/error.rb lib/connectrpc/deadline.rb
  lib/connectrpc/message_io.rb lib/connectrpc/protocol.rb lib/connectrpc/client.rb
  lib/connectrpc/server.rb lib/connectrpc/rack.rb
  lib/connectrpc/bindings/google_protobuf.rb lib/connectrpc/transports/net_http.rb
].sort
abort "Unexpected package files: #{package.contents.inspect}" unless package.contents.sort == expected
abort "Runtime dependencies leaked into package" unless package.spec.runtime_dependencies.empty?
abort "Publication guard missing" unless package.spec.metadata["allowed_push_host"] == "https://gems.invalid"

# Resolve cached artifacts while Bundler still owns the exact development lock.
optional_artifacts = %w[rake bigdecimal google-protobuf rack webrick rackup].map do |name|
  specification = Bundler.load.specs.find { |entry| entry.name == name }
  artifact = specification&.cache_file
  abort "Missing cached offline artifact: #{name}" unless artifact && File.file?(artifact)

  artifact
end

Bundler.with_unbundled_env do
  Dir.mktmpdir("connectrpc-package-") do |temporary|
    gem_home = File.join(temporary, "gems")
    working = File.join(temporary, "cwd")
    FileUtils.mkdir_p(working)
    FileUtils.cp(File.join(root, "examples/localhost.rb"), File.join(working, "localhost.rb"))
    FileUtils.cp(File.join(root, "scripts/installed_integration.rb"), File.join(working, "integration.rb"))
    environment = {
      "GEM_HOME" => gem_home, "GEM_PATH" => gem_home,
      "RUBYOPT" => nil, "RUBYLIB" => nil, "RUBYGEMS_GEMDEPS" => nil
    }
    ENV.each_key do |name|
      environment[name] = nil if name.start_with?("BUNDLE_", "BUNDLER_")
    end
    commands = [
      [RbConfig.ruby, "-S", "gem", "install", "--norc", archive, "--local", "--no-document", "--ignore-dependencies"],
      [RbConfig.ruby, "-e", <<~'RUBY']
        require "connectrpc"
        abort "Wrong version" unless ConnectRPC::VERSION == "0.1.0"
        installed = Gem.loaded_specs.fetch("connectrpc")
        abort "Loaded outside isolated GEM_HOME" unless installed.full_gem_path.start_with?("#{ENV.fetch('GEM_HOME')}/")
        abort "Unexpected runtime dependencies" unless installed.runtime_dependencies.empty?
        abort "Core loaded optional integration" if defined?(Google::Protobuf) || defined?(Rack)
        ["connectrpc/bindings/google_protobuf", "connectrpc/rack"].each do |integration|
          begin
            require integration
            abort "Missing optional dependency unexpectedly loaded"
          rescue LoadError => e
            abort "Unhelpful optional require error" unless e.message.include?("requires")
          end
        end
        puts "Isolated require: connectrpc #{ConnectRPC::VERSION} (Ruby #{RUBY_VERSION})"
      RUBY
    ]
    optional_artifacts.each do |artifact|
      commands << [RbConfig.ruby, "-S", "gem", "install", "--norc", artifact, "--local", "--no-document",
                   "--ignore-dependencies"]
    end
    commands << [RbConfig.ruby, "integration.rb"]
    commands.each do |command|
      abort "Isolated package check failed" unless system(environment, *command, chdir: working)
    end
  end
end
puts "Verified package files: #{expected.join(', ')}"
