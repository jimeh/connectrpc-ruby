# frozen_string_literal: true

require "bundler"
require "fileutils"
require "English"
require "open3"
require "rbconfig"
require "rubygems/package"
require "shellwords"
require "stringio"
require "tmpdir"

def run_package_command(environment, command, working)
  result = system(environment, *command, chdir: working)
  return if result

  status = $CHILD_STATUS
  reason = if result.nil?
             "executable missing or could not start; check task-local tools and PATH"
           else
             status.to_s
           end
  abort "Isolated package command failed (#{reason}): #{Shellwords.join(command)}"
end

root = File.expand_path("..", __dir__)
specification = Gem::Specification.load(File.join(root, "connectrpc.gemspec"))
archive = File.join(root, "pkg", specification.file_name)
package = Gem::Package.new(archive)
expected = %w[
  README.md docs/development.md docs/design.md lib/connectrpc.rb lib/connectrpc/version.rb
  exe/protoc-gen-connectrpc-ruby lib/connectrpc/generator.rb lib/connectrpc/generator/names.rb
  lib/connectrpc/generator/schema.rb lib/connectrpc/generator/renderer.rb lib/connectrpc/generator/runtime_names.rb
  lib/connectrpc/method.rb lib/connectrpc/metadata.rb lib/connectrpc/error.rb lib/connectrpc/deadline.rb
  lib/connectrpc/compression.rb lib/connectrpc/message_io.rb lib/connectrpc/protocol.rb lib/connectrpc/client.rb
  lib/connectrpc/server.rb lib/connectrpc/rack.rb
  lib/connectrpc/bindings/google_protobuf.rb lib/connectrpc/transports/net_http.rb
].sort
abort "Unexpected package files: #{package.contents.inspect}" unless package.contents.sort == expected
abort "Runtime dependencies leaked into package" unless package.spec.runtime_dependencies.empty?
abort "Publication guard missing" unless package.spec.metadata["allowed_push_host"] == "https://gems.invalid"

abort "Executable metadata missing" unless package.spec.bindir == "exe" &&
                                           package.spec.executables == ["protoc-gen-connectrpc-ruby"]
Gem::Package::TarReader.new(File.open(archive, "rb")) do |tar|
  tar.each do |entry|
    next unless entry.full_name == "data.tar.gz"

    Gem::Package::TarReader.new(Zlib::GzipReader.new(StringIO.new(entry.read))) do |data|
      data.each do |file|
        next unless file.full_name == "exe/protoc-gen-connectrpc-ruby"

        abort "Plugin is not executable in package" unless file.header.mode & 0o777 == 0o755
      end
    end
  end
end

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
    FileUtils.cp(File.join(root, "scripts/generated_integration.rb"), File.join(working, "generated_integration.rb"))
    FileUtils.cp_r(File.join(root, "spec/fixtures/generation/proto"), File.join(working, "proto"))
    environment = {
      "GEM_HOME" => gem_home, "GEM_PATH" => gem_home,
      "RUBYOPT" => nil, "RUBYLIB" => nil, "RUBYGEMS_GEMDEPS" => nil
    }
    ENV.each_key do |name|
      environment[name] = nil if name.start_with?("BUNDLE_", "BUNDLER_")
    end
    begin
      version, error, status = Open3.capture3(environment, "protoc", "--version", chdir: working)
    rescue Errno::ENOENT => e
      abort "Package command protoc --version could not start: #{e.message}; " \
            "run mise run check:package for task-local protoc 36.2"
    end
    unless status.success? && version.strip == "libprotoc 36.2"
      abort "Package command protoc --version failed (#{status}): " \
            "expected libprotoc 36.2, got #{version.inspect}; #{error}"
    end
    puts "Package compiler: #{version.strip}"
    commands = [
      [RbConfig.ruby, "-S", "gem", "install", "--norc", archive, "--local", "--no-document", "--ignore-dependencies"],
      [RbConfig.ruby, "-e", <<~'RUBY']
        require "connectrpc"
        abort "Wrong version" unless ConnectRPC::VERSION == "0.1.0"
        installed = Gem.loaded_specs.fetch("connectrpc")
        abort "Loaded outside isolated GEM_HOME" unless installed.full_gem_path.start_with?("#{ENV.fetch('GEM_HOME')}/")
        abort "Unexpected runtime dependencies" unless installed.runtime_dependencies.empty?
        abort "Core loaded optional integration" if defined?(Google::Protobuf) || defined?(Rack)
        ["connectrpc/bindings/google_protobuf", "connectrpc/rack", "connectrpc/generator"].each do |integration|
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
    installed_plugin = File.join(gem_home, "bin/protoc-gen-connectrpc-ruby")
    commands.each do |command|
      run_package_command(environment, command, working)
    end
    output, error, status = Open3.capture3(environment, installed_plugin, stdin_data: "", chdir: working)
    unless !status.success? && output.empty? && error.include?("generator requires google-protobuf >= 4.36.2, < 5")
      abort "Installed plugin missing-Google failure is not helpful: #{error}"
    end
    puts "Installed executable rejects missing Google dependency without stdout"
    commands = optional_artifacts.map do |artifact|
      [RbConfig.ruby, "-S", "gem", "install", "--norc", artifact, "--local", "--no-document",
       "--ignore-dependencies"]
    end
    commands << [RbConfig.ruby, "integration.rb"]
    generated = File.join(working, "generated")
    FileUtils.mkdir_p(generated)
    inputs = Dir.glob("**/*.proto", base: File.join(working, "proto")).sort
    commands << ["protoc", "-I", File.join(working, "proto"), "--ruby_out=#{generated}",
                 "--connectrpc-ruby_out=#{generated}", "--plugin=protoc-gen-connectrpc-ruby=#{installed_plugin}",
                 *inputs]
    commands << [RbConfig.ruby, "-I", generated, "generated_integration.rb"]
    puts "Installed plugin: #{installed_plugin} (compiler #{version.strip})"
    commands.each do |command|
      run_package_command(environment, command, working)
    end
  end
end
puts "Verified package files: #{expected.join(', ')}"
