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
abort "Unexpected package files: #{package.contents.inspect}" unless package.contents.sort == expected
abort "Runtime dependencies leaked into package" unless package.spec.runtime_dependencies.empty?
abort "MIT license metadata missing" unless package.spec.license == "MIT"
abort "Publication guard missing" unless package.spec.metadata["allowed_push_host"] == "https://gems.invalid"

abort "Executable metadata missing" unless package.spec.bindir == "exe" &&
                                           package.spec.executables == ["protoc-gen-connectrpc-ruby"]
Gem::Package::TarReader.new(File.open(archive, "rb")) do |tar|
  tar.each do |entry|
    next unless entry.full_name == "data.tar.gz"

    Gem::Package::TarReader.new(Zlib::GzipReader.new(StringIO.new(entry.read))) do |data|
      data.each do |file|
        if file.full_name == "LICENSE" && file.read != File.binread(File.join(root, "LICENSE"))
          abort "Packaged license differs from root LICENSE"
        end
        next unless file.full_name == "exe/protoc-gen-connectrpc-ruby"

        abort "Plugin is not executable in package" unless file.header.mode & 0o777 == 0o755
      end
    end
  end
end

rack_directory = File.join(root, "gems/connectrpc-rack")
rack_output = File.join(root, "vendor/package/rack")
FileUtils.mkdir_p(rack_output)
rack_archive = Dir.chdir(rack_directory) do
  spec = Gem::Specification.load("connectrpc-rack.gemspec")
  output = File.join(rack_output, spec.file_name)
  Gem::Package.build(spec, false, false, output)
  output
end
rack_package = Gem::Package.new(rack_archive)
abort "Unexpected Rack manifest" unless rack_package.contents.sort == %w[LICENSE README.md lib/connectrpc/rack.rb
                                                                         sig/connectrpc-rack.rbs sig/manifest.yaml]
rack_dependencies = rack_package.spec.runtime_dependencies.to_h { |entry| [entry.name, entry.requirement.to_s] }
unless rack_dependencies == { "connectrpc" => "~> 0.1.0", "rack" => ">= 3.2.7, < 4" } &&
       rack_package.spec.required_ruby_version.to_s == ">= 3.4" &&
       rack_package.spec.metadata["allowed_push_host"] == "https://gems.invalid" &&
       rack_package.spec.license == "MIT"
  abort "Unexpected Rack dependencies/metadata"
end

unless File.binread(File.join(rack_directory, "LICENSE")) == File.binread(File.join(root, "LICENSE"))
  abort "Package-local Rack license differs from root LICENSE"
end
Dir.mktmpdir("connectrpc-license-") do |temporary|
  rack_package.extract_files(temporary, "LICENSE")
  unless File.binread(File.join(temporary, "LICENSE")) == File.binread(File.join(root, "LICENSE"))
    abort "Packaged Rack license differs from root LICENSE"
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
    FileUtils.cp(optional_artifacts + [archive, rack_archive], working)
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
      [RbConfig.ruby, "-S", "gem", "install", "--norc", File.join(working, File.basename(archive)), "--local",
       "--no-document"],
      [RbConfig.ruby, "-e", <<~'RUBY']
        require "connectrpc"
        abort "Wrong version" unless ConnectRPC::VERSION == "0.1.0"
        installed = Gem.loaded_specs.fetch("connectrpc")
        abort "Loaded outside isolated GEM_HOME" unless installed.full_gem_path.start_with?("#{ENV.fetch('GEM_HOME')}/")
        abort "Unexpected runtime dependencies" unless installed.runtime_dependencies.empty?
        abort "Core loaded optional integration" if defined?(Google::Protobuf) || defined?(Rack)
        ["connectrpc/bindings/google_protobuf", "connectrpc/generator"].each do |integration|
          begin
            require integration
            abort "Missing optional dependency unexpectedly loaded"
          rescue LoadError => e
            abort "Unhelpful optional require error" unless e.message.include?("requires")
          end
        end
        abort "Core ships middleware" if File.exist?(File.join(installed.full_gem_path, "lib/connectrpc/rack.rb"))
        begin
          require "connectrpc/rack"
          abort "Core unexpectedly supplies middleware"
        rescue LoadError => e
          abort "Wrong missing middleware path" unless e.path == "connectrpc/rack"
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
      [RbConfig.ruby, "-S", "gem", "install", "--norc", File.join(working, File.basename(artifact)), "--local",
       "--no-document"]
    end
    commands << [RbConfig.ruby, "-S", "gem", "install", "--norc",
                 File.join(working, File.basename(rack_archive)), "--local", "--no-document"]
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
