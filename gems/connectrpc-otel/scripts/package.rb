# frozen_string_literal: true

require "bundler"
require "fileutils"
require "rbconfig"
require "rubygems/package"
require "stringio"
require "tmpdir"

require_relative "../../../scripts/release_train"

root = File.expand_path("../../..", __dir__)
version = ReleaseTrain.new(root).version
local_requirement = ReleaseTrain.new(root).requirement
output = File.join(root, "vendor/otel/pkg")
core = Gem::Package.new(File.join(output, "connectrpc-#{version}.gem"))
otel = Gem::Package.new(File.join(output, "connectrpc-otel-#{version}.gem"))
expected_core = %w[
  LICENSE README.md docs/development.md docs/design.md docs/rbs.md docs/streaming.md exe/protoc-gen-connectrpc-ruby
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
  lib/connectrpc.rb lib/connectrpc/version.rb lib/connectrpc/method.rb lib/connectrpc/metadata.rb
  lib/connectrpc/error.rb lib/connectrpc/cancellation.rb
  lib/connectrpc/deadline.rb lib/connectrpc/compression.rb lib/connectrpc/message_io.rb
  lib/connectrpc/get_query.rb
  lib/connectrpc/protocol.rb lib/connectrpc/client.rb lib/connectrpc/server.rb lib/connectrpc/instrumentation.rb
  lib/connectrpc/bindings/google_protobuf.rb lib/connectrpc/transports/net_http.rb
  lib/connectrpc/generator.rb lib/connectrpc/generator/names.rb lib/connectrpc/generator/schema.rb
  lib/connectrpc/generator/renderer.rb lib/connectrpc/generator/runtime_names.rb
].sort
abort "Core manifest changed" unless core.contents.sort == expected_core && core.spec.runtime_dependencies.empty?
expected_otel = %w[LICENSE README.md lib/connectrpc/otel.rb sig/connectrpc-otel.rbs sig/manifest.yaml
                   lib/connectrpc/otel/subscriber.rb].sort
abort "OTel manifest changed" unless otel.contents.sort == expected_otel
expected_dependencies = { "connectrpc" => local_requirement, "opentelemetry-api" => ">= 1.11.1, < 2" }
abort "OTel runtime closure changed" unless otel.spec.runtime_dependencies.to_h do |dep|
  [dep.name, dep.requirement.to_s]
end == expected_dependencies
license = File.binread(File.join(root, "LICENSE"))
mode_failure = false
[core, otel].each do |archive|
  abort "Publication guard missing" unless archive.spec.metadata["allowed_push_host"] == "https://gems.invalid"
  abort "MIT metadata missing" unless archive.spec.licenses == ["MIT"]
  Gem::Package::TarReader.new(File.open(archive.gem.path, "rb")) do |tar|
    tar.each do |entry|
      next unless entry.full_name == "data.tar.gz"

      Gem::Package::TarReader.new(Zlib::GzipReader.new(StringIO.new(entry.read))) do |data|
        data.each do |file|
          abort "Archived license differs" if file.full_name == "LICENSE" && file.read != license
          next unless file.full_name == "exe/protoc-gen-connectrpc-ruby"

          mode_failure = file.header.mode & 0o777 != 0o755
          warn "Core executable mode is #{(file.header.mode & 0o777).to_s(8)}, expected 755" if mode_failure
        end
      end
    end
  end
end
abort "Package license differs" unless File.binread(File.join(File.dirname(__dir__), "LICENSE")) == license
registry_specs = Bundler.load.specs.reject { |spec| spec.source.is_a?(Bundler::Source::Path) }
development_archives = %w[opentelemetry-sdk google-protobuf].map do |name|
  spec = registry_specs.find { |entry| entry.name == name }
  abort "Missing cached #{name}" unless spec && File.file?(spec.cache_file)

  File.basename(spec.cache_file)
end
artifacts = registry_specs.filter_map do |spec|
  spec.cache_file if !spec.source.is_a?(Bundler::Source::Path) && File.file?(spec.cache_file)
end
Bundler.with_unbundled_env do
  Dir.mktmpdir("connectrpc-otel-consumer-") do |temporary|
    working = File.join(temporary, "cwd")
    gem_home = File.join(temporary, "gems")
    FileUtils.mkdir_p(working)
    local_archives = ReleaseTrain::NAMES.map { |name| File.join(output, "#{name}-#{version}.gem") }
    FileUtils.cp([*local_archives, *artifacts], working)
    FileUtils.cp(File.join(__dir__, "integration.rb"), working)
    FileUtils.cp(Dir.glob(File.join(root, "spec/fixtures/generation/echo/*.rb")), working)
    environment = { "GEM_HOME" => gem_home, "GEM_PATH" => gem_home, "RUBYOPT" => nil, "RUBYLIB" => nil,
                    "RUBYGEMS_GEMDEPS" => nil }
    ENV.each_key { |key| environment[key] = nil if key.start_with?("BUNDLE_", "BUNDLER_") }
    run = ->(*args) { system(environment, RbConfig.ruby, *args, chdir: working, exception: true) }
    run.call("-S", "gem", "install", "--norc", "connectrpc-otel-#{version}.gem", "--local", "--no-document")
    run.call("-e", <<~'CODE')
      require "connectrpc/otel"
      abort "SDK/exporter auto-required" if defined?(OpenTelemetry::SDK)
      abort "Other optional runtime loaded" if defined?(Rack) || defined?(Async) || defined?(Google::Protobuf)
      expected = %w[connectrpc connectrpc-otel opentelemetry-api logger]
      extras = Gem::Specification.reject(&:default_gem?).map(&:name) - expected
      abort "Unexpected installed closure: #{extras}" unless extras.empty?
      %w[connectrpc connectrpc-otel opentelemetry-api].each do |name|
        abort "Source leak" unless Gem.loaded_specs.fetch(name).full_gem_path.start_with?(ENV.fetch("GEM_HOME") + "/")
      end
      puts "API-only require from normal offline dependencies outside checkout passed"
    CODE
    ["connectrpc-testing-#{version}.gem", *development_archives].each do |name|
      run.call("-S", "gem", "install", "--norc", name, "--local", "--no-document")
    end
    run.call("-I", working, "integration.rb")
    puts "Exact manifests, API-only runtime closure, MIT, guards and fresh installed generated SDK consumer passed"
  end
end
abort "Core package executable mode must be 0755" if mode_failure
