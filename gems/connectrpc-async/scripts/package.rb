# frozen_string_literal: true

require "bundler"
require "fileutils"
require "rubygems/package"
require "rbconfig"
require "tmpdir"
root = File.expand_path("../../..", __dir__)
archives = %w[connectrpc connectrpc-async].map { |name| File.join(root, "vendor/async/pkg/#{name}-0.1.0.gem") }
core, backend = archives.map { |archive| Gem::Package.new(archive) }
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
  lib/connectrpc/get_query.rb lib/connectrpc/protocol.rb
  lib/connectrpc/client.rb lib/connectrpc/server.rb lib/connectrpc/instrumentation.rb
  lib/connectrpc/bindings/google_protobuf.rb lib/connectrpc/transports/net_http.rb
].sort
abort "Core package changed" unless core.contents.sort == core_expected && core.spec.runtime_dependencies.empty?
expected = %w[LICENSE README.md lib/connectrpc/async.rb sig/connectrpc-async.rbs sig/manifest.yaml
              lib/connectrpc/async/internal.rb lib/connectrpc/async/ingress.rb lib/connectrpc/async/tls.rb
              lib/connectrpc/async/connection.rb lib/connectrpc/async/application.rb
              lib/connectrpc/async/stream_exchange.rb
              lib/connectrpc/async/tls_configuration.rb lib/connectrpc/async/server_session.rb
              lib/connectrpc/async/host.rb lib/connectrpc/async/transport.rb].sort
abort "Backend manifest changed" unless backend.contents.sort == expected
expected_dependencies = {
  "connectrpc" => "~> 0.1.0", "async-http" => "~> 0.105.0", "async" => "~> 2.46.0", "async-pool" => "~> 0.12.0",
  "protocol-http2" => "~> 0.29.2", "protocol-http" => "~> 0.72.1", "io-endpoint" => "~> 0.19.0",
  "io-stream" => "~> 0.14.0", "openssl" => "~> 4.0.2"
}
actual = backend.spec.runtime_dependencies.to_h { |dependency| [dependency.name, dependency.requirement.to_s] }
abort "Runtime dependencies changed" unless actual == expected_dependencies
[core, backend].each do |package|
  unless package.spec.metadata["allowed_push_host"] == "https://gems.invalid" && package.spec.license == "MIT"
    abort "Publication guard missing"
  end
end
unless File.binread(File.join(root, "gems/connectrpc-async/LICENSE")) == File.binread(File.join(root, "LICENSE"))
  abort "Package-local license differs from root LICENSE"
end
Dir.mktmpdir("connectrpc-license-") do |temporary|
  [core, backend].each do |package|
    destination = File.join(temporary, package.spec.name)
    package.extract_files(destination, "LICENSE")
    unless File.binread(File.join(destination, "LICENSE")) == File.binread(File.join(root, "LICENSE"))
      abort "Packaged license differs from root LICENSE: #{package.spec.name}"
    end
  end
end
specifications = Bundler.load.specs.reject do |spec|
  %w[connectrpc connectrpc-async bundler].include?(spec.name)
end
artifacts = specifications.map do |spec|
  abort "Missing offline archive #{spec.full_name}" unless File.file?(spec.cache_file)

  spec.cache_file
end
Bundler.with_unbundled_env do
  Dir.mktmpdir("connectrpc-async-package-", ENV.fetch("CONNECTRPC_ASYNC_TMPDIR", nil)) do |temporary|
    working = File.join(temporary, "outside-checkout")
    FileUtils.mkdir_p(working)
    FileUtils.cp(artifacts + archives, working)
    FileUtils.cp(File.join(__dir__, "integration.rb"), working)
    FileUtils.cp(File.join(__dir__, "../spec/support/helpers.rb"), File.join(working, "helpers.rb"))
    FileUtils.cp_r(File.join(root, "spec/fixtures/generation/echo"), File.join(working, "generated"))
    environment = { "GEM_HOME" => File.join(temporary, "home"), "GEM_PATH" => File.join(temporary, "home"),
                    "RUBYOPT" => nil, "RUBYLIB" => nil, "RUBYGEMS_GEMDEPS" => nil,
                    "CONNECTRPC_ASYNC_INSTALLED" => "1" }
    ENV.each_key { |name| environment[name] = nil if name.start_with?("BUNDLE_", "BUNDLER_") }
    commands = [
      [RbConfig.ruby, "-S", "gem", "install", "--norc", archives.first, "--local", "--no-document"],
      [RbConfig.ruby, "-e",
       'require "connectrpc"; abort if defined?(ConnectRPC::Async) || defined?(Rack) || defined?(Rails)'],
      [RbConfig.ruby, "-S", "gem", "install", "--norc", File.join(working, "json-3.1.0.gem"),
       "--local", "--no-document"],
      [RbConfig.ruby, "-S", "gem", "install", "--norc", archives.last, "--local", "--no-document"],
      [RbConfig.ruby, "-S", "gem", "install", "--norc", "google-protobuf", "--version", "4.36.2", "--local",
       "--no-document"],
      [RbConfig.ruby, "-S", "gem", "install", "--norc", "webrick", "--version", "1.9.2", "--local", "--no-document"],
      [RbConfig.ruby, "integration.rb"]
    ]
    commands.each { |command| system(environment, *command, chdir: working, exception: true) }
  end
end
puts "Exact core/async manifests, normal offline dependencies, isolated h2/mTLS and publication guards passed"
