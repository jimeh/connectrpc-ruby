# frozen_string_literal: true

require "rubygems/package"
require "tmpdir"
require_relative "api_check"
require_relative "archive"
require_relative "collection"
require_relative "../scripts/package_build"
require_relative "../gems/connectrpc-hyper/scripts/bindgen"

module InstalledAPI
  def self.archives(root:, output:)
    APICheck::PACKAGES.map do |name|
      directory = File.join(root, "gems/connectrpc-#{name}")
      archive = PackageBuild.build(directory: directory, output: output)
      SignatureArchive.check(archive, root: root)
      package = Gem::Package.new(archive)
      signatures = ["sig/connectrpc-#{name}.rbs", "sig/manifest.yaml"]
      raise "Missing owned signature manifest: #{name}" unless package.contents.grep(%r{\Asig/}).sort == signatures.sort
      raise "Push host changed: #{name}" unless package.spec.metadata["allowed_push_host"] == "https://rubygems.org"

      licenses = SignatureArchive.licenses(package.spec.name)
      raise "License metadata lost: #{name}" unless package.spec.licenses == licenses

      [archive, package.spec]
    end
  end

  # Every archive the scoped bundles cached for the running Ruby ABI. RubyGems
  # local resolution then selects a closure that satisfies every accumulated
  # requirement, platform and required_ruby_version, as a consumer install does.
  def self.cached_archives(root:)
    abi = RbConfig::CONFIG.fetch("ruby_version")
    caches = Dir.glob(File.join(root, "vendor/{bundle,*/bundle}/ruby", abi, "cache/*.gem"))
    bundler_archive = Gem.loaded_specs.fetch("bundler").cache_file
    caches << bundler_archive if File.file?(bundler_archive)
    raise "No cached #{abi} archives; run the scoped optional setups first" if caches.empty?

    caches.uniq { |file| File.basename(file) }
  end

  # The hyper source gem compiles at install time. Its archived crate graph is
  # fetched once, then the install builds offline with the active Rust.
  def self.native_environment(root:, packages:)
    archive, = packages.find { |_, spec| spec.name == "connectrpc-hyper" }
    cargo_home = File.join(root, "vendor/hyper/cargo")
    Dir.mktmpdir("connectrpc-rbs-hyper-") do |extracted|
      Gem::Package.new(archive).extract_files(extracted)
      RBSHarness.capture!(["cargo", "fetch", "--locked", "--manifest-path", File.join(extracted, "Cargo.toml")],
                          working: extracted, environment: { "CARGO_HOME" => cargo_home })
    end
    { "CARGO_HOME" => cargo_home, "CARGO_NET_OFFLINE" => "true", "CARGO_TARGET_DIR" => nil,
      "RB_SYS_CARGO_TARGET_DIR" => nil, "CONNECTRPC_HYPER_LIB_DIR" => nil, **HyperBindgen.environment }
  end

  def self.check(root:, working:, output:)
    packages = archives(root: root, output: output)
    FileUtils.cp(cached_archives(root: root), working)
    environment = native_environment(root: root, packages: packages)
    begin
      RBSHarness.capture!([RbConfig.ruby, "-S", "gem", "install", "--norc", "--local", "--no-document",
                           *packages.map(&:first)], working: working, environment: environment)
    rescue StandardError => e
      raise "Cannot resolve the installed runtime closure from cached archives: #{e.message}"
    end
    sdk = Dir.glob(File.join(ENV.fetch("GEM_HOME"), "specifications/opentelemetry-sdk-*.gemspec"))
    raise "SDK entered runtime closure" unless sdk.empty?

    %w[api falcon api_check.rb installed_runtime.rb installed_streaming.rb installed_web.rb].each do |name|
      FileUtils.cp_r(File.join(root, "typecheck", name), working)
    end
    # Each library resolves through its installed manifest beside the documented
    # connectrpc libraries its signatures reference.
    libraries = ["connectrpc", *APICheck::PACKAGES.map { |name| "connectrpc-#{name}" }]
    [["connectrpc"], *APICheck::PACKAGES.map { |name| APICheck.libraries(name) }].each do |closure|
      RBSHarness.capture!([RbConfig.ruby, "-S", "rbs", *closure.flat_map { |name| ["-r", name] }, "validate"],
                          working: working)
    end
    RBSCollection.check(working: working)
    # The parent process still has the tooling bundle's RubyGems cache. Resolve
    # the fresh GEM_HOME in an unbundled child, as the actual consumer does.
    owners = APICheck::BRIDGES.transform_keys { |name| "connectrpc-#{name}" }
                              .merge("connectrpc" => APICheck::PROTOBUF_BRIDGE)
    discovery = owners.map do |name, directory|
      "puts File.join(Gem::Specification.find_by_name(#{name.dump}).full_gem_path, #{directory.dump})"
    end.join("\n")
    bridges = RBSHarness.capture!([RbConfig.ruby, "-e", discovery], working: working).lines.map(&:chomp)
    [libraries, libraries.reverse].each do |order|
      command = [RbConfig.ruby, "-S", "rbs", *order.flat_map { |name| ["-r", name] }]
      RBSHarness.capture!([*command, "validate"], working: working)
      bridge_order = order == libraries ? bridges : bridges.reverse
      RBSHarness.capture!([*command, *bridge_order.flat_map { |directory| ["-I", directory] }, "validate"],
                          working: working)
    end
    library_loading = libraries.map { |name| "loader.add(library: #{name.dump})" }.join("\n")
    isolation = <<~CODE
      require 'rbs'
      loader = RBS::EnvironmentLoader.new
      #{library_loading}
      env = RBS::Environment.from_loader(loader).resolve_type_names
      names = env.class_decls.keys.map(&:to_s)
      abort 'Opt-in bridge declarations loaded automatically' if names.any? do |name|
        name.start_with?('::ActionController::', '::Async::Service::', '::Google::Protobuf::', '::Grpc::')
      end
      puts 'Framework, protobuf and generated health message bridges remain opt-in'
    CODE
    puts RBSHarness.capture!([RbConfig.ruby, "-e", isolation], working: working)
    runner = <<~CODE
      require_relative 'api_check'
      APICheck.check(root: Dir.pwd, working: File.join(Dir.pwd, 'installed-api'), installed: true)
    CODE
    FileUtils.mkdir_p(File.join(working, "typecheck"))
    FileUtils.cp_r(File.join(root, "typecheck/api"), File.join(working, "typecheck"))
    FileUtils.cp_r(File.join(root, "typecheck/falcon"), File.join(working, "typecheck"))
    File.write(File.join(working, "installed_api_check.rb"), runner)
    puts RBSHarness.capture!([RbConfig.ruby, "installed_api_check.rb"], working: working)
    %w[forward reverse].each do |order|
      puts RBSHarness.capture!([RbConfig.ruby, "installed_runtime.rb", order], working: working)
    end
    puts "Twelve installed signature libraries discovered with their documented closures and in two orders; " \
         "API-only OTel closure"
  end
end
