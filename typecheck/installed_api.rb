# frozen_string_literal: true

require_relative "api_check"
require_relative "archive"

module InstalledAPI # rubocop:disable Metrics/ModuleLength
  def self.archives(root:, output:)
    APICheck::PACKAGES.map do |name|
      directory = File.join(root, "gems/connectrpc-#{name}")
      archive = File.join(output, "connectrpc-#{name}-0.1.0.gem")
      Dir.chdir(directory) do
        specification = Gem::Specification.load("connectrpc-#{name}.gemspec")
        Gem::Package.build(specification, false, false, archive)
      end
      SignatureArchive.check(archive, root: root)
      package = Gem::Package.new(archive)
      signatures = ["sig/connectrpc-#{name}.rbs", "sig/manifest.yaml"]
      raise "Missing owned signature manifest: #{name}" unless package.contents.grep(%r{\Asig/}).sort == signatures.sort
      raise "Publication guard lost: #{name}" unless package.spec.metadata["allowed_push_host"] == "https://gems.invalid"
      raise "License metadata lost: #{name}" unless package.spec.licenses == ["MIT"]

      [archive, package.spec]
    end
  end

  def self.runtime_artifacts(root:, specifications:)
    locks = APICheck::PACKAGES.map do |name|
      if name == "rails"
        File.join(root, "gems/connectrpc-rails/gemfiles/rails_8.1.gemfile.lock")
      else
        File.join(root, "gems/connectrpc-#{name}/Gemfile.lock")
      end
    end
    versions = { "bundler" => Gem::Version.new("4.0.23") }
    locks.each do |lock|
      Bundler::LockfileParser.new(File.read(lock)).specs.each do |spec|
        previous = versions[spec.name]
        raise "Conflicting locked version for #{spec.name}" if previous && previous != spec.version

        versions[spec.name] = spec.version
      end
    end
    caches = Dir.glob(File.join(root, "vendor/{bundle,*/bundle}/ruby/*/cache/*.gem"))
    bundler_archive = Gem.loaded_specs.fetch("bundler").cache_file
    caches << bundler_archive if File.file?(bundler_archive)
    index = caches.group_by { |file| Gem::Package.new(file).spec.name }
    selected = {}
    pending = specifications.flat_map(&:runtime_dependencies)
    local_names = ["connectrpc", *APICheck::PACKAGES.map { |name| "connectrpc-#{name}" }]
    until pending.empty?
      dependency = pending.shift
      next if local_names.include?(dependency.name) || selected.key?(dependency.name)

      candidate = index.fetch(dependency.name, []).find do |file|
        spec = Gem::Package.new(file).spec
        spec.version == versions.fetch(spec.name) && dependency.matches_spec?(spec) && Gem::Platform.match_spec?(spec)
      end
      raise "Missing frozen runtime archive: #{dependency}" unless candidate

      selected[dependency.name] = candidate
      pending.concat(Gem::Package.new(candidate).spec.runtime_dependencies)
    end
    raise "SDK entered runtime closure" if selected.key?("opentelemetry-sdk")

    selected.values
  end

  def self.check(root:, working:, output:)
    packages = archives(root: root, output: output)
    artifacts = runtime_artifacts(root: root, specifications: packages.map(&:last))
    FileUtils.cp(artifacts, working)
    RBSHarness.capture!([RbConfig.ruby, "-S", "gem", "install", "--norc", "--local", "--no-document",
                         *packages.map(&:first)], working: working)
    %w[api falcon api_check.rb installed_runtime.rb installed_streaming.rb installed_web.rb].each do |name|
      FileUtils.cp_r(File.join(root, "typecheck", name), working)
    end
    # Each library must resolve independently through its installed manifest.
    libraries = ["connectrpc", *APICheck::PACKAGES.map { |name| "connectrpc-#{name}" }]
    libraries.each do |name|
      RBSHarness.capture!([RbConfig.ruby, "-S", "rbs", "-r", name, "validate"], working: working)
    end
    # The parent process still has the tooling bundle's RubyGems cache. Resolve
    # the fresh GEM_HOME in an unbundled child, as the actual consumer does.
    discovery = APICheck::BRIDGES.map do |name, directory|
      "puts File.join(Gem::Specification.find_by_name(#{"connectrpc-#{name}".dump}).full_gem_path, #{directory.dump})"
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
      abort 'Opt-in framework declarations loaded automatically' if names.any? do |name|
        name.start_with?('::ActionController::', '::Async::Service::')
      end
      puts 'Framework compatibility declarations remain opt-in'
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
    puts "Seven installed signature libraries discovered independently and in two orders; API-only OTel closure"
  end
end
