# frozen_string_literal: true

require "fileutils"
require "rubygems/package"
require "tmpdir"

module PackageBuild
  def self.build(directory:, output:)
    directory = File.expand_path(directory)
    output = File.expand_path(output)
    # Resolve the manifest and version metadata in the owning source directory.
    specification = Dir.chdir(directory) { Gem::Specification.load(Dir.glob("*.gemspec").fetch(0)) }
    archive = File.join(output, specification.file_name)
    FileUtils.mkdir_p(output)
    Dir.mktmpdir("connectrpc-build-") do |staging|
      stage(specification, directory, staging)
      Dir.chdir(staging) { Gem::Package.build(specification, false, false, archive) }
    end
    archive
  end

  def self.stage(specification, directory, staging)
    executables = specification.executables.map { |name| File.join(specification.bindir, name) }
    specification.files.each do |relative|
      source = File.join(directory, relative)
      raise "Package input must be a regular file: #{source}" unless File.lstat(source).file?

      destination = File.join(staging, relative)
      FileUtils.mkdir_p(File.dirname(destination))
      FileUtils.install(source, destination, mode: executables.include?(relative) ? 0o755 : 0o644)
    end
  end
  private_class_method :stage
end
