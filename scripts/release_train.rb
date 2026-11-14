# frozen_string_literal: true

require "json"
require "rubygems"

# Source preparation only. This helper never loads ConnectRPC or contacts a registry.
class ReleaseTrain # rubocop:disable Metrics/ClassLength
  NAMES = %w[connectrpc connectrpc-rack connectrpc-async connectrpc-testing connectrpc-otel
             connectrpc-rails connectrpc-falcon].freeze
  VERSION_FILES = NAMES.to_h do |name|
    file = if name == "connectrpc"
             "lib/connectrpc/version.rb"
           else
             "gems/#{name}/lib/connectrpc/#{name.delete_prefix('connectrpc-')}/version.rb"
           end
    [name, file]
  end.freeze
  LOCKS = ["Gemfile.lock", *%w[rack async falcon testing otel].map do |name|
    "gems/connectrpc-#{name}/Gemfile.lock"
  end, *%w[8.0 8.1].map { |lane| "gems/connectrpc-rails/gemfiles/rails_#{lane}.gemfile.lock" }].freeze
  GUIDES = %w[README.md docs/design.md gems/connectrpc-rack/README.md gems/connectrpc-rails/README.md
              gems/connectrpc-testing/README.md gems/connectrpc-otel/README.md].freeze
  LOCK_PACKAGES = {
    "Gemfile.lock" => %w[connectrpc connectrpc-rack],
    "gems/connectrpc-rack/Gemfile.lock" => %w[connectrpc connectrpc-rack],
    "gems/connectrpc-async/Gemfile.lock" => %w[connectrpc connectrpc-async],
    "gems/connectrpc-falcon/Gemfile.lock" => %w[connectrpc connectrpc-async connectrpc-falcon],
    "gems/connectrpc-testing/Gemfile.lock" => %w[connectrpc connectrpc-testing],
    "gems/connectrpc-otel/Gemfile.lock" => NAMES,
    "gems/connectrpc-rails/gemfiles/rails_8.0.gemfile.lock" => %w[connectrpc connectrpc-rack connectrpc-rails],
    "gems/connectrpc-rails/gemfiles/rails_8.1.gemfile.lock" => %w[connectrpc connectrpc-rack connectrpc-rails]
  }.freeze
  attr_reader :root

  def initialize(root = File.expand_path("..", __dir__))
    @root = File.expand_path(root)
  end

  def read(relative)
    File.read(File.join(root, relative))
  end

  def version
    value = read("lib/connectrpc/version.rb")[/VERSION = "([^"]+)"/, 1]
    raise "Expected a numeric 0.x release version, got #{value.inspect}" unless value&.match?(/\A0\.\d+\.\d+\z/)

    value
  end

  def requirement
    "~> #{version.split('.').first(2).join('.')}.0"
  end

  def directory(name)
    raise "Unknown release package #{name}" unless NAMES.include?(name)

    name == "connectrpc" ? root : File.join(root, "gems", name)
  end

  def specifications
    NAMES.map do |name|
      Dir.chdir(directory(name)) do
        spec = Gem::Specification.load(File.join(directory(name), "#{name}.gemspec"))
        raise "Cannot load #{name}.gemspec" unless spec

        spec
      end
    end
  end

  def check_versions! # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity
    specifications.each do |spec|
      raise "Version mismatch: #{spec.name} #{spec.version}, expected #{version}" unless spec.version.to_s == version
      raise "Push host changed: #{spec.name}" unless spec.metadata["allowed_push_host"] == "https://rubygems.org"
      raise "MFA requirement lost: #{spec.name}" unless spec.metadata["rubygems_mfa_required"] == "true"

      spec.runtime_dependencies.select { |dep| NAMES.include?(dep.name) }.each do |dependency|
        next if dependency.requirement.to_s == requirement

        raise "Local dependency policy changed: #{spec.name} -> #{dependency} (expected #{requirement})"
      end
    end
  end

  def prepared_lock(content) # rubocop:disable Metrics/MethodLength
    names = Regexp.union(NAMES)
    # Only PATH records and digest-free local CHECKSUMS may change. Keep the
    # registry resolution, third-party checksums, platforms and tool pins intact.
    content.split(/(?=^[A-Z][A-Z ]*\n)/).map do |section|
      if section.start_with?("PATH\n")
        section.gsub(/^( {4}#{names}) \([^\n)]+\)$/, "\\1 (#{version})")
               .gsub(/^( {6}#{names}) \(~> [^\n)]+\)$/, "\\1 (#{requirement})")
      elsif section.start_with?("CHECKSUMS\n")
        section.gsub(/^( {2}#{names}) \([^\n)]+\)$/, "\\1 (#{version})")
      else
        section
      end
    end.join
  end

  def prepared_files
    files = LOCKS.to_h { |file| [file, prepared_lock(read(file))] }
    GUIDES.each do |file|
      files[file] = read(file).gsub(/(connectrpc(?:-[a-z]+)?["`, ]+)~> 0\.\d+\.\d+/, "\\1#{requirement}")
    end
    files["docs/development.md"] = read("docs/development.md").gsub(%r{pkg/connectrpc-0\.\d+\.\d+\.gem},
                                                                    "pkg/connectrpc-#{version}.gem")
    files
  end

  def check_configuration! # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
    config = JSON.parse(read(".github/release-please-config.json")).fetch("packages")
    raise "Expected one root release unit" unless config.keys == ["."]

    package = config.fetch(".")
    expected = {
      "release-type" => "ruby", "component" => "connectrpc", "version-file" => "lib/connectrpc/version.rb",
      "initial-version" => "0.1.0", "include-component-in-tag" => false, "include-v-in-tag" => true,
      "bump-minor-pre-major" => true, "bump-patch-for-minor-pre-major" => true, "draft-pull-request" => true,
      # Publication adopts the draft; the forced tag keeps release history discoverable.
      "draft" => true, "force-tag-creation" => true
    }
    raise "Release configuration changed" unless expected.all? { |key, value| package[key] == value }
    raise "Permanent release-as override is forbidden" if package.key?("release-as")

    owners = VERSION_FILES.values.drop(1)
    raise "Incomplete version updater inventory" unless package.fetch("extra-files").sort == owners.sort

    owners.each do |file|
      raise "Missing version annotation: #{file}" unless read(file).include?("x-release-please-version")
    end
    manifest = JSON.parse(read(".github/.release-please-manifest.json"))
    return if manifest == { "." => version }
    return if manifest.empty? && version == package.fetch("initial-version")

    raise "Release manifest does not match candidate #{version}"
  end

  def check! # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
    check_versions!
    check_configuration!
    LOCK_PACKAGES.each do |file, names|
      content = read(file)
      specifications = content.scan(/^ {4}(connectrpc(?:-[a-z]+)?) \([^\n)]+\)$/).flatten
      checksums = content.scan(/^ {2}(connectrpc(?:-[a-z]+)?) \([^\n)]+\)$/).flatten
      unless specifications.sort == names.sort && checksums.sort == names.sort
        raise "Local path/checksum inventory changed: #{file}"
      end
    end
    prepared_files.each do |file, expected|
      raise "Stale release preparation: #{file}; run mise run release:prepare" unless read(file) == expected
    end
    self
  end

  def prepare!
    check_versions!
    prepared_files.each do |file, expected|
      File.write(File.join(root, file), expected) unless read(file) == expected
    end
    check!
  end
end
