# frozen_string_literal: true

require "digest"
require "fileutils"
require "json"
require "open3"
require "stringio"
require "tmpdir"
require "zlib"
require_relative "package_build"
require_relative "release_train"
require_relative "../typecheck/archive"

class ReleaseCandidate # rubocop:disable Metrics/ClassLength
  # Platform archives replace the crate sources with one prebuilt extension per Ruby minor.
  CRATE_SOURCES = %r{\A(?:Cargo\.(?:toml|lock)|ext/)}
  EVIDENCE_KEYS = %w[file sha256 platform ruby ruby_platform].freeze

  # Flattens connectrpc-hyper archives and smoke evidence found anywhere under
  # two source directories into the flat directories release:build reads,
  # replacing their contents. Sources default to local hyper:cross and
  # install_smoke.rb outputs; `gh run download` directories of the hyper-gems
  # artifacts work too. Returns [source, destination, count] per directory.
  def self.collect!(root, sources) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
    archives, evidence = case sources
                         in [] then %w[vendor/hyper/cross vendor/hyper/evidence].map { File.join(root, it) }
                         in [String, String] then sources.map { File.expand_path(it) }
                         else raise ArgumentError, "collect takes no paths or ARCHIVES EVIDENCE, got #{sources}"
                         end
    inputs = { File.join(root, "vendor/hyper/release-artifacts") => [archives, "connectrpc-hyper-*.gem"],
               File.join(root, "vendor/hyper/release-evidence") => [evidence, "*.json"] }
    plan = inputs.map do |destination, (source, pattern)|
      raise ArgumentError, "Collection source is missing: #{source}" unless Dir.exist?(source)

      overlap = inputs.keys.find { |output| overlapping?(source, output) }
      raise ArgumentError, "Collection source #{source} overlaps the collection output #{overlap}" if overlap

      found = Dir.glob(File.join(source, "**", pattern))
      duplicates = found.group_by { File.basename(it) }.select { |_, files| files.size > 1 }.keys
      raise "Several inputs share a name: #{duplicates.join(', ')}" if duplicates.any?

      [source, destination, found]
    end
    plan.map do |source, destination, found|
      FileUtils.rm_rf(destination)
      FileUtils.mkdir_p(destination)
      FileUtils.cp(found, destination)
      [source, destination, found.size]
    end
  end

  # Replacing an output would delete a source that is, or lies inside or
  # around, that output.
  def self.overlapping?(source, output)
    source = File.realpath(source)
    output = File.exist?(output) ? File.realpath(output) : File.expand_path(output)
    [[source, output], [output, source]].any? { |path, parent| "#{path}/".start_with?("#{parent}/") }
  end
  private_class_method :overlapping?

  def initialize(train = ReleaseTrain.new)
    @train = train
  end

  # Data members of a .gem as { name => [mode, bytes] }.
  def members(archive) # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
    actual = {}
    File.open(archive, "rb") do |io|
      Gem::Package::TarReader.new(io) do |tar|
        tar.each do |entry|
          next unless entry.full_name == "data.tar.gz"

          Zlib::GzipReader.wrap(StringIO.new(entry.read)) do |gzip|
            Gem::Package::TarReader.new(gzip) do |data|
              data.each do |file|
                raise "Duplicate or non-file member: #{file.full_name}" if actual.key?(file.full_name) || !file.file?

                actual[file.full_name] = [file.header.mode & 0o7777, file.read.to_s]
              end
            end
          end
        end
      end
    end
    actual
  end

  # Keep the complete archive walk and its byte/mode assertions together.
  def check_archive!(archive, specification) # rubocop:disable Metrics/AbcSize
    package = Gem::Package.new(archive)
    raise "Archive manifest differs: #{specification.name}" unless package.contents.sort == specification.files.sort
    raise "Archive version differs: #{specification.name}" unless package.spec.version.to_s == @train.version

    SignatureArchive.check(archive, root: @train.root)
    expected = specification.files.to_h do |file|
      executable = specification.executables.any? { |name| file == "#{specification.bindir}/#{name}" }
      [file, [executable ? 0o755 : 0o644, File.binread(File.join(@train.directory(specification.name), file))]]
    end
    raise "Archive bytes/modes differ: #{specification.name}" unless members(archive) == expected
  end

  # A platform archive is the source gem's Ruby files plus one prebuilt
  # extension per Ruby minor, without the crate sources or an extension build.
  def check_platform_archive!(archive, platform, source) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
    package = Gem::Package.new(archive)
    spec = package.spec
    label = File.basename(archive)
    minors = HyperPlatforms::RUBY_MINORS
    raise "Platform archive identity differs: #{label}" unless spec.name == source.name &&
                                                               spec.version.to_s == @train.version
    unless spec.platform == Gem::Platform.new(platform) && spec.original_platform.to_s == platform &&
           spec.platform.to_s == platform
      raise "Platform archive #{label} is #{spec.original_platform}, expected #{platform}"
    end
    raise "Platform archive declares an extension build: #{label}" unless spec.extensions.empty?
    unless spec.dependencies.map { |dependency| [dependency.name, dependency.type, dependency.requirement.to_s] } ==
           [["connectrpc", :runtime, @train.requirement]]
      raise "Platform archive dependencies differ: #{label}"
    end
    unless spec.required_ruby_version == Gem::Requirement.new(">= #{minors.first}", "< #{minors.last.succ}.dev")
      raise "Platform archive Ruby requirement differs: #{label} (#{spec.required_ruby_version})"
    end

    # rake-compiler requires RubyGems 3.3.22+ for libc-qualified Linux platforms.
    rubygems = platform.end_with?("-linux-musl") ? ">= 3.3.22" : ">= 0"
    unless spec.required_rubygems_version == Gem::Requirement.new(rubygems)
      raise "Platform archive RubyGems requirement differs: #{label} (#{spec.required_rubygems_version})"
    end
    raise "Platform archive require paths differ: #{label}" unless spec.raw_require_paths == ["lib"]
    raise "Platform archive metadata differs: #{label}" unless spec.metadata == source.metadata
    raise "Platform archive manifest differs: #{label}" unless package.contents.sort == spec.files.sort

    SignatureArchive.check(archive, root: @train.root)
    directory = @train.directory(source.name)
    extension = platform.include?("darwin") ? "bundle" : "so"
    binaries = minors.map { |minor| "lib/connectrpc/hyper/#{minor}/connectrpc_hyper.#{extension}" }
    expected = source.files.grep_v(CRATE_SOURCES).to_h do |file|
      [file, [0o644, File.binread(File.join(directory, file))]]
    end
    actual = members(archive)
    raise "Platform archive members differ: #{label}" unless actual.keys.sort == [*expected.keys, *binaries].sort
    raise "Platform archive bytes/modes differ: #{label}" unless actual.slice(*expected.keys) == expected

    binaries.each do |binary|
      mode, bytes = actual.fetch(binary)
      raise "Platform archive extension must be a non-empty 0755 file: #{binary}" unless mode == 0o755 && !bytes.empty?
    end
  end

  # Every hyper archive needs one smoke record per Ruby minor, naming its exact
  # file, bytes and platform. Records are attempt-specific, so candidate.json
  # leaves them out.
  def check_evidence!(directory, archives) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
    raise "Smoke evidence directory is missing: #{directory}" unless directory && Dir.exist?(directory)

    by_file = archives.to_h { |archive| [archive.fetch("file"), archive] }
    seen = {}
    Dir.children(directory).sort.each do |name|
      path = File.join(directory, name)
      raise "Unexpected smoke evidence entry: #{name}" unless name.end_with?(".json") && File.file?(path)

      record = JSON.parse(File.read(path))
      raise "Malformed smoke evidence: #{name}" unless record.is_a?(Hash) && record.keys.sort == EVIDENCE_KEYS.sort

      archive = by_file[record["file"]]
      raise "Smoke evidence #{name} names an unknown archive: #{record['file']}" unless archive
      unless record["sha256"] == archive.fetch("sha256") && record["platform"] == archive.fetch("platform")
        raise "Smoke evidence #{name} does not match #{record['file']}"
      end

      minor = record["ruby"].to_s[/\A\d+\.\d+(?=\.)/]
      raise "Smoke evidence #{name} is for an unexpected Ruby #{record['ruby']}" unless
        HyperPlatforms::RUBY_MINORS.include?(minor)
      raise "Duplicate smoke evidence for #{record['file']} on Ruby #{minor}" if seen.key?([record["file"], minor])

      seen[[record["file"], minor]] = name
    end
    missing = by_file.keys.product(HyperPlatforms::RUBY_MINORS).reject { |key| seen.key?(key) }
    return if missing.empty?

    raise "Missing smoke evidence: #{missing.map { |file, minor| "#{file} on Ruby #{minor}" }.join(', ')}"
  end

  # Builds every canonical source archive twice, checks the tested platform
  # archives and their smoke evidence, and only then moves the complete set
  # into output. Nothing is written to output on failure.
  def build!(output:, revision:, epoch:, platform_archives: nil, evidence: nil) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
    @train.check!
    raise "Candidate output must be missing or empty: #{output}" if Dir.exist?(output) && !Dir.empty?(output)

    tested = tested_archives(platform_archives)
    previous = ENV.fetch("SOURCE_DATE_EPOCH", nil)
    FileUtils.mkdir_p(File.dirname(output))
    staging = Dir.mktmpdir(".candidate-", File.dirname(output))
    begin
      ENV["SOURCE_DATE_EPOCH"] = Integer(epoch).to_s
      specifications = @train.specifications
      inventory = specifications.map do |spec|
        archive = PackageBuild.build(directory: @train.directory(spec.name), output: staging)
        check_archive!(archive, spec)
        { "file" => File.basename(archive), "platform" => "ruby", "sha256" => Digest::SHA256.file(archive).hexdigest,
          "members" => spec.files.sort }
      end
      Dir.mktmpdir("connectrpc-rebuild-") do |temporary|
        specifications.zip(inventory).each do |spec, record|
          rebuilt = PackageBuild.build(directory: @train.directory(spec.name), output: temporary)
          unless Digest::SHA256.file(rebuilt).hexdigest == record["sha256"]
            raise "Non-reproducible archive: #{spec.name}"
          end
        end
      end

      tested_records = []
      tested.each do |archive|
        sha256 = Digest::SHA256.file(archive.fetch(:path)).hexdigest
        if archive.fetch(:platform) == "ruby"
          canonical = inventory.find { |record| record["file"] == archive.fetch(:file) }
          raise "Tested source gem differs from the canonical build: #{archive.fetch(:file)}" unless
            canonical["sha256"] == sha256
        else
          source = specifications.find { |spec| spec.name == archive.fetch(:name) }
          check_platform_archive!(archive.fetch(:path), archive.fetch(:platform), source)
          FileUtils.cp(archive.fetch(:path), File.join(staging, archive.fetch(:file)))
          inventory << { "file" => archive.fetch(:file), "platform" => archive.fetch(:platform), "sha256" => sha256,
                         "members" => Gem::Package.new(archive.fetch(:path)).contents.sort }
        end
        tested_records << { "file" => archive.fetch(:file), "platform" => archive.fetch(:platform),
                            "sha256" => sha256 }
      end
      check_evidence!(evidence, tested_records) unless tested.empty?

      expected = @train.archives.map(&:file).sort
      raise "Unexpected candidate artifact inventory" unless Dir.children(staging).sort == expected &&
                                                             inventory.map { |record| record["file"] }.sort == expected

      record = { "version" => @train.version, "revision" => revision, "source_date_epoch" => Integer(epoch),
                 "ruby" => RUBY_VERSION, "rubygems" => Gem::VERSION, "push_host" => "https://rubygems.org",
                 "artifacts" => inventory }
      File.write(File.join(staging, "candidate.json"), "#{JSON.pretty_generate(record)}\n")
      raise "Candidate output must be missing or empty: #{output}" if Dir.exist?(output) && !Dir.empty?(output)

      begin
        Dir.rmdir(output)
      rescue Errno::ENOENT
        # No empty output directory to replace.
      end
      File.chmod(0o755, staging)
      File.rename(staging, output)
    ensure
      ENV["SOURCE_DATE_EPOCH"] = previous
      FileUtils.rm_rf(staging)
    end
    output
  end

  def build_from_git!(platform_archives:, evidence:) # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
    @train.check!
    raise "Canonical candidates require Ruby 3.4.12" unless RUBY_VERSION == "3.4.12"

    status, result = Open3.capture2("git", "status", "--porcelain", "--untracked-files=all", chdir: @train.root)
    raise "Candidate source must be a clean Git checkout" unless result.success? && status.empty?

    revision, result = Open3.capture2("git", "rev-parse", "HEAD", chdir: @train.root)
    raise "Cannot identify candidate revision" unless result.success?

    epoch, result = Open3.capture2("git", "show", "-s", "--format=%ct", "HEAD", chdir: @train.root)
    raise "Cannot identify candidate timestamp" unless result.success?

    output = File.join(@train.root, "vendor/release", @train.version, revision.strip)
    build!(output: output, revision: revision.strip, epoch: epoch.strip, platform_archives: platform_archives,
           evidence: evidence)
  end

  private

  # The CI-built archives of every gem in ReleaseTrain::PLATFORMS: exactly its
  # tested source gem and one archive per platform, in one flat directory.
  def tested_archives(directory) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
    expected = @train.archives.select { |archive| ReleaseTrain::PLATFORMS.key?(archive.name) }
    return [] if expected.empty?
    unless directory && Dir.exist?(directory)
      raise "Tested platform archives directory is missing: #{directory.inspect}"
    end

    found = Dir.children(directory).sort
    files = expected.map(&:file)
    missing = files - found
    unexpected = found - files
    raise "Missing tested archives in #{directory}: #{missing.join(', ')}" if missing.any?
    raise "Unexpected entries in #{directory}: #{unexpected.join(', ')}" if unexpected.any?

    expected.map do |archive|
      path = File.join(directory, archive.file)
      raise "Tested archive must be a regular file: #{path}" unless File.lstat(path).file?

      { name: archive.name, platform: archive.platform, file: archive.file, path: path }
    end
  end
end
