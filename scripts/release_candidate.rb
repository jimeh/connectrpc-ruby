# frozen_string_literal: true

require "digest"
require "fileutils"
require "open3"
require "stringio"
require "tmpdir"
require "zlib"
require_relative "package_build"
require_relative "release_train"
require_relative "../typecheck/archive"

class ReleaseCandidate
  def initialize(train = ReleaseTrain.new)
    @train = train
  end

  # Keep the complete archive walk and its byte/mode assertions together.
  def check_archive!(archive, specification) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
    package = Gem::Package.new(archive)
    raise "Archive manifest differs: #{specification.name}" unless package.contents.sort == specification.files.sort
    raise "Archive version differs: #{specification.name}" unless package.spec.version.to_s == @train.version

    SignatureArchive.check(archive, root: @train.root)
    expected = specification.files.to_h do |file|
      executable = specification.executables.any? { |name| file == "#{specification.bindir}/#{name}" }
      [file, [executable ? 0o755 : 0o644, File.binread(File.join(@train.directory(specification.name), file))]]
    end
    actual = {}
    File.open(archive, "rb") do |io|
      Gem::Package::TarReader.new(io) do |tar|
        tar.each do |entry|
          next unless entry.full_name == "data.tar.gz"

          Zlib::GzipReader.wrap(StringIO.new(entry.read)) do |gzip|
            Gem::Package::TarReader.new(gzip) do |data|
              data.each do |file|
                raise "Duplicate or non-file member: #{file.full_name}" if actual.key?(file.full_name) || !file.file?

                actual[file.full_name] = [file.header.mode & 0o7777, file.read]
              end
            end
          end
        end
      end
    end
    raise "Archive bytes/modes differ: #{specification.name}" unless actual == expected
  end

  def build!(output:, revision:, epoch:) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength
    @train.check!
    raise "Candidate output must be missing or empty: #{output}" if Dir.exist?(output) && !Dir.empty?(output)

    previous = ENV.fetch("SOURCE_DATE_EPOCH", nil)
    begin
      ENV["SOURCE_DATE_EPOCH"] = Integer(epoch).to_s
      specifications = @train.specifications
      inventory = specifications.map do |spec|
        archive = PackageBuild.build(directory: @train.directory(spec.name), output: output)
        check_archive!(archive, spec)
        { "file" => File.basename(archive), "sha256" => Digest::SHA256.file(archive).hexdigest,
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
      expected = specifications.map(&:file_name).sort
      raise "Unexpected candidate artifact inventory" unless Dir.children(output).sort == expected

      record = { "version" => @train.version, "revision" => revision, "source_date_epoch" => Integer(epoch),
                 "ruby" => RUBY_VERSION, "rubygems" => Gem::VERSION, "push_host" => "https://rubygems.org",
                 "artifacts" => inventory }
      File.write(File.join(output, "candidate.json"), "#{JSON.pretty_generate(record)}\n")
    ensure
      ENV["SOURCE_DATE_EPOCH"] = previous
    end
    output
  end

  def build_from_git! # rubocop:disable Metrics/AbcSize
    @train.check!
    raise "Canonical candidates require Ruby 3.4.12" unless RUBY_VERSION == "3.4.12"

    status, result = Open3.capture2("git", "status", "--porcelain", "--untracked-files=all", chdir: @train.root)
    raise "Candidate source must be a clean Git checkout" unless result.success? && status.empty?

    revision, result = Open3.capture2("git", "rev-parse", "HEAD", chdir: @train.root)
    raise "Cannot identify candidate revision" unless result.success?

    epoch, result = Open3.capture2("git", "show", "-s", "--format=%ct", "HEAD", chdir: @train.root)
    raise "Cannot identify candidate timestamp" unless result.success?

    output = File.join(@train.root, "vendor/release", @train.version, revision.strip)
    build!(output: output, revision: revision.strip, epoch: epoch.strip)
  end
end
