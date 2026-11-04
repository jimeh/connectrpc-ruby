# frozen_string_literal: true

require "fileutils"
require "open3"
require "rbconfig"
require "rubygems/package"
require "stringio"
require "tmpdir"
require_relative "../typecheck/archive"

RSpec.describe "Canonical package builds" do
  root = File.expand_path("..", __dir__)
  directories = [root, *%w[async falcon rack rails testing otel].map { |name| "#{root}/gems/connectrpc-#{name}" }]

  def archive_files(archive)
    files = {}
    File.open(archive, "rb") do |io|
      Gem::Package::TarReader.new(io) do |tar|
        tar.each do |entry|
          next unless entry.full_name == "data.tar.gz"

          Zlib::GzipReader.wrap(StringIO.new(entry.read)) do |gzip|
            Gem::Package::TarReader.new(gzip) do |data|
              data.each { |file| files[file.full_name] = [file.header.mode & 0o7777, file.read] }
            end
          end
        end
      end
    end
    files
  end

  directories.each do |directory|
    name = directory == root ? "connectrpc" : File.basename(directory)
    %w[002 022].each do |mask|
      it "preserves #{name} bytes and metadata with canonical archive modes under umask #{mask}" do
        specification = Dir.chdir(directory) { Gem::Specification.load(Dir.glob("*.gemspec").fetch(0)) }
        executables = specification.executables.map { |name| File.join(specification.bindir, name) }
        expected = specification.files.to_h do |relative|
          [relative, [executables.include?(relative) ? 0o755 : 0o644, File.binread(File.join(directory, relative))]]
        end
        Dir.mktmpdir("connectrpc-build-spec-") do |temporary|
          source = File.join(temporary, "source")
          output = File.join(temporary, "output")
          # Synthetic group-writable inputs reproduce umask-002 checkouts without
          # changing any permissions in the actual checkout.
          inputs = [*specification.files, File.basename(Dir.glob("#{directory}/*.gemspec").fetch(0))]
          inputs.each do |relative|
            destination = File.join(source, relative)
            FileUtils.mkdir_p(File.dirname(destination))
            FileUtils.cp(File.join(directory, relative), destination)
            File.chmod(executables.include?(relative) ? 0o775 : 0o664, destination)
          end
          snapshot = inputs.to_h do |relative|
            file = File.join(source, relative)
            [relative, [File.stat(file).mode, File.binread(file)]]
          end
          command = "File.umask(Integer(ARGV.shift, 8)); PackageBuild.build(directory: ARGV.shift, output: ARGV.shift)"
          stdout, stderr, status = Open3.capture3(RbConfig.ruby, "-r", File.join(root, "scripts/package_build"),
                                                  "-e", command, mask, source, output)
          expect(status.success?).to be(true), "#{stdout}\n#{stderr}"
          archive = File.join(output, specification.file_name)
          expect(archive_files(archive)).to eq(expected)
          package = Gem::Package.new(archive)
          expect(package.contents.sort).to eq(specification.files.sort)
          expect(package.spec.to_ruby).to eq(specification.to_ruby)
          SignatureArchive.check(archive, root: source)
          snapshot.each do |relative, original|
            file = File.join(source, relative)
            expect([File.stat(file).mode, File.binread(file)]).to eq(original)
          end
        end
      end
    end
  end
end
