# frozen_string_literal: true

require "rubygems/package"
require "stringio"
require "zlib"

module SignatureArchive
  def self.check(archive, root:)
    package = Gem::Package.new(archive)
    spec = package.spec
    raise "Publication guard lost: #{spec.name}" unless spec.metadata["allowed_push_host"] == "https://gems.invalid"
    raise "MIT metadata lost: #{spec.name}" unless spec.licenses == ["MIT"]
    raise "Type tool runtime dependency: #{spec.name}" if spec.runtime_dependencies.any? do |dependency|
      %w[rbs steep].include?(dependency.name)
    end

    license = File.binread(File.join(root, "LICENSE"))
    found_license = false
    found_executable = false
    Gem::Package::TarReader.new(File.open(archive, "rb")) do |tar|
      tar.each do |entry|
        next unless entry.full_name == "data.tar.gz"

        Gem::Package::TarReader.new(Zlib::GzipReader.new(StringIO.new(entry.read))) do |data|
          data.each do |file|
            if file.full_name == "LICENSE"
              raise "Archive license differs: #{spec.name}" unless file.read == license

              found_license = true
            elsif file.full_name == "exe/protoc-gen-connectrpc-ruby"
              found_executable = true
              unless file.header.mode & 0o777 == 0o755
                raise "Archived executable mode must be 0755: #{format('%04o', file.header.mode & 0o777)}"
              end
            end
          end
        end
      end
    end
    raise "Missing archive license: #{spec.name}" unless found_license
    raise "Missing core executable" if spec.name == "connectrpc" && !found_executable
  end
end
