# frozen_string_literal: true

require "rubygems/package"
require "stringio"
require "zlib"

module SignatureArchive
  GRPC_MEMBERS = %w[lib/connectrpc/bindings/descriptors/status.bin third_party/google/rpc/status.proto
                    third_party/google/LICENSE third_party/protobuf-LICENSE third_party/README.md
                    docs/grpc.md docs/grpc-web.md].freeze
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
    found_grpc = []
    Gem::Package::TarReader.new(File.open(archive, "rb")) do |tar|
      tar.each do |entry|
        next unless entry.full_name == "data.tar.gz"

        Gem::Package::TarReader.new(Zlib::GzipReader.new(StringIO.new(entry.read))) do |data|
          data.each do |file|
            if spec.name == "connectrpc" && GRPC_MEMBERS.include?(file.full_name)
              source = File.join(root, file.full_name)
              unless file.read == File.binread(source) && file.header.mode & 0o777 == File.stat(source).mode & 0o777
                raise "gRPC archive member bytes or mode differ: #{file.full_name}"
              end

              found_grpc << file.full_name
            elsif file.full_name == "LICENSE"
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
    if spec.name == "connectrpc" && found_grpc.sort != GRPC_MEMBERS.sort
      raise "Missing or duplicate gRPC archive members"
    end
    raise "Missing core executable" if spec.name == "connectrpc" && !found_executable
  end
end
