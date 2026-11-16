# frozen_string_literal: true

require "digest"
require "rubygems/package"
require "stringio"
require "zlib"

module SignatureArchive
  GRPC_MEMBERS = %w[lib/connectrpc/bindings/descriptors/status.bin third_party/google/rpc/status.proto
                    third_party/google/LICENSE third_party/protobuf-LICENSE third_party/README.md
                    docs/grpc.md docs/grpc-web.md].freeze
  # The health package vendors the Apache-2.0 grpc-proto schema and its notice.
  HEALTH_SCHEMA = "proto/grpc/health/v1/health.proto"
  HEALTH_SCHEMA_SHA256 = "164b670058855c6277a0511a14e2a302dd5a8ff899853ca6851b6b201a413d28"
  HEALTH_NOTICES = %w[proto/LICENSE proto/README.md].freeze
  LICENSES = { "connectrpc-health" => %w[MIT Apache-2.0].freeze }.freeze

  def self.licenses(name) = LICENSES.fetch(name, %w[MIT])

  # Returns the member name after checking its pinned schema or license notice.
  def self.health_member(name, bytes)
    raise "Vendored health schema changed" if name == HEALTH_SCHEMA && Digest::SHA256.hexdigest(bytes) !=
                                                                       HEALTH_SCHEMA_SHA256
    raise "Apache license notice missing" if name == "proto/LICENSE" && !bytes.include?("Apache License")

    name
  end

  def self.check(archive, root:)
    package = Gem::Package.new(archive)
    spec = package.spec
    raise "Push host changed: #{spec.name}" unless spec.metadata["allowed_push_host"] == "https://rubygems.org"
    raise "MFA requirement lost: #{spec.name}" unless spec.metadata["rubygems_mfa_required"] == "true"
    raise "License metadata changed: #{spec.name}" unless spec.licenses == licenses(spec.name)
    raise "Type tool runtime dependency: #{spec.name}" if spec.runtime_dependencies.any? do |dependency|
      %w[rbs steep].include?(dependency.name)
    end

    license = File.binread(File.join(root, "LICENSE"))
    found_license = false
    found_executable = false
    found_grpc = []
    found_health = []
    Gem::Package::TarReader.new(File.open(archive, "rb")) do |tar|
      tar.each do |entry|
        next unless entry.full_name == "data.tar.gz"

        Gem::Package::TarReader.new(Zlib::GzipReader.new(StringIO.new(entry.read))) do |data|
          data.each do |file|
            if spec.name == "connectrpc" && GRPC_MEMBERS.include?(file.full_name)
              source = File.join(root, file.full_name)
              unless file.read == File.binread(source) && file.header.mode & 0o777 == 0o644
                raise "gRPC archive member bytes differ or mode is not 0644: #{file.full_name}"
              end

              found_grpc << file.full_name
            elsif spec.name == "connectrpc-health" && [HEALTH_SCHEMA, *HEALTH_NOTICES].include?(file.full_name)
              found_health << health_member(file.full_name, file.read.to_s)
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
    return unless spec.name == "connectrpc-health" && found_health.sort != [HEALTH_SCHEMA, *HEALTH_NOTICES].sort

    raise "Missing or duplicate health schema notices"
  end
end
