# frozen_string_literal: true

require "digest"
require "fileutils"
require "open3"
require "tmpdir"

raise "Usage: build_grpc_status.rb [--check]" unless ARGV.empty? || ARGV == ["--check"]

root = File.expand_path("..", __dir__)
schema = File.join(root, "third_party/google/rpc/status.proto")
unless Digest::SHA256.file(schema).hexdigest == "f5bfd262e6705c7ae73f32e0ad8ee20ce8c0a2578df8c4f76ebf76b572f295ed"
  raise "Canonical google.rpc.Status schema changed"
end

version, status = Open3.capture2("protoc", "--version")
raise "Expected protoc 36.2" unless status.success? && version.strip == "libprotoc 36.2"

Dir.mktmpdir("grpc-status-") do |temporary|
  output = File.join(temporary, "status.bin")
  system("protoc", "-I", File.join(root, "third_party"), "--include_imports", "--descriptor_set_out=#{output}",
         "google/rpc/status.proto", exception: true)
  bytes = File.binread(output)
  digest = Digest::SHA256.hexdigest(bytes)
  unless digest == "01744162857a59da819ea411e4a24484ba0b65830c03f62f53f1c7fb0b6edd24"
    raise "Canonical status descriptor differs: #{digest}"
  end

  target = File.join(root, "lib/connectrpc/bindings/descriptors/status.bin")
  if ARGV == ["--check"]
    raise "Run mise run grpc:status:build" unless File.binread(target) == bytes
  else
    File.binwrite(target, bytes)
  end
  puts "Private Any + Status descriptor SHA256 #{digest} (#{bytes.bytesize} bytes)"
end
