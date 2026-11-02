# frozen_string_literal: true

require "digest"
require "json"
require "open3"
require "tmpdir"

root = File.expand_path("../conformance", __dir__)
version, status = Open3.capture2("protoc", "--version")
raise "Expected libprotoc 36.2: #{version}" unless status.success? && version.strip == "libprotoc 36.2"

hashes = JSON.parse(File.read(File.join(root, "schema-sha256.json")))
hashes.each do |name, expected|
  raise "Vendored schema changed: #{name}" unless Digest::SHA256.file(File.join(root, "proto",
                                                                                name)).hexdigest == expected
end
inputs = %w[config service client_compat server_compat].map { |name| "connectrpc/conformance/v1/#{name}.proto" }
Dir.mktmpdir("descriptor-", File.join(root, "..", "vendor")) do |temporary|
  output = File.join(temporary, "descriptor.bin")
  # protoc finds its bundled google/protobuf includes relative to its own binary.
  system("protoc", "--proto_path=#{File.join(root, 'proto')}", "--include_imports",
         "--descriptor_set_out=#{output}", *inputs, exception: true)
  target = File.join(root, "descriptor.bin")
  if ARGV == ["--check"]
    unless File.binread(target) == File.binread(output)
      raise "Conformance descriptor differs; run mise run conformance:build"
    end
  elsif ARGV.empty?
    File.binwrite(target, File.binread(output))
  else
    raise "Usage: build_conformance.rb [--check]"
  end
  puts "Official conformance descriptor SHA256 #{Digest::SHA256.file(output).hexdigest}"
end
