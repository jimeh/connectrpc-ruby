# frozen_string_literal: true

require "digest"
require "fileutils"
require "json"
require "open3"
require "rbconfig"

root = File.expand_path("..", __dir__)
output = File.expand_path(ARGV.fetch(0))
source = File.join(root, "spec/fixtures/rbs/proto")
plugin = ENV.fetch("CONNECTRPC_RBS_PLUGIN", File.join(root, "exe/protoc-gen-connectrpc-ruby"))
{ "protoc" => "libprotoc 36.2", "buf" => "1.75.0" }.each do |tool, expected|
  version, status = Open3.capture2(tool, "--version")
  raise "Expected #{tool} #{expected}" unless status.success? && version.strip == expected
end
inputs = Dir.glob("*.proto", base: source).sort
imports = %w[google/protobuf/timestamp.proto google/protobuf/empty.proto]
%w[default protoc buf upstream].each { |name| FileUtils.mkdir_p(File.join(output, name)) }
environment = { "RUBYLIB" => File.join(root, "lib") }
base = ["protoc", "-I", source, "--plugin=protoc-gen-connectrpc-ruby=#{plugin}"]
system(environment, *base, "--ruby_out=#{output}/default", "--connectrpc-ruby_out=#{output}/default",
       *inputs, *imports, exception: true)
system(environment, *base, "--ruby_out=#{output}/protoc", "--rbs_out=#{output}/protoc",
       "--connectrpc-ruby_out=rbs=true:#{output}/protoc", *inputs, *imports, exception: true)
system(environment, "protoc", "-I", source, "--rbs_out=#{output}/upstream", *inputs, *imports, exception: true)
template = { version: "v2", plugins: [
  { protoc_builtin: "ruby", out: "#{output}/buf" },
  { protoc_builtin: "rbs", out: "#{output}/buf" },
  { local: [plugin], out: "#{output}/buf", opt: "rbs=true" }
] }
system(environment, "buf", "generate", source, "--template", JSON.generate(template), exception: true)
# Buf generates local schemas; protoc also generates the imported WKT signatures
# which applications must put in their RBS environment.
Dir.glob("**/*_connect.{rb,rbs}", base: "#{output}/protoc").sort.each do |name|
  bytes = File.binread("#{output}/protoc/#{name}")
  raise "Buf glue mismatch: #{name}" unless bytes == File.binread("#{output}/buf/#{name}")
end
Dir.glob("**/*_pb.rbs", base: "#{output}/buf").each do |name|
  raise "Buf official RBS mismatch: #{name}" unless File.binread("#{output}/buf/#{name}") ==
                                                    File.binread("#{output}/upstream/#{name}")
end
Dir.glob("**/*.rb", base: "#{output}/default").each do |name|
  raise "Default Ruby changed: #{name}" unless File.binread("#{output}/default/#{name}") ==
                                               File.binread("#{output}/protoc/#{name}")
end
Dir.glob("**/*_pb.rbs", base: "#{output}/upstream").each do |name|
  raise "Official RBS changed: #{name}" unless File.binread("#{output}/upstream/#{name}") ==
                                               File.binread("#{output}/protoc/#{name}")
end
# A repeated invocation must reproduce every byte, including official fields.
first = Dir.glob("**/*", base: "#{output}/protoc").select { |name| File.file?("#{output}/protoc/#{name}") }
           .to_h { |name| [name, Digest::SHA256.file("#{output}/protoc/#{name}").hexdigest] }
system(environment, *base, "--ruby_out=#{output}/protoc", "--rbs_out=#{output}/protoc",
       "--connectrpc-ruby_out=rbs=true:#{output}/protoc", *inputs, *imports, exception: true)
raise "Nondeterministic generation" unless first.all? do |name, sha|
  sha == Digest::SHA256.file("#{output}/protoc/#{name}").hexdigest
end

puts "protoc/Buf RBS parity; default Ruby bytes unchanged; official field RBS unchanged; " \
     "deterministic #{first.size} files"
