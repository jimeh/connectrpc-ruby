# frozen_string_literal: true

require "digest"
require "fileutils"
require "json"
require "open3"
require "rbconfig"
require "tmpdir"

raise "Usage: build_generation.rb [--check]" unless ARGV.empty? || ARGV == ["--check"]

{ "protoc" => "libprotoc 36.2", "buf" => "1.75.0" }.each do |tool, expected|
  version, status = Open3.capture2(tool, "--version")
  raise "Expected #{tool} #{expected}: #{version}" unless status.success? && version.strip == expected
end
root = File.expand_path("..", __dir__)
fixture = File.join(root, "spec/fixtures/generation")
source = File.join(fixture, "proto")
plugin = File.join(root, "exe/protoc-gen-connectrpc-ruby")
# The executable is also usable uninstalled via an explicit development load path.
ENV["RUBYLIB"] = File.join(root, "lib")
inputs = Dir.glob("**/*.proto", base: source).sort
# The optional health gem vendors the canonical grpc-proto schema unchanged.
health = File.join(root, "gems/connectrpc-health")
health_schema = "grpc/health/v1/health.proto"
health_digest = Digest::SHA256.file(File.join(health, "proto", health_schema)).hexdigest
unless health_digest == "164b670058855c6277a0511a14e2a302dd5a8ff899853ca6851b6b201a413d28"
  raise "Canonical grpc.health.v1 schema changed: #{health_digest}"
end

Dir.mktmpdir("generation-") do |temporary|
  protoc_output = File.join(temporary, "protoc")
  buf_output = File.join(temporary, "buf")
  echo_output = File.join(temporary, "echo")
  demo_output = File.join(temporary, "demo")
  health_output = File.join(temporary, "health")
  [protoc_output, buf_output, echo_output, demo_output, health_output].each { |dir| FileUtils.mkdir_p(dir) }
  system("protoc", "-I", source, "--ruby_out=#{protoc_output}", "--connectrpc-ruby_out=#{protoc_output}",
         "--plugin=protoc-gen-connectrpc-ruby=#{plugin}", *inputs, exception: true)
  # JSON is a YAML subset. Both entries are real local Buf plugin invocations;
  # the message generator is the pinned official protoc builtin.
  template = {
    version: "v2", plugins: [
      { protoc_builtin: "ruby", out: buf_output },
      { local: [plugin], out: buf_output }
    ]
  }
  system("buf", "generate", source, "--template", JSON.generate(template), exception: true)
  system("protoc", "-I", File.join(root, "spec/fixtures"), "--ruby_out=#{echo_output}",
         "--connectrpc-ruby_out=#{echo_output}", "--plugin=protoc-gen-connectrpc-ruby=#{plugin}",
         "echo.proto", exception: true)
  # The runnable examples share one checked-in demo schema and its official output.
  system("protoc", "-I", File.join(root, "examples/proto"), "--ruby_out=#{demo_output}",
         "--connectrpc-ruby_out=#{demo_output}", "--plugin=protoc-gen-connectrpc-ruby=#{plugin}",
         "demo/v1/demo.proto", exception: true)
  system("protoc", "-I", File.join(health, "proto"), "--ruby_out=#{health_output}", "--rbs_out=#{health_output}",
         "--connectrpc-ruby_out=rbs=true:#{health_output}", "--plugin=protoc-gen-connectrpc-ruby=#{plugin}",
         health_schema, exception: true)
  files = Dir.glob("**/*.rb", base: protoc_output).sort
  raise "Buf file set differs" unless files == Dir.glob("**/*.rb", base: buf_output).sort

  files.grep(/_connect\.rb\z/).each do |name|
    unless File.binread(File.join(protoc_output, name)) == File.binread(File.join(buf_output, name))
      raise "protoc/Buf byte mismatch: #{name}"
    end
  end
  outputs = { protoc_output => File.join(fixture, "ruby"), echo_output => File.join(fixture, "echo"),
              demo_output => File.join(root, "examples/gen") }
  outputs.each do |from, to|
    generated = Dir.glob("**/*.rb", base: from).sort
    if ARGV == ["--check"]
      existing = Dir.glob("**/*.rb", base: to).sort
      raise "Fixture file set differs: #{to}" unless generated == existing
    end

    generated.each do |name|
      bytes = File.binread(File.join(from, name))
      target = File.join(to, name)
      if ARGV == ["--check"]
        raise "Fixture differs: #{name}; run mise run generation:build" unless File.binread(target) == bytes
      else
        FileUtils.mkdir_p(File.dirname(target))
        File.binwrite(target, bytes)
      end
      puts "#{name} SHA256 #{Digest::SHA256.hexdigest(bytes)}"
    end
  end
  # Ruby lands under the gem's lib/ and signatures under its sig/, both at
  # schema-relative paths. Each generated subtree must match exactly.
  { "rb" => "lib", "rbs" => "sig" }.each do |extension, directory|
    generated = Dir.glob("**/*.#{extension}", base: health_output).sort
    target_root = File.join(health, directory)
    if ARGV == ["--check"]
      existing = Dir.glob("grpc/**/*.#{extension}", base: target_root).sort
      raise "Health #{directory}/grpc file set differs" unless generated == existing
    end

    generated.each do |name|
      bytes = File.binread(File.join(health_output, name))
      target = File.join(target_root, name)
      if ARGV == ["--check"]
        unless File.binread(target) == bytes
          raise "Health file differs: #{directory}/#{name}; run mise run generation:build"
        end
      else
        FileUtils.mkdir_p(File.dirname(target))
        File.binwrite(target, bytes)
      end
      puts "health #{directory}/#{name} SHA256 #{Digest::SHA256.hexdigest(bytes)}"
    end
  end
  [protoc_output, buf_output].each do |load_path|
    system(RbConfig.ruby, "-I", load_path, File.join(root, "scripts/generated_integration.rb"), exception: true)
  end
  puts "protoc 36.2 / Buf 1.75.0 glue parity: #{files.grep(/_connect\.rb\z/).size} files"
  puts "Both official message outputs loaded; official echo: 2 files; runnable demo: 2 files; " \
       "health: 2 Ruby files and 2 signatures from schema SHA256 #{health_digest}"
end
