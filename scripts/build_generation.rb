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
Dir.mktmpdir("generation-") do |temporary|
  protoc_output = File.join(temporary, "protoc")
  buf_output = File.join(temporary, "buf")
  echo_output = File.join(temporary, "echo")
  [protoc_output, buf_output, echo_output].each { |dir| FileUtils.mkdir_p(dir) }
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
  files = Dir.glob("**/*.rb", base: protoc_output).sort
  raise "Buf file set differs" unless files == Dir.glob("**/*.rb", base: buf_output).sort

  files.grep(/_connect\.rb\z/).each do |name|
    unless File.binread(File.join(protoc_output, name)) == File.binread(File.join(buf_output, name))
      raise "protoc/Buf byte mismatch: #{name}"
    end
  end
  { protoc_output => File.join(fixture, "ruby"), echo_output => File.join(fixture, "echo") }.each do |from, to|
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
  [protoc_output, buf_output].each do |load_path|
    system(RbConfig.ruby, "-I", load_path, File.join(root, "scripts/generated_integration.rb"), exception: true)
  end
  puts "protoc 36.2 / Buf 1.75.0 glue parity: #{files.grep(/_connect\.rb\z/).size} files"
  puts "Both official message outputs loaded; official echo: 2 files"
end
