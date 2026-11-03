# frozen_string_literal: true

require "digest"
require "fileutils"
require "tmpdir"

raise "Usage: build_grpc_peer.rb [--check]" unless ARGV.empty? || ARGV == ["--check"]

root = File.expand_path("..", __dir__)
Dir.mktmpdir("grpc-peer-generation-") do |temporary| # rubocop:disable Metrics/BlockLength
  go = File.join(temporary, "go")
  ruby = File.join(temporary, "ruby")
  [go, ruby].each { |dir| FileUtils.mkdir_p(dir) }
  ENV["RUBYLIB"] = File.join(root, "lib")
  system("protoc", "-I", File.join(root, "interop/grpc/proto"), "--go_out=paths=source_relative:#{go}",
         "--go-grpc_out=paths=source_relative:#{go}",
         "--plugin=protoc-gen-go=#{File.join(root, 'vendor/grpc/bin/protoc-gen-go')}",
         "--plugin=protoc-gen-go-grpc=#{File.join(root, 'vendor/grpc/bin/protoc-gen-go-grpc')}",
         "--ruby_out=#{ruby}", "--rbs_out=#{ruby}", "--connectrpc-ruby_out=rbs=true:#{ruby}",
         "--plugin=protoc-gen-connectrpc-ruby=#{File.join(root, 'exe/protoc-gen-connectrpc-ruby')}",
         "peer.proto", exception: true)
  { go => "interop/grpc/gen", ruby => "interop/grpc/ruby" }.each do |source, destination|
    target = File.join(root, destination)
    names = Dir.children(source).sort
    raise "Generated file set differs: #{destination}" if ARGV == ["--check"] && Dir.children(target).sort != names

    FileUtils.mkdir_p(target)
    names.each do |name|
      bytes = File.binread(File.join(source, name))
      file = File.join(target, name)
      if ARGV == ["--check"]
        raise "Generated file differs: #{file}" unless File.binread(file) == bytes
      else
        File.binwrite(file, bytes)
      end
      puts "#{destination}/#{name} SHA256 #{Digest::SHA256.hexdigest(bytes)}"
    end
  end
end
