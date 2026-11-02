# frozen_string_literal: true

require "rbconfig"
require "tmpdir"

root = File.expand_path("..", __dir__)
Dir.mktmpdir("generated-example-") do |temporary|
  environment = { "RUBYLIB" => File.join(root, "lib") }
  system(environment, "protoc", "-I", File.join(root, "examples/proto"), "--ruby_out=#{temporary}",
         "--connectrpc-ruby_out=#{temporary}",
         "--plugin=protoc-gen-connectrpc-ruby=#{File.join(root, 'exe/protoc-gen-connectrpc-ruby')}",
         "greeting.proto", exception: true)
  system(RbConfig.ruby, "-I", temporary, File.join(root, "examples/generated.rb"), exception: true)
end
