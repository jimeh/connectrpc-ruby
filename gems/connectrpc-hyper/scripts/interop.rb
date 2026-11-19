# frozen_string_literal: true

# The grpc-go/connect-go interop matrix: every call shape over h2c, TLS and
# mTLS, connect-go over HTTP/1.1, trailers-only, Connect GET, ALPN and server
# resets, against the checksum-verified local peer that hyper:peer built.
require "rbconfig"

package = File.expand_path("..", __dir__)
system("go", "mod", "verify", chdir: File.join(package, "interop"), exception: true)
system(RbConfig.ruby, "-S", "rspec", "--tag", "interop", *ARGV, chdir: package, exception: true)
puts "connectrpc-hyper grpc-go/connect-go interop passed on Ruby #{RUBY_VERSION}"
