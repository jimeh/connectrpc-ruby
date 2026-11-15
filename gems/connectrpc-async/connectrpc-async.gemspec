# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = "connectrpc-async"
  # Read without loading library code before SimpleCov starts.
  spec.version = File.read(File.join(__dir__, "lib/connectrpc/async/version.rb"))[/VERSION = "([^"]+)"/, 1]
  spec.summary = "Async HTTP/2 host and HTTP/1.1 and HTTP/2 clients for ConnectRPC"
  spec.description = "Standalone Async HTTP/2 host and pooled client transports for connectrpc: Connect unary and " \
                     "streaming, native gRPC and server gRPC-Web over h2c, verified TLS and mTLS, an explicit " \
                     "HTTP/1.1 client, and ThreadedTransport for unary and server-streaming calls from ordinary " \
                     "threads. Part of the unofficial connectrpc-ruby project. Experimental and not production ready."
  spec.authors = ["jimeh"]
  spec.homepage = "https://github.com/jimeh/connectrpc-ruby"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.4"
  spec.metadata["allowed_push_host"] = "https://rubygems.org"
  spec.metadata["rubygems_mfa_required"] = "true"
  spec.files = Dir.glob("sig/**/*.{rbs,yaml}") + Dir["lib/**/*.rb", "LICENSE", "README.md"].sort
  spec.require_paths = ["lib"]
  local_requirement = "~> #{spec.version.to_s.split('.').first(2).join('.')}.0"
  # async, async-http, protocol-http2, protocol-http1 and io-stream stay patch-only (~> x.y.z) because
  # the transports and completion joins couple to their private internals (see issue #68). The rest use
  # only public APIs, so they follow the floor policy: the tested release as floor, capped below 1.
  {
    "connectrpc" => local_requirement, "async-http" => "~> 0.105.0", "async" => "~> 2.46.0",
    "protocol-http2" => "~> 0.29.2", "protocol-http1" => "~> 0.41.1",
    "protocol-http" => [">= 0.72.1", "< 1"], "io-endpoint" => [">= 0.19.0", "< 1"],
    "io-stream" => "~> 0.14.0", "openssl" => ">= 3.3"
  }.each { |name, requirement| spec.add_dependency(name, *requirement) }
end
