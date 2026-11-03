# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = "connectrpc-async"
  spec.version = "0.1.0"
  spec.summary = "Optional Async HTTP/2 host and transport for ConnectRPC unary"
  spec.authors = ["jimeh"]
  spec.homepage = "https://github.com/jimeh/connectrpc-ruby"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.4"
  spec.metadata["allowed_push_host"] = "https://gems.invalid"
  spec.files = Dir.glob("sig/**/*.{rbs,yaml}") + Dir["lib/**/*.rb", "LICENSE", "README.md"].sort
  spec.require_paths = ["lib"]
  {
    "connectrpc" => "~> 0.1.0", "async-http" => "~> 0.105.0", "async" => "~> 2.46.0",
    "async-pool" => "~> 0.12.0", "protocol-http2" => "~> 0.29.2", "protocol-http" => "~> 0.72.1",
    "io-endpoint" => "~> 0.19.0", "io-stream" => "~> 0.14.0", "openssl" => "~> 4.0.2"
  }.each { |name, requirement| spec.add_dependency(name, requirement) }
end
