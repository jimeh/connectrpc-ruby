# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = "connectrpc-async"
  # Read without loading library code before SimpleCov starts.
  spec.version = File.read(File.join(__dir__, "lib/connectrpc/async/version.rb"))[/VERSION = "([^"]+)"/, 1]
  spec.summary = "Optional Async host and HTTP/1.1 and HTTP/2 ConnectRPC transports"
  spec.authors = ["jimeh"]
  spec.homepage = "https://github.com/jimeh/connectrpc-ruby"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.4"
  spec.metadata["allowed_push_host"] = "https://rubygems.org"
  spec.metadata["rubygems_mfa_required"] = "true"
  spec.files = Dir.glob("sig/**/*.{rbs,yaml}") + Dir["lib/**/*.rb", "LICENSE", "README.md"].sort
  spec.require_paths = ["lib"]
  local_requirement = "~> #{spec.version.to_s.split('.').first(2).join('.')}.0"
  {
    "connectrpc" => local_requirement, "async-http" => "~> 0.105.0", "async" => "~> 2.46.0",
    "async-pool" => "~> 0.12.0", "protocol-http2" => "~> 0.29.2", "protocol-http1" => "~> 0.41.1",
    "protocol-http" => "~> 0.72.1",
    "io-endpoint" => "~> 0.19.0", "io-stream" => "~> 0.14.0", "openssl" => "~> 4.0.2"
  }.each { |name, requirement| spec.add_dependency(name, requirement) }
end
