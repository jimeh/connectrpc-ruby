# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = "connectrpc-testing"
  spec.version = "0.1.0"
  spec.authors = ["jimeh"]
  spec.summary = "In-process transport for real ConnectRPC unary round trips"
  spec.description = "Optional synchronous testing transport using an application-owned core Server."
  spec.homepage = "https://github.com/jimeh/connectrpc-ruby"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.4"
  spec.metadata["allowed_push_host"] = "https://gems.invalid"
  spec.metadata["source_code_uri"] = spec.homepage
  spec.files = Dir.glob("lib/**/*.rb") + %w[README.md LICENSE]
  spec.require_paths = ["lib"]
  spec.add_dependency "connectrpc", "~> 0.1.0"
end
